""" Python Control Program
"""
from __future__ import annotations
import abc
import pydantic
import argparse
import psutil
import httpx
import typing
import subprocess
import pathlib
import sys
import os
import logging
import pprint
import time
import logging

# from pydantic.main import create_model

OnWindows = sys.platform == 'win32'

PARENTDIR = pathlib.Path(__file__).resolve().parents[0]
sys.path.append(str(PARENTDIR))  # put our parent dir on path

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('pcp')

SubclassError = NotImplementedError('must be implemented in subclass')
NotAvailableError = NotImplementedError('action not available')


class Controllable(pydantic.BaseModel):
    """ A thing that is controlled by this program and that can be started, stopped and checked. """

    # Instances : typing.ClassVar[dict[str,'Controllable']] = {}
    # https://github.com/pydantic/pydantic/issues/3679#issuecomment-1337575645
    Instances: typing.ClassVar[dict[str, typing.Any]] = {}
    name: str

    def __init__(self, *a, **d):
        super().__init__(*a, **d)
        # I believe that this is a Pydantic bug - class variables lost in copying (by import):
        if getattr(Controllable, 'Instances', None) is None:
            Controllable.Instances = {}
        Controllable.Instances[self.name] = self

    def __str__(self) -> str:
        return f'{self.name} ({self.__class__.__name__})'

    @classmethod
    def get(cls, *k, default=None) -> list[Controllable]:
        """ Shorthand to get instances """
        return [cls.Instances.get(s, default) for s in k]

    @abc.abstractmethod
    def start(self, args):
        pass
        # raise SubclassError

    @abc.abstractmethod
    def stop(self, args):
        pass
        # raise SubclassError

    def restart(self, args):
        """ restart, usually stops and starts """
        if self.stop(args):
            self.start(args)
        else:
            logger.error(f'Cannot restart process {self}.')
        return

    def is_running(self, args) -> bool:
        """ Check whether process is running by external means """
        return self.getprocess() is not None

    def getprocess(self) -> typing.Optional[psutil.Process]:
        return []

    def health(self, args) -> dict:
        """ send a /health probe to the proccess """
        proc = self.getprocess()
        return {'status': '?', 'pid': proc.pid if proc else '?', 'detail': "This process doesn't provide health information"}

    def check(self, args, _initial=True):
        """ bring the program up if it's not running and healthy """
        check_again = False
        proc = self.getprocess()
        if proc:
            h = self.health(args)
            if h.get('status', '?').lower() not in ('ok', '?'):
                logger.error(f'Process {self} at pid={proc.pid} is running but not healthy: {h}')
                if _initial:
                    self.restart(args)  # running, but not healthy -> restart
                    check_again = True
            else:
                logger.info(f'Process {self} is running at pid={proc.pid} and healthy.')
        else:
            logger.error(f'Process {self} is not running.')
            if _initial:
                self.start(args)  # not running, start
                time.sleep(2)  # let it startup
                check_again = True
        if check_again and _initial:
            self.check(args, _initial=False)
        return


class ProcessGroup(Controllable):
    """ A group of Controllables that can be checked together """

    processes: list[Controllable] = []

    def __str__(self) -> str:
        return f'{self.name} ({self.__class__.__name__}): '+', '.join(map(str, self.processes))

    def add(self, controllable: Controllable):
        self.processes.append(controllable)

    def start(self, args):
        return [process.start(args) for process in self.processes]

    def stop(self, args):
        return all([process.stop(args) for process in self.processes])

    def is_running(self, args) -> bool:
        """ True of all processes in group are running """
        return all(process.is_running(args) for process in self.processes)

    def health(self, args) -> dict[str, dict]:
        """ return dict of dicts of health info """
        return {process.name: process.health(args) for process in self.processes}

    def check(self, args):
        return [process.check(args) for process in self.processes]


class Runnable(Controllable):
    """ Process that is runnable on OS """

    def start(self, args):
        env = os.environ
        if args.env:
            env['SERVER_TYPE'] = args.env
        cmd, add_env, param = self._startcmd()
        env.update(add_env)
        logger.info(
            f"Starting {self} with {' '.join(cmd)} and serverparam={env.get('SERVER_TYPE','*default*')}")
        if OnWindows:
            p = subprocess.Popen(cmd, bufsize=-1, cwd=PARENTDIR, env=env,
                                 creationflags=subprocess.DETACHED_PROCESS, **param)
        else:
            p = subprocess.Popen(cmd, bufsize=-1, cwd=PARENTDIR, env=env, start_new_session=True,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **param)
        logger.info(f'{self} started: pid={p.pid}')

    def stop(self, args, timeout: int = 5) -> bool:
        """ stop a process if it is running. 
            Terminates gracefully, if it doesn't stop within 5 secs, kills it.
        """
        process = self.getprocess()
        if process:
            logger.info(f'Stopping {self} with pid={process.pid}')
            process.terminate()
            try:
                process.wait(timeout=timeout)
            except psutil.TimeoutExpired:
                logger.info(f'Killing {self} with pid={process.pid}')
                process.kill()
            logger.info(f'{self} stopped.')
            process = self.getprocess()
            if process:
                logger.warn(f'{self} with pid={process.pid} could not be stopped.')
                return False
        return True

    def getprocess(self) -> typing.Optional[psutil.Process]:
        """ return one single process or None """
        ps = next((proc for proc in psutil.process_iter(
            ['pid', 'name'], ad_value=None) if self._processfilter(proc)), None)
        return ps

    @abc.abstractmethod
    def _startcmd(self) -> tuple[list, dict[str, str], dict]:
        """ return start parameters:
            - list of command line parameters
            - environment additions
            - kw parameters to Popen

        """
        # raise SubclassError

    @abc.abstractmethod
    def _processfilter(self, proc: psutil.Process) -> bool:
        """ return filter to psutils """
        raise SubclassError


class AsgiProcess(Runnable):
    """ Process running an ASGI server
        Currently, uvicorn is our ASGI server.
    """
    uvicorn_exe = r".venv\scripts\uvicorn" if OnWindows else r'.venv/scripts/uvicorn'
    app: str
    port: int

    def _startcmd(self):
        """ return start parameters """
        # '--no-access-log'
        return ([self.uvicorn_exe,  self.app, '--port', str(self.port), '--no-use-colors'], {'port': str(self.port)}, {})

    def _processfilter(self, proc: psutil.Process) -> bool:
        """ return filter to psutils """
        return 'uvicorn' in proc.name() and self.app in ' '.join(proc.cmdline()) and str(self.port) in proc.cmdline()

    def url(self) -> str:
        """ return url as string """
        return f'http://localhost:{self.port}'

    def health(self, args, appid=None) -> dict:
        """ send a /health probe to the proccess """
        proc = self.getprocess()
        url = self.url()+'/health'
        if appid:
            url += '?appid='+str(appid)
        h = {}
        try:
            r = httpx.get(url)
            h = r.json()
            if not 'pid' in h:
                h['pid'] = proc.pid if proc else '?'
            r.raise_for_status()
        except Exception as e:
            h.update({'status': 'ERR', 'error': str(e)})

        return h


class PythonProcess(Runnable):
    """ A Python process that runs for a long time (otherwise, check and stop wouldn't make sense. """

    python_exe = r"C:\Program Files\Python39\python.exe" if OnWindows else "python3"
    module: pathlib.Path
    healthprocess: typing.Optional[AsgiProcess] = None
    args: list[str] = []

    def _startcmd(self):
        """ return start parameters """
        return ([self.python_exe, str(self.module)]+self.args, {}, {})

    def _processfilter(self, proc: psutil.Process) -> bool:
        """ return filter to psutils """
        mod = str(self.module).split('/')[-1]
        return 'python' in proc.name() and mod in ' '.join(proc.cmdline())

    def health(self, args) -> dict:
        if self.healthprocess:
            return self.healthprocess.health(args, appid=self.module.stem)
        else:
            return super().health(args)


def list_all(args, controllables: list[Controllable]):
    """ list all registered controllables, controllables is ignored """
    print('\n'.join(map(str, Controllable.Instances.values())))
    return


def status(args, controllables: list[Controllable]):
    """ nicely print health """
    h = {c.name: c.health(args) for c in controllables}
    pprint.pprint(h)
    return


def check(args, controllables: list[Controllable]):
    [c.check(args) for c in controllables]
    return


def start(args, controllables: list[Controllable]):
    [c.start(args) for c in controllables]
    return


def stop(args, controllables: list[Controllable]):
    [c.stop(args) for c in controllables]
    return


def restart(args, controllables: list[Controllable]):
    [c.restart(args) for c in controllables]
    return


available_functions = {'list': list_all, 'status': status,
                       'check': check, 'start': start, 'stop': stop, 'restart': restart}


def build_parser(controllable_cls: typing.Type[Controllable]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('command', choices=available_functions.keys())
    # It's a bit tricky to allow an empty argument:
    parser.add_argument('controllables', nargs='*', action='append',
                        choices=[[]]+list(Controllable.Instances.keys()), default=[])  # type: ignore
    parser.add_argument('-a', '--all', dest='doall', action='store_true',
                        help='Apply to all registered controllables, excluding groups')
    parser.add_argument('-e', '--env', type=str, default=None,
                        help='config file environment part, e.g. test or dev (start only)')
    parser.set_defaults(doall=False)
    return parser


def pcp():
    """ main controller, runs functions """
    global logger
    curdir = os.curdir  # ensure we go back to current dir
    try:
        os.chdir(PARENTDIR)  # ensure this is our current dir
        parser = build_parser(Controllable)
        args = parser.parse_args()
        cmdfn = available_functions[args.command]
        if args.doall or args.command == 'list':
            conts = [cont for cont in Controllable.Instances.values(
            ) if not isinstance(cont, ProcessGroup)]
        else:
            cnames = args.controllables[0]
            if not cnames:
                print('error: Specify one or more controllables or --all.')
                return
            else:
                conts = [Controllable.Instances.get(cname) for cname in cnames]
        cmdfn(args, conts)
    finally:
        if curdir:
            os.chdir(curdir)
    return


def getargs() -> argparse.Namespace:
    """ return default args """
    ns = build_parser(Controllable).parse_args(['start', 'ddh'])
    return ns
