""" Utilities """
import sys, threading, traceback, collections, inspect, typing, itertools
from time import time as _time, sleep as _sleep
import heapq
import logging

logger = logging.getLogger(__name__)


def isPyPy():
    return not hasattr(sys, 'getrefcount')


def load_module(modulename, raiseError=True):
    """ Dynamically load a module, return module
        If raiseError is False, error is logged and None is returned.
    """
    module = sys.modules.get(modulename)
    if module is None:
        logger.debug("Importing module "+modulename)
        msg = ''
        try:
            __import__(modulename, globals(), locals(), [])
        except Exception as e:
            msg = traceback.format_exc()
        module = sys.modules.get(modulename)
        if module is None:
            msg = 'Could not load module: '+str(modulename)+'\n '+str(msg)
            if raiseError:
                raise ImportError(msg)
            else:
                logger.error(msg)
    return module


def load_class(modulename: str, classname: str) -> type:

    module = sys.modules.get(modulename, None)
    if module is None:
        module = load_module(modulename)
    cls = getattr(module, classname)
    return cls


def typecheck(obj, cls, allowNone=False):
    """ Simple type check. obj is checked for instance of cls. If obj is a sequence, and cls is not, the
        elements of obj are checked.
    """
    if not isinstance(obj, cls):
        if allowNone and obj is None: return
        if type(obj) in (list, tuple):
            for i in obj:
                typecheck(i, cls)
        else:
            raise TypeError('Invalid object '+str(obj)+' of type '+type(obj).__name__+'; expected '+cls.__name__+'.')
    return


T = typing.TypeVar('T')


def ensure_tuple(seq: typing.Iterable[T] | T) -> tuple[T]:
    """ ensure seq is a tuple, converting strings and other simple elements to 1-elem tuples """
    if type(seq) in (list, tuple, set, frozenset):
        return tuple(seq)
    else:
        return (seq,)


def batched(iterable, n):
    # TODO: Python 3.12 -> replace by itertools.batched()
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def allin(a, b):
    """ return True if all items of sequence a are in sequence b """
    return not False in [x in b for x in a]


def anyin(a, b):
    """ return True if any item of sequence a is in sequence b """
    return True in [x in b for x in a]


def raise_if_error(exception):
    """ raise exception unless None. Would be nice if Python did it that way. """
    if exception:
        raise exception
    else:
        return


def consume_iterator(iterator):
    """ consume an iterator to the end efficiently """
    # feed the entire iterator into a zero-length deque
    collections.deque(iterator, maxlen=0)


def topological_sort(items, partial_order):
    """ Perform topological sort.
        items is a list of items to be sorted.
        partial_order is a list of pairs. If pair (a,b) is in it, it means
        that item a should appear before item b.
        Returns a list of the items in one of the possible orders, or None
        if partial_order contains a loop.

        The sort is now stable as we use a heapq to store according to original indices. 

        Original topological sort code written by Ofer Faigon (www.bitformation.com) and used with permission
        (c) Copyright Ofer Faigon 2007

        Taken from http://www.bitformation.com/art/python_toposort.html       
    """

    # step 1 - create a directed graph with an arc a->b for each input
    # pair (a,b).
    # The graph is represented by a dictionary. The dictionary contains
    # a pair item:list for each node in the graph. /item/ is the value
    # of the node. /list/'s 1st item is the count of incoming arcs, and
    # the rest are the destinations of the outgoing arcs. For example:
    #           {'a':[0,'b','c'], 'b':[1], 'c':[1]}
    # represents the graph:   c <-- a --> b
    # The graph may contain loops and multiple arcs.
    # Note that our representation does not contain reference loops to
    # cause GC problems even when the represented graph contains loops,
    # because we keep the node names rather than references to the nodes.
    graph = {}
    for i, node in enumerate(items):
        if node not in graph:
            graph[node] = [i, 0]  # 0 = number of arcs coming into this node.
    for fromnode, tonode in partial_order:
        """ Add an arc to a graph. Can create multiple arcs.
            The end nodes must already exist."""
        graph[fromnode].append(tonode)
        # Update the count of incoming arcs in tonode.
        graph[tonode][1] = graph[tonode][1] + 1

    # Step 2 - find all roots (nodes with zero incoming arcs), use original index as key:
    roots = [(nodeinfo[0], node) for (node, nodeinfo) in list(graph.items()) if nodeinfo[1] == 0]
    heapq.heapify(roots)

    # step 3 - repeatedly emit a root and remove it from the graph. Removing
    # a node may convert some of the node's direct children into roots.
    # Whenever that happens, we append the new roots to the list of
    # current roots.
    tsorted = []
    while len(roots) != 0:
        # If len(roots) is always 1 when we get here, it means that
        # the input describes a complete ordering and there is only
        # one possible output.
        # When len(roots) > 1, we can choose any root to send to the
        # output; this freedom represents the multiple complete orderings
        # that satisfy the input restrictions. We arbitrarily take one of
        # the roots using pop(). Note that for the algorithm to be efficient,
        # this operation must be done in O(1) time.
        dummy, root = heapq.heappop(roots)
        tsorted.append(root)
        for child in graph[root][2:]:
            graph[child][1] = graph[child][1] - 1
            if graph[child][1] == 0:
                heapq.heappush(roots, (graph[child][0], child))
        del graph[root]
    if len(graph) != 0:
        # There is a loop in the input.
        return None
    return tsorted


def formatted_stack(maxdepth=None):
    try:
        s = inspect.stack()
    except: return []
    if maxdepth is None: maxdepth = len(s)
    return [l[1]+'.'+l[3]+'['+str(l[2])+']' for l in s[1:maxdepth+2]]


class Singleton(object):
    """ Use this class as a superclass to ensure the derived class is Singleton, i.e., only one
        object is ever created.

        This operation is made thread-safe my using a lock.

        Please note that __init__ is called on the Singleton each time, not only for the first time!        
    """
    _tlock = threading.Lock()

    def __new__(cls, *p, **k):
        Singleton._tlock.acquire()
        try:  # new might fail on a cls, ensure lock is released
            if not '_the_instance' in cls.__dict__:
                cls._the_instance = object.__new__(cls)
        finally:
            Singleton._tlock.release()
        return cls._the_instance


class Invalidatable:
    """ simple object to keep a valid status and invoke a callable to revalidate """

    def __init__(self, revalidate: typing.Callable):
        self.revalidate = revalidate
        self._valid: bool = False

    def invalidate(self):
        self._valid = False

    def use(self):
        """ call before using the parent, used to revalidate if parent is invalid """
        if not self._valid:
            self.revalidate()
            self._valid = True
        return


class _DummyContext(object):
    """ A context that does nothing on entry and exit and also offers dummy lock methods """

    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc_value, traceback):
        return None

    def acquire(self, blocking=True):
        return True

    def release(self):
        return None

    def locked(self):
        return False


DummyContext = _DummyContext()  # useful only as object
