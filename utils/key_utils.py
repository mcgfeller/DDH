""" Utilities involving DDHKey's """
import typing
from core import keys


class LookupByKey(dict):
    """ A dictionary of {DDHkey: value} with a lookup that returns the most specific key first """

    def most_specific(self, ddhkey: keys.DDHkey, default=None) -> tuple[keys.DDHkey, typing.Any]:
        """ return the most specific (=longest) (key,value), or default """
        return next(((s, v) for s in ddhkey.longest_segments()
                     if (v := self.get(s)) is not None), (ddhkey, default))


def nested_get_key(data: dict, key: keys.DDHkey | tuple) -> typing.Any:
    """ get elements of nested dict according to key (full key or tuple)  """
    key = key.key if isinstance(key, keys.DDHkey) else tuple(key)
    for k in key:
        data = data.get(k, {})
    return data
