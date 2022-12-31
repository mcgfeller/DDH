""" Utilities involving DDHKey's """
import typing
from core import keys


class LookupByKey(dict):
    """ A dictionary of {DDHkey: value} with a lookup that returns the most specific key first """

    def most_specific(self, ddhkey: keys.DDHkey, default=None) -> tuple[keys.DDHkey, typing.Any]:
        """ return the most specific (=longest) (key,value), or default """
        return next(((s, v) for s in ddhkey.longest_segments()
                     if (v := self.get(s)) is not None), (ddhkey, default))
