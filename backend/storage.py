""" DDH abstract storage
"""
from __future__ import annotations
import zlib
import enum

from core import keys,permissions,nodes,transactions
from utils.pydantic_utils import NoCopyBaseModel

@enum.unique
class Variant(enum.IntEnum):
    """ Storage variant, record whether blob is compressed.
    """
    uncompressed = 0
    zlib = 1

class StorageClass(NoCopyBaseModel):

    byId : dict[nodes.PersistId,StorageBlock] = {}


    def __contains__(self, id : nodes.PersistId):
        """ does id exist in storage? """
        return id in self.byId

    def store(self,id : nodes.PersistId, data : bytes, transaction: transactions.Transaction):
        self.byId[id] = StorageBlock(variant=Variant.uncompressed,blob=data)
        return

    def delete(self,id : nodes.PersistId, transaction: transactions.Transaction):
        """ delete from storage, must supply key to verify """
        self.byId.pop(id,None)
        return

    def load(self,id : nodes.PersistId, transaction: transactions.Transaction) -> bytes:
        sb = self.byId.get(id,None)
        if not sb:
            raise KeyError(id)
        else:
            if sb.variant == Variant.uncompressed:
                data = sb.blob
            elif sb.variant == Variant.zlib:
                data = zlib.decompress(sb.blob)
            else: 
                raise ValueError(f'Unknown storage variant {sb.variant}')
            return data

Storage = StorageClass()

class StorageBlock(NoCopyBaseModel):
    """ Elementary block of storage """
    variant : Variant = Variant.uncompressed
    blob : bytes

