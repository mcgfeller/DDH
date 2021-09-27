""" DDH abstract storage
"""
from __future__ import annotations
import zlib
import cryptography.fernet
import enum

from core import keys,permissions,nodes
from utils.pydantic_utils import NoCopyBaseModel

@enum.unique
class Variant(enum.IntEnum):
    """ Storage variant, record whether blob is compressed.
    """
    uncompressed = 0
    zlib = 1

class StorageClass(NoCopyBaseModel):

    byId : dict[nodes.NodeId,StorageBlock] = {}

    def store(self,id : nodes.NodeId, key: bytes, data : bytes):
        data = zlib.compress(data, level=-1)
        data = cryptography.fernet.Fernet(key).encrypt(data)
        self.byId[id] = StorageBlock(variant=Variant.zlib,blob=data)
        return

    def delete(self,id : nodes.NodeId, key: bytes):
        """ delete from storage, must supply key to verify """
        data = self.load(id,key) # just load to verify
        self.byId.pop(id,None)
        return

    def load(self,id : nodes.NodeId, key: bytes) -> bytes:
        sb = self.byId.get(id,None)
        if not sb:
            raise KeyError(id)
        else:
            data = cryptography.fernet.Fernet(key).decrypt(sb.blob)
            if sb.variant == Variant.uncompressed:
                pass
            elif sb.variant == Variant.zlib:
                data = zlib.decompress(data)
            else: 
                raise ValueError(f'Unknown storage variant {sb.variant}')
            return data

Storage = StorageClass()

class StorageBlock(NoCopyBaseModel):
    """ Elementary block of storage """
    variant : Variant = Variant.uncompressed
    blob : bytes

