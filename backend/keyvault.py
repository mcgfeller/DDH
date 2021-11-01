""" DDH keyvault.
    This is the center of all crypto operations involving keys. Keys should not "leave" this module. 
    It implements the section 7.3 "Protection of data at rest and on the move" of the DDH paper.
"""
from __future__ import annotations
from abc import abstractmethod
import typing
import hashlib
import cryptography.fernet

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding,rsa
import cryptography.fernet 
import base64
from core import common_ids


from core import keys,permissions,nodes,principals
from utils.pydantic_utils import NoCopyBaseModel

class StorageKey:
    """ Ephemeral storage key, never stored """
    def __init__(self,key : bytes):
        try:
            self._fernet = cryptography.fernet.Fernet(key)
        except Exception as e:
            print(e,key)
            self._fernet = cryptography.fernet.Fernet(base64.urlsafe_b64encode(key))

    def encrypt(self, plaintext : bytes) -> bytes:
        ciphertext = self._fernet.encrypt(plaintext) 
        return ciphertext
        

    def decrypt(self, ciphertext : bytes) -> bytes:
        plaintext = self._fernet.decrypt(ciphertext)
        return plaintext

class AccessKey(NoCopyBaseModel):
    """ StorageKey for a Node encrypted by the Principal's public key.
        To access the Node, it needs to be decrypted by the Principal's private key, which is stored 
        only in the PrincipalKey.
    """
    nodeid : str
    principal : principals.Principal
    key : bytes



class AccessKeyVaultClass(NoCopyBaseModel):
    """ Hold AccessKey by (principal.id, and key.nodeid) """
    access_keys : dict[tuple,AccessKey] = {}

    def add(self,key: AccessKey):
        self.access_keys[(key.principal.id, key.nodeid)] = key

    def remove(self,principal : principals.Principal, nodeid :str ):
        self.access_keys.pop((principal.id, nodeid),None) 


    def get_storage_key(self, principal : principals.Principal, nodeid : common_ids.PersistId) -> StorageKey:
        p_key = PrincipalKeyVault.key_for_principal(principal)
        if not p_key:
            raise KeyError(f'no key found for principal={principal}')
        else:
            a_key = self.access_keys[(principal.id, nodeid)]
            # s_key = StorageKey(add_consent_hash(p_key.decrypt(a_key.key),node.consents))
            s_key = StorageKey(p_key.decrypt(a_key.key))
        return s_key



AccessKeyVault = AccessKeyVaultClass()

class PrincipalKey(NoCopyBaseModel):
    """ Public/Private key for a principal """

    principal  : principals.Principal
    key        : typing.Any # the exact type is something deep in authlib.
    key_params : typing.ClassVar[dict] = {'kty':'RSA','crv_or_size':2048,'is_private':True} # params to JsonWebKey.generate_key

    Padding : typing.ClassVar[typing.Any] =  padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()),
        algorithm=hashes.SHA256(),
        label=None
        )

    @classmethod
    def create(cls,principal):
        """ Would like to be more generic with key generation here, but Padding needs to corrspond. """
        return cls(principal=principal,key=rsa.generate_private_key(public_exponent=65537,key_size=cls.key_params['crv_or_size'],))

    def encrypt(self, plaintext : bytes) -> bytes:
        public_key = self.key.public_key()
        ciphertext = public_key.encrypt(plaintext,self.Padding)
        return ciphertext
        

    def decrypt(self, ciphertext : bytes) -> bytes:
        plaintext = self.key.decrypt(ciphertext,self.Padding)
        return plaintext


class PrincipalKeyVaultClass(NoCopyBaseModel):

    key_by_principal : dict[str,PrincipalKey] = {}

    def key_for_principal(self, principal : principals.Principal) -> typing.Optional[PrincipalKey]:
        return self.key_by_principal.get(principal.id)

    def create(self,principal : principals.Principal) -> PrincipalKey:
        """ create a user key, store and return it """
        assert principal.id not in self.key_by_principal
        key = PrincipalKey.create(principal=principal)
        self.key_by_principal[principal.id] = key
        return key

PrincipalKeyVault = PrincipalKeyVaultClass()

def get_nonce() -> bytes:
    return cryptography.fernet.Fernet.generate_key()

def add_consent_hash(key : bytes , consents : permissions.Consents):
    ch = hashlib.blake2b(consents.json().encode(),digest_size=len(key)).digest()
    key = base64.urlsafe_b64decode(key)
    key = bytes([a ^ b for a, b in zip(key,ch)]) # xor
    key = base64.urlsafe_b64encode(key)
    return key


def set_new_storage_key(node : nodes.DataNode, principal: principals.Principal, effective : set[principals.Principal], removed : set[principals.Principal]):
    """ set storage key based on private key of principal and public keys of consentees """
    # assert node.consents
    storage_key = get_nonce() # add_consent_hash(get_nonce(),node.consents) # new storage key

    for p in {principal} | effective:
        p_key = PrincipalKeyVault.key_for_principal(p)
        if not p_key:
            p_key = PrincipalKeyVault.create(p)
        p_storage_key = AccessKey(nodeid=node.id,principal=p,key=p_key.encrypt(storage_key))
        AccessKeyVault.add(p_storage_key)
    for p in removed: # remove old entries
        AccessKeyVault.remove(principal=p,nodeid=node.id)
    return 

def encrypt_data(principal : principals.Principal, nodeid : common_ids.PersistId, data : bytes) -> bytes:
    """ Encrypt data going to storage for a node and accessing Principal """
    storage_key = AccessKeyVault.get_storage_key(principal,nodeid)
    cipherdata = storage_key.encrypt(data)
    return cipherdata

def decrypt_data(principal : principals.Principal, nodeid : common_ids.PersistId, cipherdata : bytes) -> bytes:
    """ Decrypt data coming from storage for a node and accessing Principal """
    storage_key = AccessKeyVault.get_storage_key(principal,nodeid)
    data = storage_key.decrypt(cipherdata)
    return data





