import json
from dataclasses import dataclass, field
from typing import Dict, List, Union
#from ..security import keycloak_openid, oauth2_scheme
from dacite import from_dict
from fastapi import HTTPException, Depends
from keycloak import KeycloakGetError


@dataclass
class KeycloakTokenData:
    exp: int = -1
    iat: int = -1
    jti: str = None
    iss: str = None
    aud: Union[str, List] = None
    sub: str = None
    typ: str = None
    azp: str = None
    session_state: str = None
    name: str = None
    given_name: str = None
    family_name: str = None
    preferred_username: str = None
    email: str = None
    email_verified: bool = False
    acr: str = None
    realm_access: Dict = field(default_factory=dict)
    resource_access: Dict = field(default_factory=dict)
    groups: List = field(default_factory=list)
    scope: str = None
    sid: str = None
    client_id: str = None
    username: str = None
    active: bool = False



class SentoAuth:

    # Store instance of keycloak_openid as static class variable
    #_keycloak = keycloak_openid

    def __init__(self):
        pass

    # def __init__(self, token: str = Depends(oauth2_scheme)):
    #     self._token = token
    #     self.__introspect_token(token)

    def __introspect_token(self, token):
        try:
            # # Decode Token
            # KEYCLOAK_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----\n" + self._keycloak.public_key() + "\n-----END PUBLIC KEY-----"
            # options = {"verify_signature": True, "verify_aud": False, "verify_exp": True}
            # try:
            #     token_info = self._keycloak.decode_token(token, key=KEYCLOAK_PUBLIC_KEY, options=options)
            #     token_info["active"] = True
            # except Exception as e:
            #     print(e)
            #     token_info = {'active': False}
            token_info = self._keycloak.introspect(token)
        except KeycloakGetError as e:
            raise HTTPException(401, detail=f"Token is not valid: {str(e)}")

        token_info = from_dict(
            data_class=KeycloakTokenData,
            data=token_info
        )
        if not token_info.active:
            raise HTTPException(401, detail="Token is not active. Try to refresh token")

        self.__token_info = token_info

    def get_client_roles(self, client=None) -> List:
        if not self.__token_info.active:
            return []
        client_id = self.__token_info.client_id if client is None else client
        return self.__token_info.resource_access.get(client_id, {}).get("roles", [])

    def get_realms_roles(self) -> List:
        if not self.__token_info.active:
            return []
        return self.__token_info.realm_access.get("roles", [])

    def get_groups(self) -> List:
        return self.__token_info.groups

    def validate_token(self):
        if self.__token_info.sub is None:
            raise HTTPException(
                status_code=401,
                detail=json.dumps(
                    {"error": "invalid_grant", "error_description": "Request Forbidden: Invalid Token"}
                )
            )

    def get_token_info(self) -> KeycloakTokenData:
        return self.__token_info


