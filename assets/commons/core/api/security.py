# from fastapi.security import OAuth2PasswordBearer
# from keycloak import KeycloakOpenID, KeycloakAuthenticationError
# from ..config.Settings import Settings
#
# keycloak_openid = KeycloakOpenID(server_url=Settings.KEYCLOAK_HOST,
#                                  realm_name=Settings.INDEXING_KEYCLOAK_REALM,
#                                  client_id=Settings.INDEXING_KEYCLOAK_CLIENT_ID,
#                                  client_secret_key=Settings.INDEXING_KEYCLOAK_CLIENT_SECRET_KEY)
#
# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
