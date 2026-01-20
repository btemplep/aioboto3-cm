"""See the ``AIOBoto3CM`` class.
"""

__all__ = [
    "AIOBoto3CM"
]

import asyncio
from contextlib import AsyncExitStack
from typing import Any, Dict

import aioboto3
from botocore.config import Config

from aioboto3_cm.exceptions import SessionConflictError, SessionNotFoundError


class AIOBoto3CM:
    """aioboto3 Client Manager

    Manage and cache aioboto3 clients without using context managers. 
    
    Parameters
    ----------
    default_client_kwargs : Dict[str, Any], default=None
        Specify default kwargs that will be passed when creating clients via ``AIOBoto3CM().client()``. 
        Takes all arguments for ``aioboto3.Session().client()`` besides ``service_name``. 
        Non-default values passed to ``AIOBoto3CM().client()`` override any values specified in this parameter.

    Examples
    --------
    .. code-block:: python

        import asyncio

        from aioboto3_cm import AIOBoto3CM

        # this can be created outside of a coroutine!
        abcm = AIOBoto3CM()

        async def main():
            sts_client = await abcm.client("sts")
            resp = await sts_client.get_caller_identity()
            print(resp)
            # clients are cached and reused
            same_sts_client = await abcm.client("sts") 
            # close the client before exiting
            await abcm.close("sts") 
            # you can also close all clients at once if you created several
            await abcm.close_all() 


        asyncio.run(main())
    """


    def __init__(self, default_client_kwargs: Dict[str, Any] = None):
        self._default_client_kwargs = default_client_kwargs if default_client_kwargs is not None else {}
        self._lock = asyncio.Lock()
        """Locks the whole lut for client creation reads and client lock writes."""
        self._client_lut = {}
        """Session and client lookup table
        .. code-block:: python
            
            {
                "<session name or None>": {
                    "session": "<aioboto3 Session>",
                    "groups": {
                        "<group name or None>": {
                            "<region name or None>": {
                                "<service name>": {
                                    "aes": "<Async Exit Stack>",
                                    "client": "<aioboto3 client>",
                                    "lock": "<temporary client lock or None>"
                                }
                            }
                        }
                    }
                }
            }
        """


    def register_session(self, session:aioboto3.Session, abcm_session_name: str | None=None) -> None:
        """Register a new aioboto3 Session. 
        
        Parameters
        ----------
        session : aioboto3.Session
            The new session to register.
        abcm_session_name : str | None, default=None
            Name of the session to register. If `abcm_session_name` is not given, the session will be registered as the default session. 
        
        Raises
        ------
        aioboto3_cm.exceptions.SessionConflictError
            A session with the given name is already registered.
        """
        if abcm_session_name in self._client_lut:
            raise SessionConflictError(f"Session name '{abcm_session_name}' is already registered.")

        self._client_lut[abcm_session_name] = {
            "session": session,
            "groups": {}
        }
    

    def get_session(self, abcm_session_name: str | None=None) -> aioboto3.Session:
        """Retrieve an existing session by name. 

        Parameters
        ----------
        abcm_session_name : str
            Name of the session to retrieve.

        Returns
        -------
        aioboto3.Session
            Names session.
        
        Raises
        ------
        aioboto3_cm.exceptions.SessionNotFoundError
            Session not found.
        """
        try:
            return self._client_lut[abcm_session_name]['session']
        except KeyError as exc:
            if abcm_session_name is None:
                raise SessionNotFoundError(f"The default session has not been created yet. You must either register a default session or create a client under the default session first.") from exc 
               
            raise SessionNotFoundError(f"Session name '{abcm_session_name}' is not registered.") from exc


    def _get_client_arg(self, arg_name: str, arg_value: Any, default_value: Any) -> Any:
        if arg_value == default_value:
            return self._default_client_kwargs.get(arg_name, default_value)
            
        return arg_value
        
    
    async def client(
        self,
        service_name: str, 
        region_name: str=None, 
        api_version: str=None, 
        use_ssl: bool=True, 
        verify: bool | str=None, 
        endpoint_url: str=None, 
        aws_access_key_id: str=None, 
        aws_secret_access_key: str=None, 
        aws_session_token: str=None, 
        config: Config=None,
        aws_account_id: str=None,
        abcm_session_name: str | None=None,
        abcm_client_group: str | None=None
    ) -> Any:
        """Create or retrieve an aioboto3 Client.

        All parameters not starting with ``abcm_`` are passed directly to the client when creating a new one. 

        Passing parameters other than service, region, abcm_session_name, and abcm_client_group 
        after a client is created will have no effect on the cached client.

        Parameters
        ----------
        service_name : str
            The name of a service.
        region_name : str, default=None
            The name of the region associated with the client. A client is associated with a single region.
        api_version : str, default=None
            The API version to use. By default, botocore will use the latest API version when creating a client. You only need to specify this parameter if you want to use a previous API version of the client.
        use_ssl : bool, default=None
            Whether or not to use SSL. By default, SSL is used. Note that not all services support non-ssl connections., by default True
        verify : bool | str, default=True
            Whether or not to verify SSL certificates. By default SSL certificates are verified. You can provide the following values:
            - False - do not validate SSL certificates. SSL will still be used (unless use_ssl is False), but SSL certificates will not be verified.
            - path/to/cert/bundle.pem - A filename of the CA cert bundle to uses. You can specify this argument if you want to use a different CA cert bundle than the one used by botocore.
        endpoint_url : str, default=None
            _descThe complete URL to use for the constructed client. Normally, botocore will automatically construct the appropriate URL to use when communicating with a service. You can specify a complete URL (including the “http/https” scheme) to override this behavior. If this value is provided, then use_ssl is ignored.ription_
        aws_access_key_id : str, default=None
            _desThe access key to use when creating the client. This is entirely optional, and if not provided, the credentials configured for the session will automatically be used. You only need to provide this argument if you want to override the credentials used for this specific client.cription_
        aws_secret_access_key : str, default=None
            The secret key to use when creating the client. Same semantics as aws_access_key_id above.
        aws_session_token : str, default=None
            The session token to use when creating the client. Same semantics as aws_access_key_id above.
        config : botocore.config.Config, default=None
            Advanced client configuration options. If region_name is specified in the client config, its value will take precedence over environment variables and configuration values, but not over a region_name value passed explicitly to the method. See botocore config documentation for more details.
        aws_account_id : str, default=None
            The account id to use when creating the client. Same semantics as aws_access_key_id above.
        abcm_session_name : str | None, default=None
            Name of the registered session to create or retrieve the client under. 
        abcm_client_group : str | None, default=None
            Name of the client group to create or retrieve the client under. 
            Creates a new group if the given one does not exist. 

        Returns
        -------
        Any
            The new or cached client.

        Raises
        ------
        aioboto3_cm.exceptions.SessionNotFoundError
            The given session name was not previously registered.
        """
        # first try to return if cached
        try:
            return self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name][service_name]['client']
        except KeyError:
            pass
        
        client_kwargs = {
            "service_name": service_name,
            "region_name": self._get_client_arg("region_name", region_name, None),
            "api_version": self._get_client_arg("api_version", api_version, None),
            "use_ssl": self._get_client_arg("use_ssl", use_ssl, True),
            "verify": self._get_client_arg("verify", verify, None),
            "endpoint_url": self._get_client_arg("endpoint_url", endpoint_url, None),
            "aws_access_key_id": self._get_client_arg("aws_access_key_id", aws_access_key_id, None),
            "aws_secret_access_key": self._get_client_arg("aws_secret_access_key", aws_secret_access_key, None),
            "aws_session_token": self._get_client_arg("aws_session_token", aws_session_token, None),
            "config": self._get_client_arg("config", config, None),
            "aws_account_id": self._get_client_arg("aws_account_id", aws_account_id, None)
        }
        # if cache miss, then lock LUT on read and write so that we can check/create the path for the client
        await self._lock.acquire()
        # After acquiring lock, check to see if client is created or being created
        if (
            abcm_session_name in self._client_lut
            and abcm_client_group in self._client_lut[abcm_session_name]['groups']
            and region_name in self._client_lut[abcm_session_name]['groups'][abcm_client_group]
            and service_name in self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name]
        ):
            # When the client was already created while we waited for the lock
            if "client" in self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name][service_name]:
                self._lock.release()
                return self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name][service_name]['client']
            # When the client is being created, so if we wait for the client lock, then it will be done. 
            else: 
                self._lock.release()
                client_lock: asyncio.Lock = self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name][service_name]['lock']
                await client_lock.acquire()
                client_lock.release()

                # if that fails then we will retry to make our own client
                try:
                    return self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name][service_name]['client']
                except KeyError:
                    return await self.client(
                        **client_kwargs,
                        abcm_session_name=abcm_session_name,
                        abcm_client_group=abcm_client_group
                    )
        
        if abcm_session_name not in self._client_lut:
            if abcm_session_name is None:
                self._client_lut[None] = {
                    "session": aioboto3.Session(),
                    "groups": {}
                }
            else:
                self._lock.release()
                raise SessionNotFoundError(f"Session name '{abcm_session_name}' is not registered.")
        
        if abcm_client_group not in self._client_lut[abcm_session_name]['groups']:
            self._client_lut[abcm_session_name]['groups'][abcm_client_group] = {}
    
        # If it's a bad region name, just leave it anyway 
        if region_name not in self._client_lut[abcm_session_name]['groups'][abcm_client_group]:
            self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name] = {}
        
        # At this point, the rest of the path exists besides the service and client
        client_lock = asyncio.Lock()
        await client_lock.acquire()
        self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name][service_name] = {
            "lock": client_lock
        }
        self._lock.release()
        aes = AsyncExitStack() 
        try:
            new_client = await aes.enter_async_context(
                self._client_lut[abcm_session_name]['session'].client(**client_kwargs)
            )
        except Exception:
            self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name].pop(service_name)
            client_lock.release()  
        
        self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name][service_name] = {
            "aes": aes,
            "client": new_client
        }
        client_lock.release()

        return new_client

    
    async def close(
        self,
        service_name: str, 
        region_name: str=None,
        abcm_session_name: str | None=None,
        abcm_client_group: str | None=None 
    ) -> None:
        """Close a single client if it exists, or else do nothing.

        Preserves sessions, and client groups.
        Does not close clients that are in the process of being created.

        Parameters
        ----------
        service_name : str
            The name of a service.
        region_name : str, default=None
            The name of the region associated with the client.
        abcm_session_name : str | None, default=None
            Name of the registered session the client is under. 
        abcm_client_group : str | None, default=None
            Name of the client group the client is under.
        """
        service = None
        async with self._lock:
            # if client exists and is created then pop it and release lock
            try:
                if "lock" not in self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name][service_name]:
                    service = self._client_lut[abcm_session_name]['groups'][abcm_client_group][region_name].pop(service_name)
            except KeyError:
                pass
        
        if service is not None:
            await service['aes'].aclose()


    async def close_all(self) -> None:
        """Close all clients. 

        Preserves sessions, and client groups.
        Does not close clients that are in the process of being created.
        """
        close_services = []
        async with self._lock:
            for session_name in self._client_lut:
                for group in self._client_lut[session_name]['groups']:
                    for region in self._client_lut[session_name]['groups'][group]:
                        keep_services = {}
                        for service in self._client_lut[session_name]['groups'][group][region]:
                            if "lock" in self._client_lut[session_name]['groups'][group][region][service]:
                                keep_services[service] = self._client_lut[session_name]['groups'][group][region][service]
                            else:
                                close_services.append(
                                    self._client_lut[session_name]['groups'][group][region][service]
                                )

                        self._client_lut[session_name]['groups'][group][region] = keep_services
        
        await asyncio.gather(*[s['aes'].aclose() for s in close_services])

