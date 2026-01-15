# AIOBoto3 Client Manager

Manage and cache [aioboto3](https://github.com/terricain/aioboto3) clients without context managers. 

- [Installation](#installation)
- [Tutorial](#tutorial)
- [FastAPI Tutorial](#fastapi-tutorial)


## Installation

```console
$ pip install aioboto3-cm
```


## Tutorial

```python
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
```

Other useful and more advanced feature include:
- aioboto3-cm can keep track of several named aioboto3 Sessions, client groups, and clients. 
- Client groups are a construct used to arbitrarily group clients if needed. 
- The default session refers to the session that is referenced if a session name is not specified in the `client` function. 
Same for the default client group.
- The default session and client group are automatically created if not specified when the first client is created. 
- Setting default client args at the class instance level
- Registering the default session manually
- Registering other named sessions from tools like [aioboto3-assume](https://github.com/btemplep/aioboto3-assume) for auto-refreshing, indefinite sessions. 
- Async Safe - Use in multiple concurrent coroutines.  **Not thread safe**

A more advanced example outlining all of these features:

```python
import asyncio

from aioboto3 import Session
from aioboto3_cm import AIOBoto3CM
from aioboto3_assume import assume_role
from botocore.config import Config


abcm = AIOBoto3CM(
    default_client_kwargs={ # set defaults when creating clients
        "config": Config(
            retries={
                "total_max_attempts": 10
            }
        )
    }
)
session = Session(region_name="us-east-1")
# Register a default session 
abcm.register_session(session)
# Add sessions for things like cross accounts roles
abcm.register_session(
    session=assume_role( # use aioboto3_assume.assume_role for auto-refreshing credentials
        source_session=session,
        assume_role_kwargs={
            "RoleArn": "arn:aws:iam::123412341234:role/my_role",
            "RoleSessionName": "my-role-session"
        }
    ),
    abcm_session_name="123412341234"
)

async def main():
    # This client will have the default Config object we configured above
    sts_client = await abcm.client("sts") 
    # This client is for the cross account session, and will be apart of a new group "no_retries"
    cross_account_sts_client = await abcm.client(
        "sts", 
        config=Config(
            retries={
                "total_max_attempts": 1
            }
        ),
        abcm_session_name="123412341234", # this will use the cross account session we registered earlier
        abcm_client_group="no_retries" # Will be kept under a specific client group
    )
    # This are cached and can be retrieved using the same service, region, session name, and client group name
    same_cross_account_sts_client = await abcm.client(
        "sts", 
        config=Config( # Note that since this client exists, any client KWArgs do not do anything
            retries={
                "total_max_attempts": 10
            }
        ),
        abcm_session_name="123412341234",
        abcm_client_group="no_retries"
    )
    
    # Make sure to close all clients before exiting. 
    await abcm.close_all() 


asyncio.run(main())
```


## FastAPI Tutorial

aioboto3-cm can be integrated into [FastAPI](https://fastapi.tiangolo.com/) with the lifecycle feature. 

This assumes you will be using aioboto3-cm throughout the life of your FastAPI app as a cache for aioboto3 clients. 

```python
from contextlib import asynccontextmanager

from aioboto3_cm import AIOBoto3CM
from fastapi import FastAPI

abcm = AIOBoto3CM()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Nothing needed for startup. You could prefill sessions or clients...
    yield
    # Clean up all clients before exiting
    await abcm.close_all()


app = FastAPI(lifespan=lifespan)

@app.get("/my_role")
async def my_role():
    sts_client = await abcm.client("sts")
    result = await sts_client.get_caller_identity()

    return {"result": result}
```
