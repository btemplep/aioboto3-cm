"""aioboto3-cm - aioboto Client Manager

Manage and cache aioboto3 clients without context managers. 

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
__all__ = [
    "AIOBoto3CM",
    "AIOBoto3CMError",
    "SessionConflictError",
    "SessionNotFoundError"
]

__version__ = "0.1.0"


from aioboto3_cm.aioboto3_cm import AIOBoto3CM
from aioboto3_cm.exceptions import (
    AIOBoto3CMError,
    SessionConflictError,
    SessionNotFoundError
)

