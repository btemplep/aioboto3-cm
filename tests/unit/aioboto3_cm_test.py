
import asyncio
from contextlib import asynccontextmanager
from typing import Callable
from unittest.mock import patch

import aioboto3
from botocore.config import Config
import pytest

from aioboto3_cm import AIOBoto3CM, SessionConflictError, SessionNotFoundError


async def test_register_session() -> None:
    abcm = AIOBoto3CM()
    sess = aioboto3.Session()
    with pytest.raises(SessionNotFoundError):
        abcm.get_session()

    abcm.register_session(sess)
    assert abcm.get_session() == sess

    new_sess = aioboto3.Session()
    with pytest.raises(SessionConflictError):
        abcm.register_session(new_sess)
    
    abcm.register_session(new_sess, "new")
    assert abcm.get_session() == sess
    assert abcm.get_session() != new_sess
    assert abcm.get_session("new") == new_sess
    assert abcm.get_session("new") != sess

    with pytest.raises(SessionNotFoundError):
        abcm.get_session("no_sess_here")


async def test_close(abcm: AIOBoto3CM) -> None:
    sts_client = await abcm.client("sts")
    assert abcm._client_lut[None]['groups'][None][None]['sts']['client'] == sts_client
    await abcm.close("sts")
    with pytest.raises(KeyError):
        abcm._client_lut[None]['groups'][None][None]['sts']


async def test_close_no_client(abcm: AIOBoto3CM) -> None:
    await abcm.close("sts")
    # no exceptions is a pass


async def test_close_client_lock(
    abcm: AIOBoto3CM,
) -> None:
    await abcm.client("sts")
    abcm._client_lut[None]['groups'][None][None]['sts'] |= {
        "lock": "mock_lock"
    }
    await abcm.close("sts")
    # if this exists then it wasn't deleted
    assert "lock" in abcm._client_lut[None]['groups'][None][None]['sts']
    assert "aes" in abcm._client_lut[None]['groups'][None][None]['sts']
    assert "client" in abcm._client_lut[None]['groups'][None][None]['sts']
    abcm._client_lut[None]['groups'][None][None]['sts'].pop("lock")
    await abcm.close("sts")
    assert "sts" not in abcm._client_lut[None]['groups'][None][None]


async def test_close_all(abcm: AIOBoto3CM) -> None:
    sts_client = await abcm.client("sts")
    assert abcm._client_lut[None]['groups'][None][None]['sts']['client'] == sts_client
    sts_ue1_client = await abcm.client("sts", region_name="us-east-1")
    assert abcm._client_lut[None]['groups'][None]['us-east-1']['sts']['client'] == sts_ue1_client
    await abcm.close_all()
    with pytest.raises(KeyError):
        abcm._client_lut[None]['groups'][None][None]['sts']
    
    with pytest.raises(KeyError):
        abcm._client_lut[None]['groups'][None]['us-east-1']['sts']


async def test_close_all_no_clients(abcm: AIOBoto3CM) -> None:
    await abcm.close_all()


async def test_close_all_client_lock(
    abcm: AIOBoto3CM
) -> None:
    await abcm.client("sts")
    abcm._client_lut[None]['groups'][None][None]['sts'] |= {
        "lock": "mock_lock"
    }
    await abcm.client("sts", region_name="us-east-1")
    abcm._client_lut[None]['groups'][None]['us-east-1']['sts'] |= {
        "lock": "mock_lock"
    }
    await abcm.client("sts", region_name="us-east-2")
    await abcm.close_all()
    assert "lock" in abcm._client_lut[None]['groups'][None][None]['sts']
    assert "aes" in abcm._client_lut[None]['groups'][None][None]['sts']
    assert "client" in abcm._client_lut[None]['groups'][None][None]['sts']
    assert "lock" in abcm._client_lut[None]['groups'][None]['us-east-1']['sts']
    assert "aes" in abcm._client_lut[None]['groups'][None]['us-east-1']['sts']
    assert "client" in abcm._client_lut[None]['groups'][None]['us-east-1']['sts']
    assert "sts" not in abcm._client_lut[None]['groups'][None]['us-east-2']

    abcm._client_lut[None]['groups'][None][None].pop("sts")
    assert "sts" not in abcm._client_lut[None]['groups'][None][None]
    assert "lock" in abcm._client_lut[None]['groups'][None]['us-east-1']['sts']
    assert "aes" in abcm._client_lut[None]['groups'][None]['us-east-1']['sts']
    assert "client" in abcm._client_lut[None]['groups'][None]['us-east-1']['sts']

    abcm._client_lut[None]['groups'][None]['us-east-1'].pop("sts")
    assert "sts" not in abcm._client_lut[None]['groups'][None]['us-east-1']


async def test_client(
    abcm: AIOBoto3CM,
    sts_client: str
) -> None:    
    await sts_client.get_caller_identity()
    assert sts_client == await abcm.client("sts", region_name="us-east-1")


async def test_client_session_not_found(abcm: AIOBoto3CM, moto_server: str):
    with pytest.raises(SessionNotFoundError):
        await abcm.client("sts", region_name="us-east-1", abcm_session_name="not_found")
    
    abcm.register_session(aioboto3.Session(region_name="us-east-1"), "other_sess")
    sts_client = await abcm.client(
        "sts", 
        region_name="us-east-1", 
        endpoint_url=moto_server,
        abcm_session_name="other_sess",
        abcm_client_group="my_clients"
    )
    await sts_client.get_caller_identity()


async def test_client_different_sessions_and_groups(abcm: AIOBoto3CM) -> None:
    with pytest.raises(SessionNotFoundError):
        await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess1") 
    
    with pytest.raises(SessionNotFoundError):
        await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess2") 

    abcm.register_session(aioboto3.Session(), "sess1")
    abcm.register_session(aioboto3.Session(), "sess2")
    abcm.get_session("sess1") != abcm.get_session("sess2")
    
    sts_client1 = await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess1") 
    sts_client2 = await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess2") 
    assert sts_client1 == await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess1") 
    assert sts_client2 == await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess2") 
    assert sts_client1 != sts_client2
    assert (
        await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess1") 
        != await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess2") 
    )

    sts_client1 = await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess1", abcm_client_group="group1") 
    sts_client2 = await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess2", abcm_client_group="group2") 
    assert sts_client1 == await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess1", abcm_client_group="group1") 
    assert sts_client2 == await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess2", abcm_client_group="group2") 
    assert sts_client1 != sts_client2
    assert (
        await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess1", abcm_client_group="group1") 
        != await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess2", abcm_client_group="group2") 
    )
    assert (
        await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess1") 
        != await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess1", abcm_client_group="group1") 
    )
    assert (
        await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess2") 
        != await abcm.client("sts", region_name="us-east-1", abcm_session_name="sess2", abcm_client_group="group2") 
    )


async def test_client_default_kwargs() -> None:
    abcm = AIOBoto3CM()
    await abcm.client("sts")
    config = Config(region_name="us-west-2")
    abcm_defaults = AIOBoto3CM(default_client_kwargs={"config": config})
    sts_client_defaults = await abcm_defaults.client("sts")
    assert sts_client_defaults.meta.config.region_name == config.region_name


@asynccontextmanager
async def _fake_client(*args, **kwargs):
    await asyncio.sleep(1)

    yield None


async def test_client_aioboto3_fail_exception(
    abcm: AIOBoto3CM,
    moto_server: str
) -> None:
    sess = aioboto3.Session()
    abcm.register_session(sess)
    with patch.object(sess, "client", side_effect=Exception("something went wrong")):
        with pytest.raises(Exception):
            await abcm.client("sts")
    
    sts_client = await abcm.client("sts", region_name="us-east-1", endpoint_url=moto_server)
    await sts_client.get_caller_identity()


async def test_client_ready_after_lut_lock(
    abcm: AIOBoto3CM,
    new_aio_lock: Callable[[], asyncio.Lock]
) -> None:
    # Create a client, then manually remove it and acquire lock to emulate waiting for lock with no existing client
    await abcm.client("sts")
    sts_info = abcm._client_lut[None]['groups'][None][None].pop("sts")
    abcm._lock = new_aio_lock()
    await abcm._lock.acquire()
    task = asyncio.create_task(abcm.client("sts"))
    # sleep to let the task run until await
    await asyncio.sleep(1)
    abcm._client_lut[None]['groups'][None][None]['sts'] = sts_info
    abcm._lock.release()
    sts_client = await task
    # 2 each because initial client was before mock lock
    assert abcm._lock.acquire.call_count == 2
    assert abcm._lock.release.call_count == 2
    assert sts_info['client'] == sts_client


async def test_client_ready_after_client_lock(
    abcm: AIOBoto3CM,
    new_aio_lock: Callable[[], asyncio.Lock]
) -> None:
    # Create a client, then manually remove it and acquire lock to emulate waiting for lock with no existing client
    await abcm.client("sts")
    sts_info = abcm._client_lut[None]['groups'][None][None].pop("sts")
    abcm._lock = new_aio_lock()
    await abcm._lock.acquire()
    # emulate having the client lock
    client_lock = new_aio_lock()
    await client_lock.acquire()
    abcm._client_lut[None]['groups'][None][None]['sts'] = {
        "lock": client_lock
    }
    task = asyncio.create_task(abcm.client("sts"))
    # sleep to let the task run until await of lock
    await asyncio.sleep(1)
    # first wait for the lut lock
    # then the client lock
    abcm._lock.release()
    # sleep to let the task run until await of client lock
    await asyncio.sleep(1)
    abcm._client_lut[None]['groups'][None][None]['sts'] = sts_info
    client_lock.release()
    sts_client = await task
    assert abcm._lock.acquire.call_count == 2
    assert abcm._lock.release.call_count == 2
    assert client_lock.acquire.call_count == 2
    assert client_lock.release.call_count == 2
    assert sts_info['client'] == sts_client


async def test_client_init_create_failed(
    abcm: AIOBoto3CM,
    new_aio_lock: Callable[[], asyncio.Lock]
) -> None:
    # Create a client, then manually remove it and acquire lock to emulate waiting for lock with no existing client
    await abcm.client("sts")
    sts_info = abcm._client_lut[None]['groups'][None][None].pop("sts")
    abcm._lock = new_aio_lock()
    await abcm._lock.acquire()
    # emulate having the client lock
    client_lock = new_aio_lock()
    await client_lock.acquire()
    abcm._client_lut[None]['groups'][None][None]['sts'] = {
        "lock": client_lock
    }
    task = asyncio.create_task(abcm.client("sts"))
    # sleep to let the task run until await of lock
    await asyncio.sleep(1)
    # first wait for the lut lock
    # then the client lock
    abcm._lock.release()
    # sleep to let the task run until await of client lock
    await asyncio.sleep(1)
    abcm._client_lut[None]['groups'][None][None].pop("sts")
    client_lock.release()
    sts_client = await task
    # This is 3 because it tries to create the client over again
    assert abcm._lock.acquire.call_count == 3
    assert abcm._lock.release.call_count == 3
    # this remains 2 because the whole sts info section was popped
    assert client_lock.acquire.call_count == 2
    assert client_lock.release.call_count == 2
    # should be whole new client because the initial failed
    assert sts_info['client'] != sts_client


async def test_client_both_create_failed(
    abcm: AIOBoto3CM,
    new_aio_lock: Callable[[], asyncio.Lock],
    moto_server: str
) -> None:
    sess = aioboto3.Session()
    abcm.register_session(sess)
    # Create a client, then manually remove it and acquire lock to emulate waiting for lock with no existing client
    await abcm.client("sts")
    sts_info = abcm._client_lut[None]['groups'][None][None].pop("sts")
    abcm._lock = new_aio_lock()
    await abcm._lock.acquire()
    # emulate having the client lock
    client_lock = new_aio_lock()
    await client_lock.acquire()
    abcm._client_lut[None]['groups'][None][None]['sts'] = {
        "lock": client_lock
    }
    task = asyncio.create_task(abcm.client("sts"))
    # sleep to let the task run until await of lock
    await asyncio.sleep(1)
    # first wait for the lut lock
    # then the client lock
    abcm._lock.release()
    # sleep to let the task run until await of client lock
    await asyncio.sleep(1)
    abcm._client_lut[None]['groups'][None][None].pop("sts")
    client_lock.release()
    with patch.object(sess, "client", side_effect=Exception("something went wrong")):
        with pytest.raises(Exception):
            await task
    
    # This is 3 because it tries to create the client over again
    assert abcm._lock.acquire.call_count == 3
    assert abcm._lock.release.call_count == 3
    # this remains 2 because the whole sts info section was popped
    assert client_lock.acquire.call_count == 2
    assert client_lock.release.call_count == 2

    # even after all failures can still create the client and use it
    sts_client = await abcm.client("sts", region_name="us-east-1", endpoint_url=moto_server)
    assert sts_client != sts_info['client']
    await sts_client.get_caller_identity()

