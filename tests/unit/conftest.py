
import asyncio
from functools import partial
import os
from typing import Any, AsyncGenerator, Callable, Generator

from moto import mock_aws
from moto.server import ThreadedMotoServer
import pytest
from pytest_mock import MockerFixture

from aioboto3_cm import AIOBoto3CM


@pytest.fixture(scope="session")
def moto_creds() -> Generator[None, None, None]:
    cred_env_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN"
    ]
    aws_creds = {}
    for env_var in cred_env_vars:
        aws_creds[env_var] = os.environ.get(env_var, None)
        os.environ[env_var] = "testing"

    yield

    for cred in aws_creds:
        if aws_creds[cred] is None:
            os.environ.pop(cred)
        else:
            os.environ[cred] = aws_creds[cred]


@pytest.fixture(scope="function")
def moto_server(moto_creds: None) -> Generator[str, None, None]:
    server = ThreadedMotoServer()
    server.start()

    yield "http://localhost:5000"

    server.stop()


@pytest.fixture(scope="function")
def role_arn() -> str:
    return "arn:aws:iam::123412341234:role/my_role"


@pytest.fixture(scope="function")
def session_name() -> str:
    return "tester-session"


@pytest.fixture(scope="function")
def sts_arn() -> str:
    return "arn:aws:sts::123412341234:assumed-role/my_role/tester-session"


@pytest.fixture(scope="function")
async def abcm() -> AsyncGenerator[str, None]:
    _abcm = AIOBoto3CM()
    yield _abcm

    await _abcm.close_all()


@pytest.fixture(scope="function")
async def sts_client(moto_server: str, abcm: AIOBoto3CM) -> AsyncGenerator[Any, None]:
    sts_client = await abcm.client("sts", region_name="us-east-1", endpoint_url=moto_server)
    
    yield sts_client

    await abcm.close("sts", region_name="us-east-1")


def _new_aio_lock(mocker: MockerFixture) -> asyncio.Lock:
    lock = asyncio.Lock()
    mocker.spy(lock, "acquire")
    mocker.spy(lock, "release")

    return lock


@pytest.fixture(scope="function")
def new_aio_lock(mocker: MockerFixture) -> Callable[[], asyncio.Lock]:
    return partial(_new_aio_lock, mocker)