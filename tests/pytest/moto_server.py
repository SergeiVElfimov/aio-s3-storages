from __future__ import annotations

import asyncio
import functools
import logging
import os
import socket
import threading
import time
import typing

import aiohttp
import werkzeug.serving
from moto.server import DomainDispatcherApplication, create_backend_app

if typing.TYPE_CHECKING:
    from collections import abc

    from _typeshed import Unused

    P = typing.ParamSpec("P")
    R = typing.TypeVar("R")


host = "127.0.0.1"

_PYCHARM_HOSTED = os.environ.get("PYCHARM_HOSTED") == "1"
_CONNECT_TIMEOUT = aiohttp.client.ClientTimeout(total=90.0 if _PYCHARM_HOSTED else 10.0)


def get_free_tcp_port(release_socket: bool = False) -> tuple[socket.socket, int]:
    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.bind((host, 0))
    addr, port = sckt.getsockname()
    if release_socket:
        sckt.close()
        return port

    return sckt, port


class MotoService:
    """The service is reference counted, so only one will enter the process.

    The real service will be returned from `__aenter__`.
    """

    _thread: threading.Thread
    _server: werkzeug.serving.BaseWSGIServer
    _refcount: int
    _services: dict[str, typing.Self] = {}

    def __init__(self, service_name: str, port: int = 0, ssl: bool = False) -> None:
        self._service_name = service_name

        if port:
            self._socket, self._port = None, port
        else:
            self._socket, self._port = get_free_tcp_port()

        self._logger = logging.getLogger("MotoService")
        self._refcount = 0
        self._ip_address = host
        self._ssl_ctx = werkzeug.serving.generate_adhoc_ssl_context() if ssl else None
        self._schema = "http" if not self._ssl_ctx else "https"

    @property
    def endpoint_url(self) -> str:
        return f"{self._schema}://{self._ip_address}:{self._port}"

    def __call__(self, func: abc.Callable[P, abc.Awaitable[R]]) -> abc.Callable[P, abc.Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            await self._start()
            try:
                result = await func(*args, **kwargs)
            finally:
                await self._stop()
            return result

        functools.update_wrapper(wrapper, func)
        wrapper.__wrapped__ = func
        return wrapper

    async def __aenter__(self) -> typing.Self:
        svc = self._services.get(self._service_name)
        if svc is None:
            self._services[self._service_name] = self
            self._refcount = 1
            await self._start()
            return self
        else:
            svc._refcount += 1
            return svc

    async def __aexit__(self, *exc_info: Unused) -> None:
        self._refcount -= 1

        if self._socket:
            self._socket.close()
            self._socket = None

        if self._refcount == 0:
            del self._services[self._service_name]
            await self._stop()

    def _server_entry(self) -> None:
        self._main_app = DomainDispatcherApplication(create_backend_app)

        if self._socket:
            self._socket.close()  # free it properly before using it
            self._socket = None

        self._server = werkzeug.serving.make_server(
            self._ip_address, self._port, self._main_app, threaded=True, ssl_context=self._ssl_ctx
        )
        self._server.serve_forever()

    async def _start(self) -> None:
        self._thread = threading.Thread(target=self._server_entry, daemon=True)
        self._thread.start()

        async with aiohttp.ClientSession() as session:
            start = time.time()

            while time.time() - start < 20:
                if not self._thread.is_alive():
                    break

                try:
                    # we need to bypass the proxies due to monkeypatches
                    async with session.get(self.endpoint_url + "/static", timeout=_CONNECT_TIMEOUT, ssl=False):
                        pass
                    break
                except (TimeoutError, aiohttp.ClientConnectionError):
                    await asyncio.sleep(0.5)
            else:
                await self._stop()  # pytest.fail doesn't call stop_process
                raise Exception(f"Can not start service: {self._service_name}")

    async def _stop(self) -> None:
        if self._server:
            self._server.shutdown()

        self._thread.join()
