import asyncio
from aiohttp import TCPConnector, ClientSession, ClientResponse
import logging
from os.path import isfile
from os import remove
from abc import ABC, abstractproperty, abstractmethod
import time

logger = logging.getLogger(__name__)


class BulkException(Exception):
    pass


class BulkRequestsJob(ABC):

    @abstractproperty
    def url(self) -> str:
        pass

    @abstractmethod
    def done(self) -> bool:
        pass

    @abstractmethod
    def undone(self):
        pass

    async def requests(self, session: ClientSession) -> bool:
        async with self.get(session) as content:
            return self.do(content)

    def get(self, session: ClientSession):
        return session.get(self.url)

    @abstractmethod
    async def do(self, content: str) -> bool:
        pass


class BulkRequestsFileJob(BulkRequestsJob):
    @abstractproperty
    def file(self) -> str:
        pass

    def done(self) -> bool:
        return isfile(self.file)

    def undone(self):
        if self.done():
            remove(self.file)

    async def do(self, response: ClientResponse) -> bool:
        content = await response.text()
        with open(self.file, "w") as f:
            f.write(content)
        return True


class BulkRequests:
    def __init__(
            self,
            tcp_limit: int = 10,
            tries: int = 4,
            sleep: int = 10
    ):
        self.tcp_limit = tcp_limit
        self.tries = tries
        self.sleep = sleep

    async def __requests(self, session: ClientSession, job: BulkRequestsJob):
        try:
            async with job.get(session) as response:
                return await job.do(response)
        except Exception:
            logger.exception()

    async def __requests_all(self, *job: BulkRequestsJob):
        my_conn = TCPConnector(limit=self.tcp_limit)
        async with ClientSession(connector=my_conn) as session:
            tasks = []
            for u in job:
                task = asyncio.ensure_future(
                    self.__requests(session=session, job=u)
                )
                tasks.append(task)
            rt = await asyncio.gather(*tasks, return_exceptions=True)
        return rt

    def run(self, *job: BulkRequestsJob, overwrite=False):
        if overwrite:
            for u in job:
                u.undone()
        self.__run(*job)

    def __run(self, *job: BulkRequestsJob):
        ko = 0
        for i in range(max(self.tries, 1)):
            job = tuple(u for u in job if not u.done())
            if len(job) == 0:
                return
            if i == 0:
                logger.info(
                    'BulkRequests' +
                    f'(tcp_limit={self.tcp_limit}).run({len(job)} items)'
                )
            else:
                time.sleep(self.sleep)
            rt = asyncio.run(self.__requests_all(*job))
            ko = len([i for i in rt if i is not True])
            if ko == 0:
                return
        raise BulkException(f"{ko} missing")
