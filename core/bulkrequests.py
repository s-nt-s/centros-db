import asyncio
from aiohttp import TCPConnector, ClientSession, ClientResponse
import logging
from os.path import isfile
from os import remove
from abc import ABC, abstractproperty, abstractmethod
from .retry import retry

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
            tcp_limit: int = 10
    ):
        self.tcp_limit = tcp_limit

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

    @retry(times=3, sleep=10, exceptions=BulkException)
    def __run(self, *job: BulkRequestsJob):
        job = tuple(u for u in job if not u.done())
        logger.info(
            'BulkRequests' +
            f'(tcp_limit={self.tcp_limit}).run({len(job)} items)'
        )
        if len(job) == 0:
            return
        rt = asyncio.run(self.__requests_all(*job))
        ok = len([i for i in rt if i is True])
        logger.info(f'{ok} urls downloaded')
        if (ok < len(job)):
            raise BulkException(f"{len(job)-ok} missing")
