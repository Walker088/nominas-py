from multiprocessing import cpu_count

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from nominas.logger.logger import logger


class NominaPgPool:
    _pool: ConnectionPool = None  # type: ignore

    @property
    def pool(self):
        return self._pool

    def __init__(self) -> None:
        self.log = logger().getLogger("NominaPgPool")

    def start(self, conninfo: str):
        self.log.info("Starting connection pool...")
        if self._pool is not None:
            self.log.info("Pool had started already, do nothing")
            return
        self._pool = ConnectionPool(
            conninfo=conninfo,
            min_size=10 // cpu_count(),
            max_size=30 // cpu_count(),
            max_waiting=10,
            open=True,
            kwargs={"row_factory": dict_row},
        )
        self.log.info("Connection pool started")

    def teardown(self):
        self._pool.close()
        self.log.info("Connection pool shutted down")


nomina_pool_manager = NominaPgPool()
