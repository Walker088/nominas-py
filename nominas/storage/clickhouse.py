import clickhouse_connect
from clickhouse_connect.driver.client import Client

from nominas.config.config import ClickhouseConf
from nominas.logger.logger import logger


class NominaChClient:
    _client: Client = None  # type: ignore

    @property
    def client(self):
        return self._client

    def __init__(self) -> None:
        self.log = logger().getLogger("NominaChClient")

    def start(self, conf: ClickhouseConf):
        self.log.info("Starting connection client...")
        if self._client is not None:
            self.log.info("Client had started already, do nothing")
            return
        self._client = clickhouse_connect.get_client(
            host=conf.host,
            port=conf.port,
            database=conf.dbname,
            username=conf.user,
            password=conf.password,
            secure=False,
        )
        self.log.info("Started connection client...")

    def teardown(self):
        if self._client is None:
            self.log.info("Client had been stopped already, do nothing")
            return
        self.client.close()


ch_client_mgr = NominaChClient()
