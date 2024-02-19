import pathlib
import tomllib
from dataclasses import dataclass


@dataclass
class PGConf:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    connect_timeout: int
    application_name: str

    def to_conn_str(self):
        return f"host={self.host} port={self.port} dbname={self.dbname} user={self.user} password={self.password} connect_timeout={self.connect_timeout} application_name={self.application_name}"


@dataclass
class ClickhouseConf:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass
class NominasConf:
    resource: str
    # st_month_year: str
    # force_download: bool
    enable_hisag: bool


@dataclass
class Config:
    pg: PGConf
    ch: ClickhouseConf
    nominas: NominasConf


config_file_path = "config.toml"


def get_config() -> pathlib.Path:
    return pathlib.Path(config_file_path)


def read_config(cf: pathlib.Path):
    read = tomllib.loads(cf.read_text())
    read_app = read.get("app", {})

    read_clickhouse = read.get("clickhouse", {})
    clickhouse_conf = ClickhouseConf(
        host=read_clickhouse.get("HOST", "localhost"),
        port=read_clickhouse.get("PORT", 8123),
        dbname=read_clickhouse.get("DB", "infopy"),
        user=read_clickhouse.get("USER", "default"),
        password=read_clickhouse.get("PASS", ""),
    )

    read_pg = read.get("postgres", {})
    pgconf = PGConf(
        host=read_pg.get("HOST", "localhost"),
        port=read_pg.get("PORT", 5432),
        dbname=read_pg.get("DB", "infopy"),
        user=read_pg.get("USER", "postgres"),
        password=read_pg.get("PASS", "postgres"),
        connect_timeout=read_pg.get("CONN_TIMEOUT", 10),
        application_name=read_app.get("APP_NAME", "nominas-py"),
    )

    read_nomina = read.get("nominas", {})
    nomina_conf = NominasConf(
        resource=read_nomina.get(
            "RESOURCE", "https://datos.hacienda.gov.py/odmh-core/rest/nomina/datos/"
        ),
        # st_month_year=read_nomina.get("ST_MONTH_YEAR", "2013-01"),
        # force_download=read_nomina.get("FORCE_DOWNLOAD", False),
        enable_hisag=read_nomina.get("ENABLE_HISAG", False),
    )
    conf: Config = Config(pg=pgconf, ch=clickhouse_conf, nominas=nomina_conf)
    return conf


app_config = read_config(get_config())
