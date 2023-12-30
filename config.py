from typing import TypedDict

import tomllib
import pathlib
    
class PGConf(TypedDict):
    host: str
    port: int
    dbname: str
    user: str
    password: str

class NominasConf(TypedDict):
    resource: str
    st_month_year: str
    force_download: bool

class Config(TypedDict):
    pg: PGConf
    nominas: NominasConf

config_file_path = "config.toml"

def get_config() -> pathlib.Path:
    return pathlib.Path(config_file_path)

def read_config(cf: pathlib.Path):
    read = tomllib.loads(cf.read_text())
    read_pg = read.get("postgres", {})
    read_nomina = read.get("nominas", {})
    pgconf: PGConf = {
        "host": read_pg.get("HOST", "localhost"),
        "port": read_pg.get("PORT", 5432),
        "dbname": read_pg.get("DB", "infopy"),
        "user": read_pg.get("USER", "postgres"),
        "password": read_pg.get("PASS", "postgres")
    }
    nomina_conf: NominasConf = {
        "resource": read_nomina.get("RESOURCE", "https://datos.hacienda.gov.py/odmh-core/rest/nomina/datos/"),
        "st_month_year": read_nomina.get("ST_MONTH_YEAR", "2013-01"),
        "force_download": read_nomina.get("FORCE_DOWNLOAD", False)
    }
    conf: Config = {
        "pg": pgconf,
        "nominas": nomina_conf
    }
    return conf
