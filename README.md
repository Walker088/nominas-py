# nominas-py
Simple analysis of the Paraguayan official officers, data source: https://datos.hacienda.gov.py/data/nomina/descargas

## Config File

```toml
[postgres]
HOST = "localhost"
PORT = "5432"
DB = "your_postgres_db_name"
USER = "your_postgres_user"
PASS = "your_postgres_pass"

[nominas]
RESOURCE = "https://datos.hacienda.gov.py/odmh-core/rest/nomina/datos"
ST_MONTH_YEAR = "2013-12"
FORCE_DOWNLOAD = false
```
