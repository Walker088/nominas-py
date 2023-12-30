import requests, zipfile, io, csv

from hashlib import sha256
from io import TextIOWrapper

from config import NominasConf, get_config, read_config

def download():
    rq = requests.get('https://datos.hacienda.gov.py/odmh-core/rest/nomina/datos/nomina_2016-01.zip')
    zf = zipfile.ZipFile(io.BytesIO(rq.content))
    with zf.open('nomina_2016-01.csv', 'r') as csv_file:
        reader = csv.reader(TextIOWrapper(csv_file, 'iso-8859-1'))
        title = next(reader)
        checksum = sha256(",".join(title).encode())
        for _ in range(10):
            row = next(reader)
            checksum.update(",".join(row).encode())
        print(checksum.hexdigest())

def import_csv_to_pg():
    pass


download()
