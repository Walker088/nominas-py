import csv
import hashlib
import io
import traceback
import zipfile
from typing import IO, Set

import backoff
import requests
from dacite import from_dict
from psycopg.rows import dict_row
from tqdm import tqdm

from nominas.config.config import app_config
from nominas.logger.logger import logger
from nominas.pipeline.postprocess.postprocess import insert_download_history
from nominas.pipeline.postprocess.types import DownloadHistory
from nominas.pipeline.preprocess.preprocess import (
    get_saved_entidades,
    get_saved_niveles,
    get_saved_objecto_gastos,
    get_saved_personas,
    get_saved_programas,
    get_saved_proyectos,
    get_saved_pub_officers,
    get_saved_unidad_responsables,
    to_entidad,
    to_nivel,
    to_objecto_gasto,
    to_persona,
    to_programa,
    to_proyecto,
    to_pub_officer,
    to_unidad_responsable,
)
from nominas.pipeline.preprocess.types import (
    Entidad,
    Nivel,
    ObjectoGasto,
    Persona,
    ProcessedCsvItems,
    Programa,
    Proyecto,
    PubOfficer,
    RawCsvItem,
    UnidadResponsable,
)
from nominas.pipeline.store.clickhouse import store_to_clickhouse
from nominas.storage.clickhouse import ch_client_mgr
from nominas.storage.postgres import pgpool_mgr

log = logger().getLogger("Main")


@backoff.on_exception(
    wait_gen=backoff.expo,
    exception=requests.ConnectionError,
    max_time=60,
    max_tries=5,
)
def backoff_get(dest: str):
    return requests.get(dest)

buffer_size = 4096

def was_file_downloaded(resource_url: str, check_sum: str) -> bool:
    was_file_downloaded = False
    query = """
    SELECT 
	    EXISTS (
	    	SELECT
	    		1
	    	FROM
	    		py_nomina_download_history h
	    	WHERE
	    		resource_url = %(resource_url)s
	    		AND check_sum = %(check_sum)s
                AND stat = %(stat)s
	    ) was_file_downloaded
    """
    with pgpool_mgr.pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rs = cur.execute(
                query, {"resource_url": resource_url, "check_sum": check_sum, "stat": "SUCCEED"}
            ).fetchone()
            if rs is not None:
                was_file_downloaded = rs["was_file_downloaded"]
    return was_file_downloaded

def get_downloaded_histories() -> list[str]:
    query = """
    SELECT
    	h.resource_url
    FROM
    	py_nomina_download_history h
    WHERE
    	h.stat = 'SUCCEED'
    GROUP BY
    	resource_url
    """
    with pgpool_mgr.pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rs = cur.execute(query).fetchall()
            return [r["resource_url"] for r in rs]

def get_csv_file_size(file: IO[bytes]):
    return len(file.read())


def get_csv_file_entries(csv_with_io_wrapper: io.TextIOWrapper) -> int:
    return sum(1 for _ in csv.reader(csv_with_io_wrapper))


def get_csv_file_hash(file: IO[bytes]) -> str:
    md5sum = hashlib.md5()
    for chunk in iter(lambda: file.read(buffer_size), b''):
        md5sum.update(chunk)
    return md5sum.hexdigest()


def download_zip_to_ch(pbar: tqdm, anio_mes: str, csv_with_io_wrapper: io.TextIOWrapper):
    try:
        pbar.set_description(f"[{anio_mes}] Initializing resources")

        saved_personas = get_saved_personas(ch_client_mgr.client)
        saved_niveles = get_saved_niveles(ch_client_mgr.client)
        saved_entidades = get_saved_entidades(ch_client_mgr.client)
        saved_programas = get_saved_programas(ch_client_mgr.client)
        saved_proyectos = get_saved_proyectos(ch_client_mgr.client)
        saved_unidad_responsables = get_saved_unidad_responsables(ch_client_mgr.client)
        saved_objecto_gastos = get_saved_objecto_gastos(ch_client_mgr.client)
        saved_pub_officers = get_saved_pub_officers(ch_client_mgr.client)

        personas: Set[Persona] = set()
        niveles: Set[Nivel] = set()
        entidades: Set[Entidad] = set()
        programs: Set[Programa] = set()
        proyectos: Set[Proyecto] = set()
        unidades: Set[UnidadResponsable] = set()
        objecto_gastos: Set[ObjectoGasto] = set()
        codigo_eventos: dict[str, int] = {}
        pub_officers: Set[PubOfficer] = set()

        pbar.set_description(f"[{anio_mes}] Reading csv file...")
        reader = csv.DictReader(csv_with_io_wrapper)
        pbar.set_description(f"[{anio_mes}] Preprocessing raw csv file...")
        for row in tqdm(reader, desc=f"[{anio_mes}]"):
            raw = from_dict(data_class=RawCsvItem, data=row)
            codigo_evento = f"{raw.anio}{raw.mes}-{raw.codigoPersona}"
            if codigo_evento in codigo_eventos.keys():
                codigo_eventos[codigo_evento] += 1
            else:
                codigo_eventos[codigo_evento] = 0

            if raw.codigoPersona not in saved_personas:
                personas.add(to_persona(raw))

            nivel_key = f"{raw.codigoNivel.strip()}-{raw.nivelAbr.strip()}"
            if nivel_key not in saved_niveles:
                niveles.add(to_nivel(nivel_key, raw))

            entidad_key = f"{raw.codigoEntidad.strip()}-{raw.entidadAbr.strip()}"
            if entidad_key not in saved_entidades:
                entidades.add(to_entidad(entidad_key, raw))

            program_key = f"{raw.codigoPrograma.strip()}-{raw.codigoSubprograma.strip()}-{raw.programaAbr.strip()}-{raw.subprogramaAbr.strip()}"
            if program_key not in saved_programas:
                programs.add(to_programa(program_key, raw))

            proyecto_key = f"{raw.codigoProyecto.strip()}-{raw.proyectoAbr.strip()}"
            if proyecto_key not in saved_proyectos:
                proyectos.add(to_proyecto(proyecto_key, raw))

            unidad_responsable_key = (
                f"{raw.codigoUnidadResponsable.strip()}-{raw.unidadAbr.strip()}"
            )
            if unidad_responsable_key not in saved_unidad_responsables:
                unidades.add(to_unidad_responsable(unidad_responsable_key, raw))

            codigo_objecto_gasto = int(raw.codigoObjetoGasto.strip())
            if codigo_objecto_gasto not in saved_objecto_gastos:
                objecto_gastos.add(to_objecto_gasto(codigo_objecto_gasto, raw))

            if f"{codigo_evento}{codigo_eventos[codigo_evento]}" not in saved_pub_officers:
                pub_officers.add(to_pub_officer(raw, codigo_evento, codigo_eventos[codigo_evento]))
        return store_to_clickhouse(
            ch_client_mgr.client,
            ProcessedCsvItems(
                personas=personas,
                niveles=niveles,
                entidades=entidades,
                programas=programs,
                proyectos=proyectos,
                unidades=unidades,
                objecto_gastos=objecto_gastos,
                pub_officers=pub_officers,
            ),
        )
    except Exception as e:
        log.error(e)


def sync_data_from_hecienda_py(dest: str, force: bool):
    try:
        log.info(f"Staring to sync data from {dest} to local storage...")
        rq = backoff_get(dest=dest)
        if rq.status_code != 200:
            log.error(f"the hacienda api is not working well: [{rq.status_code}] {rq.text}")
            return
        available_data = rq.json()
        downloaded_histories = get_downloaded_histories()
        if isinstance(available_data, list):
            for item in (pbar := tqdm(available_data)):
                anio_mes = item["periodo"]
                pbar.set_description(f"downloading nomina_{anio_mes}.zip")
                resource_url = f"https://datos.hacienda.gov.py/odmh-core/rest/nomina/datos/nomina_{anio_mes}.zip"
                if (resource_url in downloaded_histories and not force):
                    pbar.set_description(f"skipping nomina_{anio_mes}.zip")
                    continue
                rq = backoff_get(resource_url)
                pbar.set_description(f"[{anio_mes}] reading zip file")
                with zipfile.ZipFile(io.BytesIO(rq.content)) as zf:
                    pbar.set_description(f"[{anio_mes}] unzipped csv file")
                    download_history = DownloadHistory(
                        download_id=None,
                        resource_url=resource_url,
                        check_sum=None,
                        entries=None,
                        download_at_utc=None,
                        was_succeed=None,
                    )
                    with zf.open(f"nomina_{anio_mes}.csv", "r") as csv_file:
                        file_hash = get_csv_file_hash(csv_file)
                        download_history.check_sum = file_hash
                        if was_file_downloaded(resource_url, file_hash):
                            pbar.set_description(f"skipping nomina_{anio_mes}.zip")
                            continue

                    with zf.open(f"nomina_{anio_mes}.csv", "r") as csv_file:
                        csv_with_io_wrapper = io.TextIOWrapper(csv_file, "iso-8859-1")
                        download_history.entries = get_csv_file_entries(csv_with_io_wrapper)

                    with zf.open(f"nomina_{anio_mes}.csv", "r") as csv_file:
                        csv_with_io_wrapper = io.TextIOWrapper(csv_file, "iso-8859-1")
                        download_history.was_succeed = download_zip_to_ch(pbar, anio_mes, csv_with_io_wrapper)
                        insert_download_history(download_history)

                    if not download_history.was_succeed:
                        with zf.open(f"nomina_{anio_mes}.csv", "r") as csv_file:
                            with open(f"nomina_{anio_mes}.csv", "wb") as failed_file:
                                for chunk in iter(lambda: csv_file.read(buffer_size), b''):
                                    failed_file.write(chunk)

        else:
            log.error(
                f"the response of the hacienda api is not a list of zip files: [{rq.status_code}] {rq.text}"
            )
            return
    except Exception as e:
        traceback.print_exc()
        log.error(f"Error occured upon synchronizing data from hacienda api: {e}")


if __name__ == '__main__':
    log.info("Welcome to nominas-py")
    try:
        pgpool_mgr.start(app_config.pg.to_conn_str())
        ch_client_mgr.start(app_config.ch)
        sync_data_from_hecienda_py(app_config.nominas.resource, app_config.nominas.force_download)
    finally:
        pgpool_mgr.teardown()
