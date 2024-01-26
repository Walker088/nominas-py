import io
import traceback
import zipfile

import backoff
import requests
from tqdm import tqdm

from nominas.config.config import app_config
from nominas.logger.logger import logger
from nominas.postgres.postgres import nomina_pool_manager

log = logger().getLogger("Main")


@backoff.on_exception(
    wait_gen=backoff.expo,
    exception=requests.ConnectionError,
    max_time=60,
    max_tries=5,
)
def get(dest: str):
    return requests.get(dest)


def download_zip_to_pg(pbar: tqdm, anio_mes: str):
    pbar.set_description(f"downloading nomina_{anio_mes}.zip")
    rq = get(f"https://datos.hacienda.gov.py/odmh-core/rest/nomina/datos/nomina_{anio_mes}.zip")
    pbar.set_description("reading zip file")
    zf = zipfile.ZipFile(io.BytesIO(rq.content))
    pbar.set_description("reading csv file")
    with zf.open(f"nomina_{anio_mes}.csv", "r") as csv_file:
        with nomina_pool_manager.pool.connection() as conn:
            with conn.cursor() as cur:
                try:
                    pbar.set_description(f"[{anio_mes}]creating tmp table")
                    cur.execute(
                        """
                    CREATE TEMP TABLE tmp_nomina_csv_raw (
                        anio INT4,
                        mes INT4,
                        "codigoNivel" INT4,
                        "descripcionNivel" TEXT,
                        "codigoEntidad" INT4,
                        "descripcionEntidad" TEXT,
                        "codigoPrograma" INT4,
                        "descripcionPrograma" TEXT,
                        "codigoSubprograma" INT4,
                        "descripcionSubprograma" TEXT,
                        "codigoProyecto" INT4,
                        "descripcionProyecto" TEXT,
                        "codigoUnidadResponsable" INT4,
                        "descripcionUnidadResponsable" TEXT,
                        "codigoObjetoGasto" INT4,
                        "conceptoGasto" TEXT,
                        "fuenteFinanciamiento" TEXT,
                        linea TEXT,
                        "codigoPersona" TEXT,
                        nombres TEXT,
                        apellidos TEXT,
                        sexo TEXT,
                        discapacidad TEXT,
                        "codigoCategoria" TEXT,
                        cargo TEXT,
                        "horasCatedra" INT4,
                        "fechaIngreso" TEXT,
                        "tipoPersonal" TEXT,
                        lugar TEXT,
                        "montoPresupuestado" NUMERIC(16, 4),
                        "montoDevengado" NUMERIC(16, 4),
                        "mesCorte" INT4,
                        "anioCorte" INT4,
                        "fechaCorte" TEXT,
                        "nivelAbr" TEXT,
                        "entidadAbr" TEXT,
                        "programaAbr" TEXT,
                        "subprogramaAbr" TEXT,
                        "proyectoAbr" TEXT,
                        "unidadAbr" TEXT
                    ) ON COMMIT DROP
                    """
                    )
                    pbar.set_description(f"[{anio_mes}] created tmp_nomina_csv_raw")
                    pbar.set_description(f"[{anio_mes}] created tmp_nomina_csv_raw")
                    with cur.copy(
                        "COPY tmp_nomina_csv_raw FROM STDIN DELIMITER ',' CSV HEADER ENCODING 'iso-8859-1'"
                    ) as copy:
                        copy.write(csv_file.read())
                    pbar.set_description(f"[{anio_mes}] loaded csv to tmp_nomina_csv_raw")
                    cur.execute(
                        """
                    INSERT INTO py_personas (
                    	codigo_persona,
                    	nombres,
                    	apellidos,
                    	sexo,
                    	discapacidad
                    )
                    SELECT
                    	nc."codigoPersona",
                    	TRIM(nc.nombres),
                    	TRIM(nc.apellidos),
                    	CASE COALESCE(nc.sexo, 'NULL')
                    		WHEN 'NULL' THEN NULL
                    		WHEN 'M' THEN 'M'
                    		WHEN 'F' THEN 'F'
                    		ELSE LEFT(nc.sexo, 1)
                    	END,
                    	CASE COALESCE(nc.discapacidad, 'OTHERS')
                    		WHEN 'OTHERS' THEN NULL
                    		WHEN 'N' THEN 'N'
                    		WHEN 'S' THEN 'Y'
                    		ELSE LEFT(nc.discapacidad, 1)
                    	END
                    FROM
                    	tmp_nomina_csv_raw nc
                    ON CONFLICT DO NOTHING
                    """
                    )
                    pbar.set_description(f"[{anio_mes}] loaded to py_personas")
                    cur.execute(
                        """
                    INSERT INTO py_nomina_funcionarios_publicos_niveles (
                    	codigo_nivel,
                    	nivel_abr,
                    	descripcion_nivel
                    )
                    SELECT
                    	nc."codigoNivel",
                    	TRIM(nc."nivelAbr"),
                    	TRIM(nc."descripcionNivel")
                    FROM
                    	tmp_nomina_csv_raw nc
                    GROUP BY
                        nc."codigoNivel",
                    	TRIM(nc."nivelAbr"),
                    	TRIM(nc."descripcionNivel")
                    ON CONFLICT (codigo_nivel, nivel_abr)
                        DO UPDATE SET
                        	descripcion_nivel = EXCLUDED.descripcion_nivel
                    """
                    )
                    pbar.set_description(f"[{anio_mes}] loaded to niveles")
                    cur.execute(
                        """
                    INSERT INTO py_nomina_funcionarios_publicos_entidades (
                    	codigo_entidad,
                    	entidad_abr,
                    	descripcion_entidad
                    )
                    SELECT
                    	nc."codigoEntidad",
                    	trim(nc."entidadAbr"),
                    	trim(nc."descripcionEntidad")
                    FROM
                    	tmp_nomina_csv_raw nc
                    GROUP BY
                    	nc."codigoEntidad",
                    	trim(nc."entidadAbr")
                    ON CONFLICT (codigo_entidad, entidad_abr)
                        DO UPDATE SET
                        	descripcion_entidad = EXCLUDED.descripcion_entidad
                    """
                    )
                    pbar.set_description(f"[{anio_mes}] loaded to entidades")
                    cur.execute(
                        """
                    INSERT INTO py_nomina_funcionarios_publicos_programas (
                    		hash_id,
                        	codgio_programa,
                        	codigo_sub_programa,
                        	programa_abr,
                        	sub_programa_abr,
                        	descripcion_programa,
                        	descripcion_sub_programa
                        )
                    SELECT
                    	encode(
                            sha256(
                    		    convert_to(
                    		        CONCAT(
                    			        nc."codigoPrograma"::TEXT,
                    			        nc."codigoSubprograma"::TEXT,
                    			        TRIM(nc."programaAbr"),
                    			        TRIM(nc."subprogramaAbr"),
                    			        TRIM(nc."descripcionPrograma"),
                    			        TRIM(nc."descripcionSubprograma")
                    		        ),
                                    'UTF8'
                    		    )
                    	    ),
                            'hex'
                        ),
                    	nc."codigoPrograma",
                    	nc."codigoSubprograma",
                    	TRIM(nc."programaAbr"),
                    	TRIM(nc."subprogramaAbr"),
                    	TRIM(nc."descripcionPrograma"),
                    	TRIM(nc."descripcionSubprograma")
                    FROM
                    	tmp_nomina_csv_raw nc
                    GROUP BY
                    	nc."codigoPrograma",
                    	nc."codigoSubprograma",
                    	TRIM(nc."programaAbr"),
                    	TRIM(nc."subprogramaAbr"),
                    	TRIM(nc."descripcionPrograma"),
                    	TRIM(nc."descripcionSubprograma")
                    ON CONFLICT (hash_id) DO NOTHING
                    """
                    )
                    pbar.set_description(f"[{anio_mes}] loaded to programas")
                    cur.execute(
                        """
                    INSERT INTO py_nomina_funcionarios_publicos_proyectos (
                    hash_id,
                    	codigo_proyecto,
                    	proyecto_abr,
                    	descripcion_proyecto
                    )
                    SELECT
                    	encode(
                            sha256(
                    		    convert_to(
                    		        CONCAT(
                    			        nc."codigoProyecto",
	                                    TRIM(nc."proyectoAbr"),
	                                    TRIM(nc."descripcionProyecto")
                    		        ),
                                    'UTF8'
                    		    )
                    	    ),
                            'hex'
                        ),
                    	nc."codigoProyecto",
                    	TRIM(nc."proyectoAbr"),
                    	TRIM(nc."descripcionProyecto")
                    FROM
                    	tmp_nomina_csv_raw nc
                    GROUP BY
                    	nc."codigoProyecto",
                    	TRIM(nc."proyectoAbr"),
                    	TRIM(nc."descripcionProyecto")
                    ON CONFLICT (hash_id) DO NOTHING
                    """
                    )
                    pbar.set_description(f"[{anio_mes}] loaded to proyectos")
                    cur.execute(
                        """
                    INSERT INTO py_nomina_funcionarios_publicos_unidades_responsables (
                    	hash_id,
                        	codigo_unidad_responsable,
                        	unidad_abr,
                        	descripcion_unidad_responsable
                        )
                    SELECT
                    	encode(sha256(
                    		convert_to(
                    		CONCAT(
                    			nc."codigoUnidadResponsable",
                    			TRIM(nc."unidadAbr"),
                    			TRIM(nc."descripcionUnidadResponsable")
                    		), 'UTF8'
                    		)
                    	), 'hex'),
                    	nc."codigoUnidadResponsable",
                    	TRIM(nc."unidadAbr"),
                    	TRIM(nc."descripcionUnidadResponsable")
                    FROM
                    	tmp_nomina_csv_raw nc
                    GROUP BY
                    	nc."codigoUnidadResponsable",
                    	TRIM(nc."unidadAbr"),
                    	TRIM(nc."descripcionUnidadResponsable")
                    ON CONFLICT (hash_id) DO NOTHING
                    """
                    )
                    pbar.set_description(f"[{anio_mes}] loaded to unidades")
                    cur.execute(
                        """
                    INSERT INTO py_nomina_funcionarios_publicos_objecto_gasto (
                    	codigo_objecto_gasto,
                    	concepto_gasto
                    )
                    SELECT
                    	nc."codigoObjetoGasto",
                    	TRIM(nc."conceptoGasto")
                    FROM
                    	tmp_nomina_csv_raw nc
                    GROUP BY
                    	nc."codigoObjetoGasto",
                    	TRIM(nc."conceptoGasto")
                    ON CONFLICT (codigo_objecto_gasto)
                        DO UPDATE SET
                        	concepto_gasto = (
                                CASE
                        		    WHEN py_nomina_funcionarios_publicos_objecto_gasto.concepto_gasto <> EXCLUDED.concepto_gasto
                        		    	THEN CONCAT(py_nomina_funcionarios_publicos_objecto_gasto.concepto_gasto, '/', EXCLUDED.concepto_gasto)
                        		    ELSE
                        		    	py_nomina_funcionarios_publicos_objecto_gasto.concepto_gasto
                        	    END
                            )
                    """
                    )
                    pbar.set_description(f"[{anio_mes}] loaded to objecto_gastos")
                    cur.execute(
                        """
                    INSERT INTO py_nomina_funcionarios_publicos (
                        payment_id,
                    	anio,
                    	mes,
                    	codigo_persona,
                    	codigo_nivel,
                    	nivel_abr,
                    	codigo_entidad,
                    	entidad_abr,
                    	programa_hash_id,
                    	proyecto_hash_id,
                    	unidad_responsable_hash_id,
                    	codigo_objecto_gasto,
                    	fuente_financiamiento,
                    	linea,
                    	codigo_categoria,
                    	cargo,
                    	horas_catedra,
                    	fecha_ingreso,
                        fecha_ingreso_txt,
                    	antiguedad,
                    	tipo_profesional,
                    	lugar,
                    	monto_presupuesto,
                    	monto_devengado,
                    	mes_corte,
                    	anio_corte,
                    	fecha_corte,
                        fecha_corte_txt,
                    	discapacidad,
                    	check_sum
                    )
                    SELECT
						CONCAT(
							nc.anio,
							LPAD(nc.mes::TEXT, 2, '0'),
                        	'-',
                            nc."codigoPersona",
                    		'-',
                    		ROW_NUMBER() OVER ( PARTITION BY nc.anio, nc.mes, nc."codigoPersona" )
						),
                    	nc.anio,
                    	nc.mes,
                    	nc."codigoPersona",
                    	nc."codigoNivel",
                    	TRIM(nc."nivelAbr"),
                    	nc."codigoEntidad",
                    	TRIM(nc."entidadAbr"),
                    	encode(
                            sha256(
                        	    convert_to(
                        	        CONCAT(
                        		        nc."codigoPrograma"::TEXT,
                        		        nc."codigoSubprograma"::TEXT,
                        		        TRIM(nc."programaAbr"),
                        		        TRIM(nc."subprogramaAbr"),
                        		        TRIM(nc."descripcionPrograma"),
                        		        TRIM(nc."descripcionSubprograma")
                        	        ),
                                    'UTF8'
                        	    )
                            ),
                            'hex'
                        ),
                        encode(
                            sha256(
                        	    convert_to(
                        	        CONCAT(
                        		        nc."codigoProyecto",
                    	                TRIM(nc."proyectoAbr"),
                    	                TRIM(nc."descripcionProyecto")
                        	        ),
                                    'UTF8'
                        	    )
                            ),
                            'hex'
                        ),
                        encode(
                            sha256(
                        	    convert_to(
                        	        CONCAT(
                        	        	nc."codigoUnidadResponsable",
                        	        	TRIM(nc."unidadAbr"),
                        	        	TRIM(nc."descripcionUnidadResponsable")
                        	        ), 'UTF8'
                        	    )
                            ),
                            'hex'
                        ),
                        nc."codigoObjetoGasto",
                        TRIM(nc."fuenteFinanciamiento"),
                        TRIM(nc.linea),
                        TRIM(nc."codigoCategoria"),
                        TRIM(nc.cargo),
                        nc."horasCatedra",
                        CASE
                            WHEN (is_date(nc."fechaIngreso") AND TO_DATE(nc."fechaIngreso", 'YYYY-MM-DD') < CURRENT_DATE + 1)
                            	THEN TO_DATE(nc."fechaIngreso", 'YYYY-MM-DD')
                            ELSE NULL
                        END,
                        CASE
                            WHEN (is_date(nc."fechaIngreso") AND TO_DATE(nc."fechaIngreso", 'YYYY-MM-DD') < CURRENT_DATE + 1) THEN NULL
                            ELSE nc."fechaIngreso"
                        END,
                        CASE
                            WHEN (is_date(nc."fechaIngreso") AND TO_DATE(nc."fechaIngreso", 'YYYY-MM-DD') < CURRENT_DATE + 1 AND nc.mes < 13) THEN (
								DATE_PART(
                        	    	'YEAR',
                        	    	AGE(
                        	    		make_date(nc.anio, nc.mes, 1) + INTERVAL '1 MONTH' - INTERVAL '1 DAY',
                        	    		TO_DATE(nc."fechaIngreso", 'YYYY-MM-DD')
                        	    	)
                                )
							)
                            WHEN (is_date(nc."fechaIngreso") AND TO_DATE(nc."fechaIngreso", 'YYYY-MM-DD') < CURRENT_DATE + 1 AND nc.mes = 13) THEN (
								DATE_PART(
                        	    	'YEAR',
                        	    	AGE(
                        	    		make_date(nc.anio, nc.mes - 1, 1) + INTERVAL '1 MONTH' - INTERVAL '1 DAY',
                        	    		TO_DATE(nc."fechaIngreso", 'YYYY-MM-DD')
                        	    	)
                                )
							)
                            ELSE NULL
                        END,
                        TRIM(nc."tipoPersonal"),
                        TRIM(nc.lugar),
                        nc."montoPresupuestado",
                        nc."montoDevengado",
                        nc."mesCorte",
                        nc."anioCorte",
                        CASE
                            WHEN is_date(nc."fechaCorte") THEN TO_DATE(nc."fechaCorte", 'YYYY-MM-DD')
                            ELSE NULL
                        END,
                        CASE
                            WHEN is_date(nc."fechaCorte") THEN NULL
                            ELSE nc."fechaCorte"
                        END,
                        CASE COALESCE(nc.discapacidad, 'OTHERS')
                        	WHEN 'OTHERS' THEN NULL
                        	WHEN 'N' THEN 'N'
                        	WHEN 'S' THEN 'Y'
                        	ELSE LEFT(nc.discapacidad, 1)
                        END,
                        encode(
                        	sha256(
                        		convert_to(
                        			concat(
                        				nc.anio::TEXT,
                        				nc.mes::TEXT,
                        				nc."codigoPersona",
                        				nc."codigoNivel"::TEXT,
                        				nc."nivelAbr",
                        				nc."codigoEntidad"::TEXT,
                        				nc."entidadAbr",
                        				nc."codigoPrograma"::TEXT,
                        				nc."codigoSubprograma"::TEXT,
                        				TRIM(nc."programaAbr"),
                                       	TRIM(nc."subprogramaAbr"),
                                   		TRIM(nc."descripcionPrograma"),
                                      	TRIM(nc."descripcionSubprograma"),
                                      	nc."codigoProyecto",
                    	               	TRIM(nc."proyectoAbr"),
                    	               	TRIM(nc."descripcionProyecto"),
                    	               	nc."codigoUnidadResponsable",
                                       	TRIM(nc."unidadAbr"),
                                       	TRIM(nc."descripcionUnidadResponsable"),
                                       	nc."codigoObjetoGasto",
                                       	nc."fuenteFinanciamiento",
                                       	nc.linea,
                                       	nc."codigoCategoria",
                                       	nc.cargo,
                                       	nc."horasCatedra"::TEXT,
                                       	nc."fechaIngreso",
                                       	nc."tipoPersonal",
                                       	nc.lugar,
                                       	nc."montoPresupuestado"::TEXT,
                                       	nc."montoDevengado"::TEXT,
                                       	nc."mesCorte"::TEXT,
                                       	nc."anioCorte"::TEXT,
                                       	nc."fechaCorte",
                                       	nc.discapacidad
                        			),
                        			'UTF8'
                        		)
                        	),
                        	'hex'
                        )
                    FROM
                    	tmp_nomina_csv_raw nc
                    ON CONFLICT (check_sum) DO NOTHING
                    """
                    )
                    pbar.set_description(f"[{anio_mes}] loaded to nominas")
                except Exception as e:
                    print(e)


def sync_data_from_hecienda_py(dest: str):
    try:
        log.info(f"Staring to sync data from {dest} to postgres...")
        rq = get(dest=dest)
        if rq.status_code != 200:
            log.error(f"the hacienda api is not working well: [{rq.status_code}] {rq.text}")
            return
        available_data = rq.json()
        if isinstance(available_data, list):
            for item in (pbar := tqdm(available_data)):
                download_zip_to_pg(pbar, item["periodo"])
        else:
            log.error(
                f"the response of the hacienda api is not a list of zip files: [{rq.status_code}] {rq.text}"
            )
            return
    except Exception as e:
        traceback.print_exc()
        log.error(f"Error occured upon synchronizing data from hacienda api: {e}")


if __name__ == '__main__':
    try:
        log.info("Welcome to nominas-py")
        nomina_pool_manager.start(app_config.pg.to_conn_str())
        sync_data_from_hecienda_py(app_config.nominas.resource)
    finally:
        nomina_pool_manager.teardown()
