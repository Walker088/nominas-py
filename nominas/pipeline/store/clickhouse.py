import sys
import traceback

from clickhouse_connect.driver.client import Client

from nominas.pipeline.preprocess.types import ProcessedCsvItems


def store_to_clickhouse(client: Client, st: ProcessedCsvItems) -> bool:
    was_succeed = False
    try:
        client.insert(
            table="py_hacienda_pub_officers",
            data=[
                [
                    p.codigo_evento,
                    p.orden,
                    p.anio,
                    p.mes,
                    p.codigo_persona,
                    p.discapacidad,
                    p.nivel_key,
                    p.entidad_key,
                    p.programa_key,
                    p.proyecto_key,
                    p.unidad_responsable_key,
                    p.codigo_objeto_gasto,
                    p.fuente_financiamiento,
                    p.linea,
                    p.codigo_categoria,
                    p.cargo,
                    p.horas_catedra,
                    p.fecha_ingreso,
                    p.tipo_personal,
                    p.lugar,
                    p.monto_presupuestado,
                    p.monto_devengado,
                    p.anio_corte,
                    p.mes_corte,
                    p.fecha_corte,
                ]
                for p in st.pub_officers
            ],
            column_names=[
                "codigo_evento",
                "orden",
                "anio",
                "mes",
                "codigo_persona",
                "discapacidad",
                "nivel_key",
                "entidad_key",
                "programa_key",
                "proyecto_key",
                "unidad_responsable_key",
                "codigo_objeto_gasto",
                "fuente_financiamiento",
                "linea",
                "codigo_categoria",
                "cargo",
                "horas_catedra",
                "fecha_ingreso",
                "tipo_personal",
                "lugar",
                "monto_presupuestado",
                "monto_devengado",
                "anio_corte",
                "mes_corte",
                "fecha_corte",
            ],
        )

        client.insert(
            table="py_personas",
            data=[
                [p.codigo_persona, p.nombres, p.apellidos, p.fecha_nacimiento, p.sexo]
                for p in st.personas
            ],
            column_names=["codigo_persona", "nombres", "apellidos", "fecha_nacimiento", "sexo"],
        )

        client.insert(
            table="py_hacienda_pub_officers_niveles",
            data=[[n.nivel_key, n.codigo_nivel, n.nivel_abr, n.desc_nivel] for n in st.niveles],
            column_names=[
                "nivel_key",
                "codigo_nivel",
                "nivel_abr",
                "desc_nivel",
            ],
        )

        client.insert(
            table="py_hacienda_pub_officers_entidades",
            data=[
                [e.entidad_key, e.codigo_entidad, e.entidad_abr, e.desc_entidad]
                for e in st.entidades
            ],
            column_names=[
                "entidad_key",
                "codigo_entidad",
                "entidad_abr",
                "desc_entidad",
            ],
        )

        client.insert(
            table="py_hacienda_pub_officers_programas",
            data=[
                [
                    p.programa_key,
                    p.codigo_programa,
                    p.codigo_sub_programa,
                    p.programa_abr,
                    p.sub_programa_abr,
                    p.desc_programa,
                    p.desc_sub_programa,
                ]
                for p in st.programas
            ],
            column_names=[
                "programa_key",
                "codgio_programa",
                "codigo_sub_programa",
                "programa_abr",
                "sub_programa_abr",
                "desc_programa",
                "desc_sub_programa",
            ],
        )

        client.insert(
            table="py_hacienda_pub_officers_proyectos",
            data=[
                [p.proyecto_key, p.codigo_proyecto, p.proyecto_abr, p.desc_proyecto]
                for p in st.proyectos
            ],
            column_names=[
                "proyecto_key",
                "codigo_proyecto",
                "proyecto_abr",
                "desc_proyecto",
            ],
        )

        client.insert(
            table="py_hacienda_pub_officers_responsables",
            data=[
                [
                    u.unidad_responsable_key,
                    u.codigo_unidad_responsable,
                    u.unidad_responsable_abr,
                    u.desc_unidad_responsable,
                ]
                for u in st.unidades
            ],
            column_names=[
                "unidad_responsable_key",
                "codigo_unidad_responsable",
                "unidad_responsable_abr",
                "desc_unidad_responsable",
            ],
        )

        client.insert(
            table="py_hacienda_pub_officers_objecto_gasto",
            data=[
                [
                    o.codigo_objecto_gasto,
                    o.concepto_gasto,
                ]
                for o in st.objecto_gastos
            ],
            column_names=[
                "codigo_objecto_gasto",
                "concepto_gasto",
            ],
        )

        was_succeed = True
    except Exception as e:
        p = next(iter(st.pub_officers))
        print(e)
        print(f"Error on {p.anio}-{p.mes}")
    return was_succeed