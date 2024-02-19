from datetime import datetime as dt
from typing import Set

from clickhouse_connect.driver.client import Client

from nominas.pipeline.preprocess.types import (
    Entidad,
    Nivel,
    ObjectoGasto,
    Persona,
    Programa,
    Proyecto,
    PubOfficer,
    RawCsvItem,
    UnidadResponsable,
)


def to_persona(raw: RawCsvItem) -> Persona:
    return Persona(
        codigo_persona=raw.codigoPersona.strip(),
        nombres=raw.nombres.strip(),
        apellidos=raw.apellidos.strip(),
        fecha_nacimiento=None,
        sexo=raw.sexo if raw.sexo in ["F", "M"] else "O",
    )


def has_persona(codigo_persona: str, client: Client) -> bool:
    result = client.query(
        query="SELECT EXISTS (SELECT 1 FROM py_personas WHERE codigo_persona = %(codigo_persona)s)",
        parameters={"codigo_persona": codigo_persona},
    ).result_set
    return result[0][0] == 1


def get_saved_personas(client: Client) -> Set[str]:
    raws = client.query("SELECT codigo_persona FROM py_personas").result_set
    return set(raw[0] for raw in raws)


def to_nivel(nivel_key: str, raw: RawCsvItem) -> Nivel:
    return Nivel(
        nivel_key=nivel_key,
        codigo_nivel=int(raw.codigoNivel.strip()),
        nivel_abr=raw.nivelAbr.strip(),
        desc_nivel=raw.descripcionNivel.strip(),
    )


def has_nivel(nivel_key: str, client: Client) -> bool:
    result = client.query(
        query="SELECT EXISTS (SELECT 1 FROM py_hacienda_pub_officers_niveles WHERE nivel_key = %(nivel_key)s)",
        parameters={"nivel_key": nivel_key},
    ).result_set
    return result[0][0] == 1


def get_saved_niveles(client: Client) -> Set[str]:
    raws = client.query("SELECT nivel_key FROM py_hacienda_pub_officers_niveles").result_set
    return set(raw[0] for raw in raws)


def to_entidad(entidad_key: str, raw: RawCsvItem) -> Entidad:
    return Entidad(
        entidad_key=entidad_key,
        codigo_entidad=int(raw.codigoEntidad.strip()),
        entidad_abr=raw.entidadAbr.strip(),
        desc_entidad=raw.descripcionEntidad.strip(),
    )


def has_entidad(entidad_key: str, client: Client) -> bool:
    result = client.query(
        query="SELECT EXISTS (SELECT 1 FROM py_hacienda_pub_officers_entidades WHERE entidad_key = %(entidad_key)s)",
        parameters={"entidad_key": entidad_key},
    ).result_set
    return result[0][0] == 1


def get_saved_entidades(client: Client) -> Set[str]:
    raws = client.query("SELECT entidad_key FROM py_hacienda_pub_officers_entidades").result_set
    return set(raw[0] for raw in raws)


def to_programa(program_key: str, raw: RawCsvItem) -> Programa:
    return Programa(
        programa_key=program_key,
        codigo_programa=int(raw.codigoPrograma.strip()),
        codigo_sub_programa=int(raw.codigoSubprograma.strip()),
        programa_abr=raw.programaAbr.strip(),
        sub_programa_abr=raw.subprogramaAbr.strip(),
        desc_programa=raw.descripcionPrograma.strip(),
        desc_sub_programa=raw.descripcionSubprograma.strip(),
    )


def has_programa(programa_key: str, client: Client) -> bool:
    result = client.query(
        query="SELECT EXISTS (SELECT 1 FROM py_hacienda_pub_officers_programas WHERE programa_key = %(programa_key)s)",
        parameters={"programa_key": programa_key},
    ).result_set
    return result[0][0] == 1


def get_saved_programas(client: Client) -> Set[str]:
    raws = client.query("SELECT programa_key FROM py_hacienda_pub_officers_programas").result_set
    return set(raw[0] for raw in raws)


def to_proyecto(proyecto_key: str, raw: RawCsvItem) -> Proyecto:
    return Proyecto(
        proyecto_key=proyecto_key,
        codigo_proyecto=int(raw.codigoProyecto.strip()),
        proyecto_abr=raw.proyectoAbr.strip(),
        desc_proyecto=raw.descripcionProyecto.strip(),
    )


def has_proyecto(proyecto_key: str, client: Client) -> bool:
    result = client.query(
        query="SELECT EXISTS (SELECT 1 FROM py_hacienda_pub_officers_proyectos WHERE proyecto_key = %(proyecto_key)s)",
        parameters={"proyecto_key": proyecto_key},
    ).result_set
    return result[0][0] == 1


def get_saved_proyectos(client: Client) -> Set[str]:
    raws = client.query("SELECT proyecto_key FROM py_hacienda_pub_officers_proyectos").result_set
    return set(raw[0] for raw in raws)


def to_unidad_responsable(unidad_responsable_key: str, raw: RawCsvItem) -> UnidadResponsable:
    return UnidadResponsable(
        unidad_responsable_key=unidad_responsable_key,
        codigo_unidad_responsable=int(raw.codigoUnidadResponsable.strip()),
        unidad_responsable_abr=raw.unidadAbr.strip(),
        desc_unidad_responsable=raw.descripcionUnidadResponsable.strip(),
    )


def has_unidad_responsable(unidad_responsable_key: str, client: Client) -> bool:
    result = client.query(
        query="SELECT EXISTS (SELECT 1 FROM py_hacienda_pub_officers_responsables WHERE unidad_responsable_key = %(unidad_responsable_key)s)",
        parameters={"unidad_responsable_key": unidad_responsable_key},
    ).result_set
    return result[0][0] == 1


def get_saved_unidad_responsables(client: Client) -> Set[str]:
    raws = client.query(
        "SELECT unidad_responsable_key FROM py_hacienda_pub_officers_responsables"
    ).result_set
    return set(raw[0] for raw in raws)


def to_objecto_gasto(codigo_objecto_gasto: int, raw: RawCsvItem) -> ObjectoGasto:
    return ObjectoGasto(
        codigo_objecto_gasto=codigo_objecto_gasto,
        concepto_gasto=raw.conceptoGasto.strip(),
    )


def has_objecto_gasto(codigo_objecto_gasto: int, client: Client) -> bool:
    result = client.query(
        query="SELECT EXISTS (SELECT 1 FROM py_hacienda_pub_officers_objecto_gasto WHERE codigo_objecto_gasto = %(codigo_objecto_gasto)s)",
        parameters={"codigo_objecto_gasto": codigo_objecto_gasto},
    ).result_set
    return result[0][0] == 1


def get_saved_objecto_gastos(client: Client) -> Set[int]:
    raws = client.query(
        "SELECT codigo_objecto_gasto FROM py_hacienda_pub_officers_objecto_gasto"
    ).result_set
    return set(raw[0] for raw in raws)


def to_pub_officer(raw: RawCsvItem, codigo_evento: str, orden: int) -> PubOfficer:
    fecha_ingreso = None
    fecha_corte = None
    try:
        fecha_ingreso = dt.strptime(raw.fechaIngreso, "%Y-%m-%d")
        fecha_corte = dt.strptime(raw.fechaCorte, "%Y-%m-%d")
    except ValueError as ve:
        pass
    return PubOfficer(
        codigo_evento=codigo_evento,
        orden=orden,
        anio=int(raw.anio),
        mes=int(raw.mes),
        codigo_persona=raw.codigoPersona.strip(),
        discapacidad=True if raw.discapacidad == "Y" else False,
        nivel_key=f"{raw.codigoNivel.strip()}-{raw.nivelAbr.strip()}",
        entidad_key=f"{raw.codigoEntidad.strip()}-{raw.entidadAbr.strip()}",
        programa_key=f"{raw.codigoPrograma.strip()}-{raw.codigoSubprograma.strip()}-{raw.programaAbr.strip()}-{raw.subprogramaAbr.strip()}",
        proyecto_key=f"{raw.codigoProyecto.strip()}-{raw.proyectoAbr.strip()}",
        unidad_responsable_key=f"{raw.codigoUnidadResponsable.strip()}-{raw.unidadAbr.strip()}",
        codigo_objeto_gasto=int(raw.codigoObjetoGasto.strip()),
        fuente_financiamiento=raw.fuenteFinanciamiento.strip(),
        linea=raw.linea.strip(),
        codigo_categoria=raw.codigoCategoria.strip(),
        cargo=raw.cargo.strip(),
        horas_catedra=int(raw.horasCatedra.strip() or 0),
        fecha_ingreso=fecha_ingreso,
        tipo_personal=raw.tipoPersonal.strip(),
        lugar=raw.lugar.strip(),
        monto_presupuestado=int(raw.montoPresupuestado),
        monto_devengado=int(raw.montoDevengado),
        anio_corte=int(raw.anioCorte),
        mes_corte=int(raw.mesCorte),
        fecha_corte=fecha_corte,
    )


def has_pub_officer(codigo_evento: str, orden: int, client: Client) -> bool:
    result = client.query(
        query="SELECT EXISTS (SELECT 1 FROM py_hacienda_pub_officers WHERE codigo_evento = %(codigo_evento)s AND orden = %(orden)s)",
        parameters={"codigo_evento": codigo_evento, "orden": orden},
    ).result_set
    return result[0][0] == 1


def get_saved_pub_officers(client: Client) -> Set[str]:
    raws = client.query(
        "SELECT CONCAT(codigo_evento, orden) FROM py_hacienda_pub_officers"
    ).result_set
    return set(raw[0] for raw in raws)
