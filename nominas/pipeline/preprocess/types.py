from dataclasses import dataclass
from datetime import datetime
from typing import Set


@dataclass(frozen=True)
class RawCsvItem:
    anio: str
    mes: str
    codigoNivel: str
    nivelAbr: str
    descripcionNivel: str
    codigoEntidad: str
    entidadAbr: str
    descripcionEntidad: str
    codigoPrograma: str
    programaAbr: str
    descripcionPrograma: str
    codigoSubprograma: str
    subprogramaAbr: str
    descripcionSubprograma: str
    codigoProyecto: str
    proyectoAbr: str
    descripcionProyecto: str
    codigoUnidadResponsable: str
    unidadAbr: str
    descripcionUnidadResponsable: str
    codigoObjetoGasto: str
    conceptoGasto: str
    fuenteFinanciamiento: str
    linea: str
    codigoPersona: str
    nombres: str
    apellidos: str
    sexo: str
    discapacidad: str
    codigoCategoria: str
    cargo: str
    horasCatedra: str
    fechaIngreso: str
    tipoPersonal: str
    lugar: str
    montoPresupuestado: str
    montoDevengado: str
    mesCorte: str
    anioCorte: str
    fechaCorte: str


@dataclass(frozen=True)
class Persona:
    codigo_persona: str
    nombres: str
    apellidos: str
    fecha_nacimiento: datetime | None
    sexo: str


@dataclass(frozen=True)
class Nivel:
    nivel_key: str
    codigo_nivel: int
    nivel_abr: str
    desc_nivel: str


@dataclass(frozen=True)
class Entidad:
    entidad_key: str
    codigo_entidad: int
    entidad_abr: str
    desc_entidad: str


@dataclass(frozen=True)
class Programa:
    programa_key: str
    codigo_programa: int
    codigo_sub_programa: int
    programa_abr: str
    sub_programa_abr: str
    desc_programa: str
    desc_sub_programa: str


@dataclass(frozen=True)
class Proyecto:
    proyecto_key: str
    codigo_proyecto: int
    proyecto_abr: str
    desc_proyecto: str


@dataclass(frozen=True)
class UnidadResponsable:
    unidad_responsable_key: str
    codigo_unidad_responsable: int
    unidad_responsable_abr: str
    desc_unidad_responsable: str


@dataclass(frozen=True)
class ObjectoGasto:
    codigo_objecto_gasto: int
    concepto_gasto: str


@dataclass(frozen=True)
class PubOfficer:
    codigo_evento: str
    orden: int
    anio: int
    mes: int
    codigo_persona: str
    discapacidad: bool | None
    nivel_key: str | None
    entidad_key: str | None
    programa_key: str | None
    proyecto_key: str | None
    unidad_responsable_key: str | None
    codigo_objeto_gasto: int | None
    fuente_financiamiento: str | None
    linea: str | None
    codigo_categoria: str | None
    cargo: str | None
    horas_catedra: int | None
    fecha_ingreso: datetime | None
    tipo_personal: str | None
    lugar: str | None
    monto_presupuestado: int | None
    monto_devengado: int | None
    anio_corte: int | None
    mes_corte: int | None
    fecha_corte: datetime | None


@dataclass(frozen=True)
class ProcessedCsvItems:
    personas: Set[Persona]
    niveles: Set[Nivel]
    entidades: Set[Entidad]
    programas: Set[Programa]
    proyectos: Set[Proyecto]
    unidades: Set[UnidadResponsable]
    objecto_gastos: Set[ObjectoGasto]
    pub_officers: Set[PubOfficer]
