CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers (
    codigo_evento String, -- {anio}{mes}-{codigo_persona}
    orden UInt32,
    anio UInt32,
    mes UInt32,
    codigo_persona String,
    discapacidad Nullable(Boolean),
    nivel_key Nullable(String),
    entidad_key Nullable(String),
    programa_key Nullable(String),
    proyecto_key Nullable(String),
    unidad_responsable_key Nullable(String),
    codigo_objeto_gasto Nullable(UInt32),
    fuente_financiamiento Nullable(String),
    linea Nullable(String),
    codigo_categoria Nullable(String),
    cargo Nullable(String),
    horas_catedra Nullable(UInt32),
    fecha_ingreso Nullable(Date32),
    tipo_personal Nullable(String),
    lugar Nullable(String),
    monto_presupuestado Nullable(UInt128),
    monto_devengado Nullable(UInt128),
    anio_corte Nullable(UInt32),
    mes_corte Nullable(UInt32),
    fecha_corte Nullable(Date32)
)
ENGINE = MergeTree()
ORDER BY (codigo_evento, orden);

CREATE TABLE IF NOT EXISTS py_personas (
    codigo_persona String,
    nombres Nullable(String),
    apellidos Nullable(String),
    fecha_nacimiento Nullable(DATE),
    sexo Nullable(Enum8('F', 'M', 'O')), -- F, M, O (Others)
)
ENGINE = MergeTree()
ORDER BY (codigo_persona);

CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers_niveles (
    nivel_key String, -- {codigo_nivel}-{nivel_abr}
    codigo_nivel UInt32,
    nivel_abr String,
    desc_nivel Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (nivel_key);

CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers_entidades (
    entidad_key String, -- {codigo_entidad}-{entidad_abr}
    codigo_entidad UInt32,
    entidad_abr String,
    desc_entidad Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (entidad_key);

CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers_programas (
    programa_key String, -- {codgio_programa}-{codigo_sub_programa}-{programa_abr}-{sub_programa_abr}
    codgio_programa Nullable(UInt32),
    codigo_sub_programa Nullable(UInt32),
    programa_abr Nullable(String),
    sub_programa_abr Nullable(String),
    desc_programa Nullable(String),
    desc_sub_programa Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (programa_key);

CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers_proyectos (
    proyecto_key String, -- {codigo_proyecto}-{proyecto_abr}
    codigo_proyecto Nullable(UInt32),
    proyecto_abr Nullable(String),
    desc_proyecto Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (proyecto_key);

CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers_responsables (
    unidad_responsable_key String, -- {codigo_unidad_responsable}-{unidad_responsable_abr}
    codigo_unidad_responsable Nullable(UInt32),
    unidad_responsable_abr Nullable(String),
    desc_unidad_responsable Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (unidad_responsable_key);

CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers_objecto_gasto (
    codigo_objecto_gasto UInt32,
    concepto_gasto Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (codigo_objecto_gasto);
