CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers (
    codigo_evento String, -- {anio}{mes}-{codigo_persona}
    orden UInt8,
    anio UInt8,
    mes UInt8,
    codigo_persona String,
    discapacidad Nullable(Boolean),
    nivil_key Nullable(String),
    entidad_key Nullable(String),
    programa_key Nullable(String),
    proyecto_key Nullable(String),
    unidad_responsable_key Nullable(String),
    codigo_objeto_gasto String,
    fuente_financiamiento String,
    linea String,
    codigo_categoria String,
    cargo String,
    horas_catedra UInt32,
    fecha_ingreso Date,
    tipo_profesional String,
    lugar String,
    monto_presupuestado UInt64,
    monto_devengado UInt64,
    anio_corte UInt8,
    mes_corte UInt8,
    fecha_corte Date
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
    nivil_key String, -- {codigo_nivel}_{nivel_abr}
    codigo_nivel UInt8,
    nivel_abr String,
    descripcion_nivel Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (nivil_key);

CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers_entidades (
    entidad_key String, -- {codigo_entidad}_{entidad_abr}
    codigo_entidad UInt8,
    entidad_abr String,
    descripcion_entidad Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (entidad_key);

CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers_programas (
    programa_key String, -- {codgio_programa}_{codigo_sub_programa}_{programa_abr}_{sub_programa_abr}
    codgio_programa Nullable(UInt32),
    codigo_sub_programa Nullable(UInt32),
    programa_abr Nullable(String),
    sub_programa_abr Nullable(String),
    descripcion_programa Nullable(String),
    descripcion_sub_programa Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (programa_key);

CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers_proyectos (
    proyecto_key String, -- {codigo_proyecto}_{proyecto_abr}
    codigo_proyecto Nullable(UInt32),
    proyecto_abr Nullable(String),
    descripcion_proyecto Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (proyecto_key);

CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers_responsables (
    unidad_responsable_key String, -- {codigo_unidad_responsable}_{unidad_responsable_abr}
    codigo_unidad_responsable Nullable(UInt32),
    unidad_responsable_abr Nullable(String),
    descripcion_unidad_responsable Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (unidad_responsable_key);

CREATE TABLE IF NOT EXISTS py_hacienda_pub_officers_objecto_gasto (
    codigo_objecto_gasto UInt32,
    concepto_gasto Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (codigo_objecto_gasto);
