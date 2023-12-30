CREATE TABLE IF NOT EXISTS py_nomina_download_history (
    resource_url TEXT,
    check_sum TEXT,
    file_size_bytes INT8,
    download_at_utc TIMESTAMP,
    PRIMARY KEY (resource_url, check_sum)
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos (
    anio INT2,
    mes INT2,
    codigo_persona TEXT,
    orden INT2,
    codigo_nivel INT2,
    codigo_entidad INT2,
    codgio_programa INT4,
    codigo_sub_programa INT4,
    codigo_proyecto INT4,
    codigo_unidad_responsable INT2,
    codigo_objecto_gasto INT4,
    fuente_financiamiento TEXT,
    linea TEXT,
    codigo_categoria TEXT,
    cargo TEXT,
    horas_catedra INT4,
    fecha_ingreso DATE,
    tipo_profesional TEXT,
    lugar TEXT,
    monto_presupuesto NUMERIC(16, 4),
    monto_devengado NUMERIC(16, 4),
    mes_corte INT2,
    anio_corte INT2,
    fecha_corte DATE
);

CREATE TABLE IF NOT EXISTS py_personas (
    codigo_persona TEXT PRIMARY KEY,
    nombres TEXT,
    apellidos TEXT,
    fecha_nacimiento DATE,
    sexo INT2,
    discapacidad BOOLEAN
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_niveles (
    codigo_nivel INT2 PRIMARY KEY,
    nivel_abr TEXT,
    descripcion_nivel TEXT
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_entidades (
    codigo_entidad INT2 PRIMARY KEY,
    entidad_abr TEXT,
    descripcion_entidad TEXT
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_programas (
    codgio_programa INT4,
    codigo_sub_programa INT4,
    programa_abr TEXT,
    sub_programa_abr TEXT,
    descripcion_programa TEXT,
    descripcion_sub_programa TEXT,
    PRIMARY KEY (codgio_programa, codigo_sub_programa)
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_proyectors (
    codigo_proyecto INT4 PRIMARY KEY,
    proyecto_abr TEXT,
    descripcion_proyecto TEXT
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_unidades_responsables (
    codigo_unidad_responsable INT2 PRIMARY KEY,
    unidad_abr TEXT,
    descripcion_unidad_responsable TEXT
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_objecto_gasto (
    codigo_objecto_gasto INT4 PRIMARY KEY,
    concepto_gasto TEXT
);
