CREATE OR REPLACE
FUNCTION is_date(s varchar) RETURNS boolean AS $$
BEGIN
  PERFORM s::date;
  	RETURN TRUE;
  EXCEPTION
  	WHEN OTHERS THEN
      RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS py_nomina_download_history (
    resource_url TEXT,
    check_sum TEXT,
    file_size_bytes INT8,
    entries INT4,
    download_at_utc TIMESTAMP,
    PRIMARY KEY (resource_url, check_sum)
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos (
    payment_id TEXT PRIMARY KEY, -- {anio}{mes}-{codigo_persona}-{serial_number}
    anio INT2,
    mes INT2,
    codigo_persona TEXT,
    codigo_nivel INT2,
    nivel_abr TEXT,
    codigo_entidad INT2,
    entidad_abr TEXT,
    programa_hash_id TEXT,
    proyecto_hash_id TEXT,
    unidad_responsable_hash_id TEXT,
    codigo_objecto_gasto INT4,
    fuente_financiamiento TEXT,
    linea TEXT,
    codigo_categoria TEXT,
    cargo TEXT,
    horas_catedra INT4,
    fecha_ingreso DATE,
    fecha_ingreso_txt TEXT,
    antiguedad INT2,
    tipo_profesional TEXT,
    lugar TEXT,
    monto_presupuesto NUMERIC(16, 4),
    monto_devengado NUMERIC(16, 4),
    mes_corte INT2,
    anio_corte INT2,
    fecha_corte DATE,
    fecha_corte_txt TEXT,
    discapacidad CHAR, -- Y (YES), N (NO)
    check_sum TEXT UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_persona_funcionarios_publicos
    ON py_nomina_funcionarios_publicos USING btree (
        codigo_persona
    );

CREATE TABLE IF NOT EXISTS py_personas (
    codigo_persona TEXT PRIMARY KEY,
    nombres TEXT,
    apellidos TEXT,
    fecha_nacimiento DATE,
    sexo CHAR, -- F, M, O (Others)
    discapacidad CHAR -- Y (YES), N (NO)
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_niveles (
    codigo_nivel INT2,
    nivel_abr TEXT,
    descripcion_nivel TEXT,
    PRIMARY KEY (codigo_nivel, nivel_abr)
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_entidades (
    codigo_entidad INT2,
    entidad_abr TEXT,
    descripcion_entidad TEXT,
    PRIMARY KEY (codigo_entidad, entidad_abr)
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_programas (
    hash_id TEXT PRIMARY KEY,
    codgio_programa INT4,
    codigo_sub_programa INT4,
    programa_abr TEXT,
    sub_programa_abr TEXT,
    descripcion_programa TEXT,
    descripcion_sub_programa TEXT
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_proyectos (
    hash_id TEXT PRIMARY KEY,
    codigo_proyecto INT4,
    proyecto_abr TEXT,
    descripcion_proyecto TEXT
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_unidades_responsables (
    hash_id TEXT PRIMARY KEY,
    codigo_unidad_responsable INT2,
    unidad_abr TEXT,
    descripcion_unidad_responsable TEXT
);

CREATE TABLE IF NOT EXISTS py_nomina_funcionarios_publicos_objecto_gasto (
    codigo_objecto_gasto INT4 PRIMARY KEY,
    concepto_gasto TEXT
);
