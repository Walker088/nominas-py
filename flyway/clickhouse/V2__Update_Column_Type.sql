ALTER TABLE py_hacienda_pub_officers
MODIFY COLUMN monto_presupuestado Nullable(Int128),
MODIFY COLUMN monto_devengado Nullable(Int128);