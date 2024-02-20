CREATE TYPE DOWNLOAD_STAT AS ENUM ('SUCCEED', 'FAILED');

ALTER TABLE IF EXISTS py_nomina_download_history
ADD COLUMN stat DOWNLOAD_STAT DEFAULT NULL;