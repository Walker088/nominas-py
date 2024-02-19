CREATE TABLE IF NOT EXISTS py_nomina_download_history (
    download_id TEXT,
    resource_url TEXT,
    check_sum TEXT,
    entries INT4,
    download_at_utc TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'utc'),
    PRIMARY KEY (download_id)
);
