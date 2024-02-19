from nominas.pipeline.postprocess.types import DownloadHistory
from nominas.storage.postgres import pgpool_mgr


def insert_download_history(h: DownloadHistory):
    query = """
    INSERT INTO py_nomina_download_history (
    	download_id,
    	resource_url,
    	check_sum,
    	entries
    )
    SELECT
    	CONCAT(
    		'D',
    		LPAD(
    			(COUNT(*)+1)::TEXT,
    			7,
    			'0'
    		)
    	),
    	%(resource_url)s,
    	%(check_sum)s,
    	%(entries)s
    FROM
    	py_nomina_download_history
    """
    with pgpool_mgr.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                {"resource_url": h.resource_url, "check_sum": h.check_sum, "entries": h.entries},
            )
