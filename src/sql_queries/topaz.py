"""SQL queries for the Topaz MySQL database."""

TEST_QUERY = """SELECT *
FROM `api_cache`
WHERE `client_id` = 1983
AND `client_id` IS NOT NULL LIMIT 1;"""
