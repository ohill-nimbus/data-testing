"""Authorization for Aurora MySQL database."""
import os
import pandas as pd
import mysql.connector
from typing import Any
from src.custom_typing import SQLParams


class MySQL:
    """MySQL client for connecting to the Aurora MySQL database."""

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 3306,
    ) -> None:
        """Initialize the MySQL client.

        Args:
            host (str): RDS endpoint.
            user (str): Username for accessing the host.
            password (str): Password for accessing the host.
            database (str): The database being accessed in the host.
            port (int): The port on which to access the db.
        """
        config = {
            'user': user,
            'password': password,
            'host': host,
            'port': port,
            'database': database,
            'ssl_disabled': False  # Set to True if SSL is not enforced
        }
        self.connection = mysql.connector.connect(**config)

    def create_cursor(self, dict_mode: bool) -> None:
        """Create a MySQL cursor.

        Args:
            dict_mode (bool): If True, create a cursor that returns dictionaries.
                If False, create a cursor that returns tuples.
        """
        self.cursor = self.connection.cursor(dictionary=dict_mode)

    def close(self) -> None:
        """Close the MySQL connection."""
        self.connection.close()
        self.cursor.close()

    def execute_query(
        self,
        query: str,
        params: SQLParams = (),
    ) -> tuple[list[Any], list[str]]:
        """Execute a SQL query and return the result.

        Args:
            query (str): The SQL query to execute
            params (SQLParams): Parameters to pass to the query.

        Returns:
            list: The result of the query as a list of tuples.
        """
        self.create_cursor(dict_mode=False)
        self.cursor.execute(
            operation=query,
            params=params,
        )
        description = self.cursor.description
        assert description is not None, "Cursor description is None"
        columns = [desc[0] for desc in description]
        data = self.cursor.fetchall()
        return data, columns

    def execute_query_dict(
        self,
        query: str,
        params: SQLParams = (),
    ) -> Any:
        """Execute a SQL query and return the result as a dictionary.

        Args:
            query (str): The SQL query to execute
            params (SQLParams): Parameters to pass to the query.

        Returns:
            Any: The result of the query as a dictionary.
        """
        self.create_cursor(dict_mode=True)
        self.cursor.execute(
            operation=query,
            params=params,
        )
        data = self.cursor.fetchall()
        return data

    def query_to_pandas(
        self,
        query: str,
        params: SQLParams = (),
    ) -> pd.DataFrame:
        """Execute a SQL query and return the result as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute
            params (SQLParams): Parameters to pass to the query.

        Returns:
            pd.DataFrame: The result of the query as a pandas DataFrame.
        """
        data, columns = self.execute_query(
            query=query,
            params=params,
        )
        df = pd.DataFrame(data, columns=columns)
        return df

    def query_single_value(
        self,
        query: str,
        params: SQLParams = (),
    ) -> Any:
        """Execute a SQL query and return a single value.

        Args:
            query (str): The SQL query to execute
            params (SQLParams): Parameters to pass to the query.

        Returns:
            Any: The result of the query as a single value.
        """
        data, columns = self.execute_query(
            query=query,
            params=params,
        )
        assert len(columns) == 1, "Query must return exactly one column"
        assert len(data) == 1, "Query must return exactly one row"
        return data[0][0]

    def query_to_list(
        self,
        query: str,
        params: SQLParams = (),
    ) -> list[Any]:
        """Execute a SQL query and return the result as a list of tuples.

        Args:
            query (str): The SQL query to execute
            params (SQLParams): Parameters to pass to the query.

        Returns:
            list: The result of the query as a list of tuples.
        """
        data, columns = self.execute_query(
            query=query,
            params=params,
        )
        assert len(columns) == 1, "Query must return exactly one column"
        data = [row[0] for row in data]
        return data

    def insert_rows(
        self,
        table: str,
        data: list[dict[str, Any]],
    ) -> None:
        """Insert a row into the specified table.

        Args:
            table (str): Table name.
            data (dict): Dictionary of column names and values.
        """
        columns = ', '.join(f"`{col}`" for col in data[0].keys())
        placeholders = ', '.join(['%s'] * len(data[0]))
        values = [tuple(row[col] for col in data[0].keys()) for row in data]
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.create_cursor(dict_mode=False)
        self.cursor.executemany(query, values)
        self.connection.commit()


def get_portal_client() -> MySQL:
    """Get a MySQL client for the portal db.

    Returns:
        MySQL: the MySQL client.
    """
    host = os.environ['PORTAL_RDS']
    user = os.environ['PORTAL_USER']
    password = os.environ['PORTAL_PASSWORD']
    database = os.environ['PORTAL_DB']
    return MySQL(host, user, password, database)


def get_sales_client() -> MySQL:
    """Get a MySQL client for the sales db.

    Returns:
        MySQL: the MySQL client.
    """
    host = os.environ['SALES_RDS']
    user = os.environ['SALES_USER']
    password = os.environ['SALES_PASSWORD']
    database = os.environ['SALES_DB']
    return MySQL(host, user, password, database)
