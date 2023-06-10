from sqlalchemy import URL, create_engine


class MSSQLConnection(object):
    """Provides an MSSQL Connection

    Required Environment Variables:
        SQL_SERVER_PASSWORD (str): This is the password for connecting the the MS SQL Server

    Parameters:
        server (str): The server name/address
        port (int): The server port
        user (str): The username
        password (str): The passowrd
        db_name (str): The name of the database
    """

    def __init__(self, server: str, port: int, user: str, password: str, db_name: str):
        self._server = server
        self._port = port
        self._user = user
        self._password = password
        self._db_name = db_name
        self.engine = None
        self.conn = None

    def __enter__(self):
        connection_string = "DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={db};UID={uid};PWD={pwd};Trusted_Connection=No;".format(
            server=self._server,
            port=self._port,
            uid=self._user,
            pwd=self._password,
            db=self._db_name,
        )

        # Build connection URL
        connection_url = URL.create(
            "mssql+pyodbc", query={"odbc_connect": connection_string}
        )

        # SQL Server Engine
        self.engine = create_engine(connection_url, fast_executemany=True)

        self.conn = self.engine.connect()

        return self.conn

    def __exit__(self, type, value, traceback):
        self.conn.close()
        self.engine.dispose()
