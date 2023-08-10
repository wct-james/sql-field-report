from sqlalchemy import URL, create_engine


class DBConnection(object):
    """Provides a DB Connection

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


class MSSQLConnection(DBConnection):
    """Provides an MSSQL Connection

    Parameters:
        server (str): The server name/address
        port (int): The server port
        user (str): The username
        password (str): The passowrd
        db_name (str): The name of the database
    """

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


class MSSQLConnectionX(DBConnection):
    """Provides an MSSQL Connection

    Parameters:
        server (str): The server name/address
        port (int): The server port
        user (str): The username
        password (str): The passowrd
        db_name (str): The name of the database
    """

    def __enter__(self):
        self.connection_string = f"mssql://{self._user}:{self._password}@{self._server}:{self._port}/{self._db_name}"

        return self.connection_string

    def __exit__(self, type, value, traceback):
        self.connection_string = ""


class MySQLConnection(DBConnection):
    """Provides an MySQL Connection

    Parameters:
        server (str): The server name/address
        port (int): The server port
        user (str): The username
        password (str): The passowrd
        db_name (str): The name of the database
    """

    def __enter__(self):
        connection_string = (
            "mysql+mysqlconnector://{uid}:{pwd}@{server}:{port}/{db}".format(
                server=self._server,
                port=str(self._port),
                uid=self._user,
                pwd=self._password,
                db=self._db_name,
            )
        )

        # SQL Server Engine
        self.engine = create_engine(connection_string)

        self.conn = self.engine.connect()

        return self.conn

    def __exit__(self, type, value, traceback):
        self.conn.close()
        self.engine.dispose()
