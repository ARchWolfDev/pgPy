import psycopg2.extras


def row(name, primary_key=False, serial=False, nullable=True, integer=False, varchar=255, bytea=False, *args):
    return (f"{name} "
            f"{'SERIAL ' if serial else ''}"
            f"{'INTEGER ' if integer else ''}"
            f"{f'VARCHAR({varchar}) ' if varchar else ''}"
            f"{'PRIMARY KEY ' if primary_key else ''}"
            f"{'NOT NULL ' if not nullable else ''}"
            f"{'BYTEA ' if bytea else ''}"
            f"{args if args else ''}"
            )


class Database:

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database)
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            return self

        except psycopg2.OperationalError:
            print("Database connection failed. Please check your connection details and try again.")


    def version(self):
        self.cursor.execute("SELECT version()")
        return self.cursor.fetchone()


    class _Create:

        def __init__(self, db):
            self.db = db

        def database(self, database):
            self.db.connection.autocommit = True
            self.db.cursor.execute(f"CREATE DATABASE {database}")

        def table(self, name: str, rows: list):
            db_row = ", ".join(rows)
            self.db.cursor.execute(f"CREATE TABLE {name} ({db_row})")
            self.db.connection.commit()


    def create(self):
        return Database._Create(self)


    class _Select:

        def __init__(self, db, table: str, columns: str = "*"):
            self.db = db
            self.table = table
            self.columns = columns

        def all(self):
            self.db.cursor.execute(f"select * from {self.table}")
            return self.db.cursor.fetchall()

        def description(self):
            self.db.cursor.execute(f"SELECT * FROM {self.table}")
            return self.db.cursor.description

        def where(self, **kwargs):
            where = []
            for key, value in kwargs.items():
                if isinstance(value, dict):
                    kwargs = value

            for k, v in kwargs.items():
                where.append(k)
                if "%" in v:
                    where.append("LIKE")
                elif "n(" in v:
                    where.append("NOT IN")
                    v = v.replace("n(", "(")
                elif "(" in v:
                    where.append("IN")
                else:
                    where.append("=")
                if "'" not in v:
                    v = f"'{v}'"
                where.append(f"{v}")
                where.append("AND")
            where.pop(-1)
            final_where = " ".join(where)
            self.db.cursor.execute(
                f"SELECT {self.columns} "
                f"FROM {self.table} "
                f"WHERE {final_where}")

            return self.db.cursor.fetchall()

        def count(self):
            self.db.cursor.execute(f"SELECT COUNT({self.columns}) from {self.table}")
            return self.db.cursor.fetchone()

    def select(self, table: str, columns: str ="*"):
        return Database._Select(self, table, columns=columns)


    class _Insert:

        def __init__(self, db, table: str, data: dict):
            self.db = db
            self.table = table
            self.data = data
            self._insert()

        def _insert(self):
            columns = []
            values = []

            for col, val in self.data.items():
                columns.append(col)
                values.append(f"'val'")

            self.db.cursor.execute(
                f"INSERT INTO {self.table} ({', '.join(columns)}) "
                f"VALUES ({', '.join(values)})"
            )

            self.db.connection.commit()

    def insert(self, table: str, data: dict):
        return Database._Insert(self, table, data)


    class _Update:

        def __init__(self, db, table: str, data: dict, where_val: str, where_key: str = "id", where_opr: str = "="):
            self.db = db
            self.table = table
            self.data = data
            self.where_val = where_val
            self.where_key = where_key
            self.where_opr = where_opr
            self._update()

        def _update(self):
            data_to_update = []

            for key, val in self.data.items():
                data_to_update.append(f"{key} = '{val}'")

            self.db.cursor.execute(
                f"UPDATE {self.table} "
                f"SET {', '.join(data_to_update)} "
                f"WHERE {self.where_key} {self.where_opr} {self.where_val}"
            )

            self.db.connection.commit()

    def update(self, table: str, data: dict, where_val: str, where_key: str = "id", where_opr: str = "="):
        return Database._Update(self, table, data, where_val, where_key, where_opr)


    class _Delete:

        def __init__(self, db, table: str, where_val: str, where_key: str = "id", where_opr: str = "="):
            self.db = db
            self.table = table
            self.where_val = where_val
            self.where_key = where_key
            self.where_opr = where_opr
            self._delete()

        def _delete(self):
            self.db.cursor.execute(
                f"DELETE FROM {self.table} "
                f"WHERE {self.where_key} {self.where_opr} {self.where_val}"
            )

            self.db.connection.commit()

    def delete(self, table: str, where_val: str, where_key: str = "id", where_opr: str = "="):
        return Database._Delete(self, table, where_val, where_key, where_opr)


    class _Query:

        def __init__(self, db, query: str):
            self.db = db
            self.query = query


        def fetch(self):
            self.db.cursor.execute(self.query)
            return self.db.cursor.fetchall()


    def query(self, query: str):
        return Database._Query(self, query)


