import psycopg2

class PostgresHelper:
    def __init__(
            self,
            uri: str = None,
            host: str = None,
            port: str = None,
            user: str = None,
            database: str = None,
            password: str = None,
            table: str = None
    ):
        if uri:
            self.connection_mode = 'uri'
        else:
            self.connection_mode = 'log_pass'
        self.uri = uri
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.password = password
        self.table = table
        self.connection = None
        self.cursor = None

    def _connect(self):
        if not self.connection:
            if self.connection_mode == 'uri':
                self.connection = psycopg2.connect(self.uri, sslmode='require')
            else:
                self.connection = psycopg2.connect(
                    user=self.user,
                    dbname=self.database,
                    password=self.password,
                    host=self.host,
                    port=self.port
                )

    def _disconnect(self):
        self.connection.close()
        self.connection = None

    def _set_cursor(self):
        if self.connection:
            self.cursor = self.connection.cursor()
        else:
            self._connect()
            self.cursor = self.connection.cursor()

    def _unset_cursor(self):
        pass

    def setup_table(self, columns: dict):
        temp = []
        for key, value in columns.items():
            condition = f"{key}  {value}"
            temp.append(condition)
        columns = ',\n'.join(temp)
        sql = f"""
        CREATE TABLE {self.table} IF NOT EXISTS
        (
        {columns}
        )
        """
        self.cursor.execute(sql)

    @staticmethod
    def __parse_select_query(values):
        if type(values) == str:
            return values
        if type(values) == list:
            return f"({', '.join(values)}"
        else:
            return '*'

    @staticmethod
    def __parse_values(values):
        return f"({', '.join(values)})"

    @staticmethod
    def __parse_fields(fields):
        return f"({', '.join(fields)})"

    @staticmethod
    def __as_dict(input_dict: dict):
        return {input_dict.get('fields')[i]: input_dict.get('data')[i] for i in range(len(input_dict.get('fields')))}

    def __from_dict(self, **kwargs: dict):
        return f"""
        INSERT INTO {kwargs['headers'].get('table')} {self.__parse_fields(kwargs.get('data').keys())}
        VALUES {self.__parse_values(kwargs.get('data').values())}
        """

    @staticmethod
    def __parse_conditions(conditions):
        if type(conditions) != dict:
            return ''
        temp = []
        for key, value in conditions.items():
            condition = f"{key} = {value}"
            temp.append(condition)
        return f"WHERE {' AND '.join(temp)}"

    @staticmethod
    def __parse_target(target):
        return f"{target.get('field')} = {target.get('value')}"

    def __generate_select_sql(self, **kwargs):
        sql =  f"""
        SELECT {self.__parse_select_query(kwargs['data'].get('values'))} 
        FROM {kwargs['headers'].get('table')} 
        {self.__parse_conditions(kwargs['headers'].get('conditions'))}
        """
        return sql
    def __generate_insert_sql(self, **kwargs):
        if kwargs['headers'].get('from_dict'):
            return self.__from_dict(**kwargs)
        return f"""
        INSERT INTO {kwargs['headers'].get('table')} {self.__parse_fields(kwargs['data'].get('fields'))} 
        VALUES {self.__parse_values(kwargs['data'].get('values'))}
        """

    def __generate_update_sql(self, **kwargs):
        conditions = self.__parse_conditions(kwargs.get('conditions'))
        target = None
        if kwargs.get('target'):
            target = self.__parse_target(kwargs.get('target'))
        return f"UPDATE {kwargs.get('table')} SET {target} {conditions}"

    def __generate_delete_sql(self, **kwargs):
        if self._select_one(**kwargs):
            conditions = self.__parse_conditions(kwargs.get('conditions'))
            return f"DELETE FROM {kwargs.get('table')} {conditions}"
        return None

    def __select_fields(self, table_name):
        self.cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        column_names = [desc[0] for desc in self.cursor.description]
        return column_names

    def _select_one(self, **kwargs):
        with self.connection:
            self.cursor.execute(self.__generate_select_sql(**kwargs))
            data = self.cursor.fetchone()
        if kwargs['headers'].get('with_field_names') or kwargs['headers'].get('as_dict'):
            column_names = self.__select_fields(table_name=kwargs.get('table'))
            return {'fields': column_names, 'data': data}
        if data:
            return data[0]
        else:
            return 0

    def _select_all(self, **kwargs):
        with self.connection:
            self.cursor.execute(self.__generate_select_sql(**kwargs['data']))
            data = self.cursor.fetchall()
        if kwargs['headers'].get('with_field_names') or kwargs['headers'].get('as_dict'):
            column_names = self.__select_fields(table_name=kwargs['headers'].get('table'))
            return {'fields': column_names, 'data': data}
        return data

    def _insert(self, **kwargs):
        with self.connection:
            sql = self.__generate_insert_sql(**kwargs)
            self.cursor.execute(sql)
            return self.connection.commit()

    def _delete(self, **kwargs):
        sql = self.__generate_delete_sql(**kwargs)
        if sql:
            with self.connection:
                self.cursor.execute(sql)
                return self.connection.commit()

    def _update(self, **kwargs):
        sql = self.__generate_update_sql(**kwargs)
        if sql:
            with self.connection:
                self.cursor.execute(sql)
                return self.connection.commit()


    def _check_state(self):
        if not self.cursor:
            if not self.connection:
                self._connect()
            self._set_cursor()

    def execute(self, **kwargs):
        self._check_state()
        if kwargs['headers'].get('action_type') == 'select_one':
            resp = self._select_one(**kwargs)
        if kwargs['headers'].get('action_type') == 'select_all':
            resp = self._select_all(**kwargs)
        if kwargs['headers'].get('action_type') == 'insert':
            resp = self._insert(**kwargs)
        if kwargs['headers'].get('action_type') == 'update':
            resp = self._update(**kwargs)
        if kwargs['headers'].get('action_type') == 'delete':
            resp = self._delete(**kwargs)
        if kwargs['headers'].get('as_dict'):
            return self.__as_dict(resp)
        return resp