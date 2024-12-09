import psycopg2

class ExtractorPostgresql:
    def __init__(self, primarykey_writer, foreignkey_writer, host, port, username, password, database):
        self.primarykey_writer = primarykey_writer
        self.foreignkey_writer = foreignkey_writer

        self.type = "PostgreSQL"
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

    def extract(self):
        # Connect ot database
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.username,
            password=self.password
        )
        cursor = conn.cursor()

        schemas = self.get_schemas(cursor)
        for schema in schemas:
            tables = self.get_tables(cursor, schema)
            if not tables: continue
            for table in tables:
                # Primarykey extraction
                primary_keys = self.get_primary_keys(cursor, schema, table)
                self.primarykey_writer.add_primarykey(self.type, self.host, self.port, self.database, table, primary_keys)
                # Foreignkey extraction
                foreign_keys = self.get_foreign_keys(cursor, schema, table)
                for fk in foreign_keys:
                    foreign_attribute = fk["foreign_column"]
                    primary_datastorage = fk["primary_table"]
                    primary_attribute = fk["primary_column"]
                    self.foreignkey_writer.add_forgeinkey(
                        self.type, self.host, self.port, self.database, primary_datastorage, primary_attribute,
                        self.type, self.host, self.port, self.database, table, foreign_attribute
                    )

        cursor.close()
        conn.close()
        self.primarykey_writer.write()
        self.foreignkey_writer.write()

    def get_schemas(self, cursor):
        query = "SELECT schema_name FROM information_schema.schemata;"
        cursor.execute(query)
        schema_rows = cursor.fetchall()
        schema_names = [row[0] for row in schema_rows]
        schema_names.remove("information_schema")
        schema_names.remove("pg_catalog")
        schema_names.remove("pg_toast")    
        return schema_names

    def get_tables(self, cursor, schema):
        tables = []
        query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_type = 'BASE TABLE';
        """
        cursor.execute(query)
        # Fetch all table names
        for row in cursor.fetchall():
            name = row[0]
            tables.append(name)
        return tables

    def get_primary_keys(self, cursor, schema, table):
        query = f"""
            SELECT a.attname
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = '{schema}.{table}'::regclass
            AND    i.indisprimary;
        """
        cursor.execute(query)
        result = cursor.fetchall()
        keys = [str(row[0]) for row in result]
        result = ", ".join(keys)
        return result

    def get_foreign_keys(self, cursor, schema, table):
        query = f"""
            SELECT
                kcu.table_name AS foreign_table_name,
                kcu.column_name AS foreign_column_name, 
                ccu.table_name AS primary_table_name,
                ccu.column_name AS primary_column_name 
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema='{schema}'
                AND tc.table_name='{table}';
        """
        cursor.execute(query)
        result = cursor.fetchall()
        entries = []
        for entry in result:
            foreign_table = entry[0]
            foreign_column = entry[1]
            primary_table = entry[2]
            primary_column = entry[3]
            dic = {"foreign_table": foreign_table, "foreign_column":foreign_column, "primary_table":primary_table, "primary_column":primary_column}
            entries.append(dic)
        return entries

