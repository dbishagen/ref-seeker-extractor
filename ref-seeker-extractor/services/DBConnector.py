import mariadb
import psycopg2

class DBConnector:

    def __init__(self, DBType, host, port, user, password, database):
        """
        Initializes the DBConnector object with connection parameters.

        Args:
            DBType (str): Type of database to connect. Possible: MariaDB
            host (str): The hostname of the server.
            port (int): The portnumber of the server.
            user (str): The username for connecting to the database.
            password (str): The password for the specified username.
            database (str): The name of the database to connect to.
        """
        self.DBType = DBType
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.list_values_batchimport = []

    # Basic functions

    def connect(self):
        """
        Establishes a connection to the MariaDB database.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        if self.DBType == "MariaDB":
            try:
                self.connection = mariadb.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                self.connection.auto_reconnect= True
                self._create_table_servers()
                self._create_table_loadeddatabases()
                self._create_table_datastorage()
                self._create_table_attributes()
                self._create_table_values()
                self._create_table_uniqueattributecombinations()
                self._create_table_inclusiondependencies()
                self._create_table_max_inclusiondependencies()
                self._create_table_implicitly_references()
                self._create_table_explicit_references()
                self._create_table_primarykeys()
                self._create_view_inclusionsdependencies()
                self._create_view_explicite_references()
                self._create_view_implicitly_references()
                self._create_view_primarykeys()
                return True
            except mariadb.Error as err:
                print(f"Error: {err}")
                return False
        elif self.DBType == "PostgreSQL":
            try:
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                self.connection.autocommit = True
                self._create_table_servers()
                self._create_table_loadeddatabases()
                self._create_table_datastorage()
                self._create_table_attributes()
                self._create_table_values()
                self._create_table_uniqueattributecombinations()
                self._create_table_inclusiondependencies()
                self._create_table_max_inclusiondependencies()
                self._create_table_implicitly_references()
                self._create_table_explicit_references()
                self._create_table_primarykeys()
                self._create_view_inclusionsdependencies()
                self._create_view_explicite_references()
                self._create_view_implicitly_references()
                self._create_view_primarykeys()
                return True
            except psycopg2.Error as err:
                print(f"Error: {err}")
                return False  

    def query(self, query, parameters=None):
        """
        Returns results of query.

        Args:
            query (str): The query to execute.
             parameters (list): List with the optinal parameters.

        Returns:
            list: Query result.
        """
        if not self.connection:
            print("Not connected to DB.")
            return None
        
        if self.DBType == "MariaDB":
            try:
                cursor = self.connection.cursor()
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
            except mariadb.Error as err:
                print(f"Error executing query: {err}")
            finally:
                if cursor:
                    cursor.close()    
        elif self.DBType == "PostgreSQL":
            try:
                cursor = self.connection.cursor()
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
            except psycopg2.Error as err:
                print(f"Error executing query: {err}")
            finally:
                if cursor:
                    cursor.close()                

    def query_wo_return(self, query):
        """
        Makes a query, without anything to retrun.

        Args:
            query (str): The query to execute.        
        """
        if not self.connection:
            print("Not connected to MariaDB.")
            return None
        
        if self.DBType == "MariaDB":
            try:
                cursor = self.connection.cursor()
                cursor.execute(query)
                self.connection.commit() 
            except mariadb.Error as err:
                print(f"Error executing query: {err}")
            finally:
                if cursor:
                    cursor.close()
        elif self.DBType == "PostgreSQL":  
            try:
                cursor = self.connection.cursor()
                cursor.execute(query)
                self.connection.commit()
            except psycopg2.Error as err:
                print(f"Error executing query: {err}")
            finally:
                if cursor:
                    cursor.close()     

    def query_update(self, query):
        """
        Returns results of query.

        Args:
            query (str): The query to execute.

        Returns:
            list: Query result.
        """
        if not self.connection:
            print("Not connected to MariaDB.")
            return None
        
        if self.DBType == "MariaDB":
            try:
                cursor = self.connection.cursor()
                cursor.execute(query)
                self.connection.commit() 
            except mariadb.Error as err:
                print(f"Error creating table: {err}")
            finally:
                if cursor:
                    cursor.close()
        elif self.DBType == "PostgreSQL":
            try:
                cursor = self.connection.cursor()
                cursor.execute(query)
                self.connection.commit() 
            except psycopg2.Error as err:
                print(f"Error creating table: {err}")
            finally:
                if cursor:
                    cursor.close()         

    def query_insert(self, query, parameters):
        """
        Insert entrie to DB, returns the ID.

        Args:
            query (str): The query to execute.
            parameters (list): List with the parameters.

        Returns:
            int: ID of the added entry.
        """
        if not self.connection:
            print("Not connected to MariaDB.")
            return None
        
        if self.DBType == "MariaDB":
            try:
                cursor = self.connection.cursor()
                cursor.execute(query, parameters)
                self.connection.commit()
                return cursor.lastrowid
            except mariadb.Error as err:
                print(f"Error insert tuple: {err}")
            finally:
                if cursor:
                    cursor.close()
        elif self.DBType == "PostgreSQL":
            try:
                cursor = self.connection.cursor()
                cursor.execute(query, parameters)
                cursor.execute('SELECT LASTVAL()')
                return cursor.fetchone()[0]
            except psycopg2.Error as err:
                print(f"Error insert tuple: {err}")
            finally:
                if cursor:
                    cursor.close()                

    def delete_everything(self):
        """
        Deletes everything from the server.
        """

        query = """
            DELETE FROM servers;
        """
        self.query_wo_return(query)

    def close(self):
        """
        Closes the connection to the database.
        """

        if self.connection:
            self.connection.close()

    # Functions to create tables

    def _table_exists(self, table_name):
        """
        Checks if a table exists in the connected database.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        if not self.connection:
            print("Not connected to MariaDB.")
            return False

        if self.DBType == "MariaDB":
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if bool(cursor.fetchone()):
                    # print(f"Table exist: {table_name}")
                    return True
                else:
                    return False                
            except mariadb.Error as err:
                print(f"Error checking table existence: {err}")
                return False
            finally:
                if cursor:
                    cursor.close()
        elif self.DBType == "PostgreSQL":
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')")
                if cursor.fetchone()[0]:
                    return True
                else:
                    return False                
            except psycopg2.Error as err:
                print(f"Error checking table existence: {err}")
                return False
            finally:
                if cursor:
                    cursor.close()            

    def _create_new_table(self, table_name, table_query):
        """
        Creates a new table for the databases.

        Args:
            table_name (str): The name of the table.
            table_query (str): The query text for the new table.
        """
        if not self.connection:
            print("Not connected to MariaDB.")
            return
        if self._table_exists(table_name):
            # print(f"Table '{table_name}' already exists.")
            return

        if self.DBType == "MariaDB":
            try:
                cursor = self.connection.cursor()
                # Define the table structure
                create_table_query = f"""
                    CREATE TABLE {table_name}({table_query});
                """
                # Execute the CREATE TABLE query
                cursor.execute(create_table_query)
            except mariadb.Error as err:
                print(f"Error creating table ({table_name}): {err}")
            finally:
                if cursor:
                    cursor.close()
        elif self.DBType == "PostgreSQL":
            try:
                cursor = self.connection.cursor()
                # Define the table structure
                create_table_query = f"""
                    CREATE TABLE {table_name}({table_query});
                """
                # Execute the CREATE TABLE query
                cursor.execute(create_table_query)
            except psycopg2.Error as err:
                print(f"Error creating table ({table_name}): {err}")
            finally:
                if cursor:
                    cursor.close()

    # Functions to add entries

    def add_server(self, server_type, host, port):
        """
        Adds a new entry to the "servers" table.

        Args:
            server_type (str): The type of the server.
            host (str): The hostname of the server.
            port (int): The port number of the server.

        Returns:
            int: The ID of the newly added entry.        
        """
        # Insert new entry into servers table
        insert_query = """
            INSERT INTO servers (host, port, server_type)
            VALUES (%s, %s, %s);
        """
        new_entry_id = self.query_insert(insert_query, (host, port, server_type))
        return new_entry_id      

    def add_database(self, db_name, server_id):
        """
        Adds a new entry to the "loaded_databases" table.

        Args:
            db_name (str): The name of the database.
            server_id (int): The entry id of server.

        Returns:
            int: The ID of the newly added entry.
        """
        # Insert new entry into loaded_databases table
        insert_query = """
            INSERT INTO loaded_databases (db_name, server_id)
            VALUES (%s, %s);
        """
        new_entry_id = self.query_insert(insert_query, (db_name, server_id))
        return new_entry_id

    def add_datastorage(self, storage_name, databse_id, parent_datastorage_id = None):
        """
        Adds a new entry to the "datastorage" table.

        Args:
            db_name (str): The name of the datastorage (collection or table).
            database_id (int): The entry id of server.
            parent_datastorage_id (int): Optional for embedded objects. The ID of the parent datastorage.
        Returns:
            int: The ID of the newly added entry.
        """
        # Insert new entry into datastorage table
        if parent_datastorage_id is None:
            # If its not an embedded obejct. 
            insert_query = """
                INSERT INTO datastorage (storage_name, db_id)
                VALUES (%s, %s);
            """
            new_entry_id = self.query_insert(insert_query, (storage_name, databse_id))
        else:
            insert_query = """
                INSERT INTO datastorage (storage_name, db_id, parent_id)
                VALUES (%s, %s, %s);
            """                
            new_entry_id = self.query_insert(insert_query, (storage_name, databse_id, parent_datastorage_id))
        return new_entry_id

    def add_attribute(self, attribut_name, datastorage_id):
        """
        Adds a new entry to the "attributes" table.

        Args:
            attribut_name (str): The name of the attribute.
            datastorage_id (int): The entry id of datastorage.

        Returns:
            int: The ID of the newly added entry.
        """
        # Insert new entry into attribute table
        insert_query = """
            INSERT INTO loaded_attributes (attribute_name, datastorage_id )
            VALUES (%s, %s);
        """
        new_entry_id = self.query_insert(insert_query, (attribut_name, datastorage_id))
        return new_entry_id

    def add_value(self, attribute_id, entry_no, value, value_type, value_length, position):
        """
        Adds value to database.

        Args:
            attribute_id (int): ID of the Attribute.
            entry_no (int): Number of the entry.
            value (string): Value
            value_type (string): Type of the value.
            value_lenght (int): Lenght of the entry string.
            position (int): Position of the attribute in the document.
        """
        table_name = "loaded_values"
        # Insert new entry into value table
        insert_query = f"""
            INSERT INTO {table_name} (attribute_id, entry_no, value, value_type, length, position)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        self.query_insert(insert_query, (attribute_id, entry_no, value, value_type, value_length, position))    

    def add_UAC(self, server, database, datastorage, attributes):
        """
        Adds a UAC to the table.

        Attributes:
            server (int): server_id
            database (int): database_id
            datastorage (int): datastorage_id
            attributes (list): List of attribute_ids.

        Returns:
            int: The ID of the newly added entry.
        """      
        server_host = self.get_server_host(server)
        server_port = self.get_server_port(server)
        server_type = self.get_server_type(server)
        database_name = self.get_database_name(database)
        datastorage_name = self.get_datastorage_name(datastorage)

        attribute_names = []
        attribute_types = []
        for attribute_id in attributes:
            attribute_name = self.get_attribute_name(attribute_id)
            attribute_names.append(attribute_name)
            attribute_type = self.get_attribute_types(attribute_id)
            attribute_types.extend(attribute_type)
        attributes_ids_string = ", ".join(map(str, attributes))
        attribute_names_string = ", ".join(attribute_names)
        attribute_types = list(set(attribute_types)) # Remove double entries
        attribute_types_string = ", ".join(attribute_types)

        # Insert new entry into servers table
        insert_query = """
            INSERT INTO unique_attributecombinations (
                server_id, server_host, server_port, server_type, 
                db_id, db_name,
                datastorage_id, datastorage_name, 
                attribute_ids, attribute_names, attribute_types
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """ 
        new_entry_id = self.query_insert(
            insert_query, (
            server, server_host, server_port, server_type, 
            database, database_name,
            datastorage, datastorage_name,
            attributes_ids_string, attribute_names_string, attribute_types_string
        ))

        return new_entry_id 

    def add_UAC_PKscores(self, UAC_id, score_cardinality, score_valuelenght, score_position, score_namesuffix, score_datatype):
        """
        Adds a PK scores to the given UAC.

        Args:
            UAC_id (int): The ID of the UAC.
            score_cardinality (float): Score for the cardinality.
            score_valuelenght (float): Score for the value lenght.
            score_position (float): Score for the position.
            score_namesuffix (float): Score for the name suffix.
            score_datatype (float): Score for the datatype.
        """
        query = f"""
            UPDATE unique_attributecombinations SET 
            score_cardinality = {score_cardinality},
            score_valuelenght = {score_valuelenght},
            score_position = {score_position},
            score_namesuffix = {score_namesuffix},
            score_datatype = {score_datatype}
            WHERE id = {UAC_id};
        """
        self.query_update(query)

    def add_UAC_possibility(self, UAC_id, hopf_score, iris_score):
        """
        Adds a PK possibilities to the given UAC.

        Args:
            UAC_id (int): The ID of the UAC.
            hopf_score (int): Possbility HoPF-Score.
            iris_score (int): Possbility IRIS-Score.
        """
        query = f"""
            UPDATE unique_attributecombinations SET 
            pk_score_hopf = {hopf_score},
            pk_score_iris = {iris_score}
            WHERE id = {UAC_id};
        """
        self.query_update(query)

    def add_IND_FKscores(self, 
                         IND_id, name_weighted_similarity, bhattacharyya, iris_similarity, hybrid_score, 
                         hopf_probability, iris_probability, hybrid_only_name_probability, hybrid_probability):
        """
        Adds a FK scores to the given IND.

        Args:
            UAC_id (int): The ID of the UAC.
            name_weighted_similarit (float): Score of the weighted syntactic name similarity.
            name_similarity (float): Score of the syntactic name similarity.
            bhattacharyya (float): Score of the datadistribution.
            iris_similarity (float): Score of the IRIS score.
        """
        query = f"""
            UPDATE inclusion_dependencies SET 
            score_syntactic_name_similarity = {name_weighted_similarity},
            score_datadistribution = {bhattacharyya},
            score_IRIS_name_similarity = {iris_similarity},
            score_hybrid_name_similarity = {hybrid_score},
            HoPF_probability = {hopf_probability},
            IRIS_probability = {iris_probability},
            hybrid_only_name_probability = {hybrid_only_name_probability},
            hybrid_probability = {hybrid_probability} 
            WHERE id = {IND_id};
        """
        self.query_update(query)

    def add_IND(self, UAC_id, child_server, child_database, child_datastorage, child_attributes):
        """
        Adds a IND to the table.

        Attributes:
            UAC_id (int): The ID of the UAC.
            child_server (int): server_id of the child.
            child_database (int): database_id of the child.
            child_datastorage (int): datastorage_id of the child.
            child_attributes (list): List of attributes IDs of the child.

        Returns:
            int: The ID of the newly added entry.        
        """
        child_server_host = self.get_server_host(child_server)
        child_server_port = self.get_server_port(child_server)
        child_server_type = self.get_server_type(child_server)
        child_database_name = self.get_database_name(child_database)
        child_datastorage_name = self.get_datastorage_name(child_datastorage)

        child_attribute_names = []
        for attribute_id in child_attributes:
            child_attribute_name = self.get_attribute_name(attribute_id)
            child_attribute_names.append(child_attribute_name)     
        child_attribute_names_string = ", ".join(child_attribute_names)
        child_attribute_ids_string = ", ".join(map(str, child_attributes))

        # Insert new entry into servers table
        insert_query = """
            INSERT INTO inclusion_dependencies (
                UAC_id, 
                child_server_id, child_server_host, child_server_port, child_server_type,
                child_db_id, child_db_name,
                child_datastorage_id, child_datastorage_name,
                child_attribute_ids, child_attribute_names
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        new_entry_id = self.query_insert(
            insert_query, (
            UAC_id,
            child_server, child_server_host, child_server_port, child_server_type,
            child_database, child_database_name,
            child_datastorage, child_datastorage_name,
            child_attribute_ids_string, child_attribute_names_string
        ))

        return new_entry_id

    def add_maxIND(self, parent_server_id, parent_database_id, parent_datastorage_id, parent_attribute_ids,
                   child_server_id, child_database_id, child_datastorage_id, child_attribute_ids):
        """
        Adds a maximal IND to the table.

        Args:
            parent_server_id (int): The server ID of the parent.
            parent_database_id (int): The database ID of the parent.
            parent_datastorage_id (int): The data storage ID of the parent.
            parent_attribute_ids (list[int]): List of attribute IDs of the parent.
            child_server_id (int): The server ID of the child.
            child_database_id (int): The database ID of the child.
            child_datastorage_id (int): The data storage ID of the child.
            child_attribute_ids (list[int]): List of attribute IDs of the child.

        Returns:
            int: The ID of the newly added entry.        
        """
        # Parent information
        parent_server_host = self.get_server_host(parent_server_id)
        parent_server_port = self.get_server_port(parent_server_id)   
        parent_server_type = self.get_server_type(parent_server_id)
        parent_database_name = self.get_database_name(parent_database_id)    
        parent_datastorage_name = self.get_datastorage_name(parent_datastorage_id)
        parent_attribute_names = []
        for parent_attribute_id in parent_attribute_ids:
            name = self.get_attribute_name(parent_attribute_id)
            parent_attribute_names.append(name)
        parent_attribute_names_string = ", ".join(parent_attribute_names)
        parent_attribute_ids_string = ", ".join(map(str, parent_attribute_ids))
        # Child information
        child_server_host = self.get_server_host(child_server_id)
        child_server_port = self.get_server_port(child_server_id)
        child_server_type = self.get_server_type(child_server_id)
        child_database_name = self.get_database_name(child_database_id)
        child_datastorage_name = self.get_datastorage_name(child_datastorage_id)
        child_attribute_names = []
        for child_attribute_id in child_attribute_ids:
            name = self.get_attribute_name(child_attribute_id)
            child_attribute_names.append(name)
        child_attribute_names_string = ", ".join(child_attribute_names)
        child_attribute_ids_string = ", ".join(map(str, child_attribute_ids))

        # Insert new entry into servers table
        insert_query = """
            INSERT INTO max_inclusion_dependencies (
                parent_server_id, parent_server_host, parent_server_port, parent_server_type,
                parent_db_id, parent_db_name, parent_datastorage_id, parent_datastorage_name,
                parent_attribute_ids, parent_attribute_names,           
                child_server_id, child_server_host, child_server_port, child_server_type,
                child_db_id, child_db_name, child_datastorage_id, child_datastorage_name,
                child_attribute_ids, child_attribute_names
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        new_entry_id = self.query_insert(
            insert_query, (
                parent_server_id, parent_server_host, parent_server_port, parent_server_type, parent_database_id, parent_database_name,
                parent_datastorage_id, parent_datastorage_name, parent_attribute_ids_string, parent_attribute_names_string,
                child_server_id, child_server_host, child_server_port, child_server_type, child_database_id, child_database_name,
                child_datastorage_id, child_datastorage_name, child_attribute_ids_string, child_attribute_names_string))
        
        return new_entry_id

    def add_value_batchimport(self, attribute_id, entry_no, value, value_type, value_length, position):
        """
        Adds value to database.

        Args:
            attribute_id (int): ID of the Attribute.
            entry_no (int): Number of the entry.
            value (string): Value
            value_type (string): Type of the value.
            value_lenght (int): Lenght of the entry string.
            position (int): Position of the attribute in the document.
        """
        parameter_list = [attribute_id, entry_no, value, value_type, value_length, position]
        self.list_values_batchimport.append(parameter_list)
        batch_size = 10000
        if len(self.list_values_batchimport) >= batch_size:
            self.add_value_batchimport_end()

    def add_value_batchimport_end(self):
        """
        
        """
        # Insert new entry into value table
        query = f"""
            INSERT INTO loaded_values (attribute_id, entry_no, value, value_type, length, position)
            VALUES (%s, %s, %s, %s, %s, %s);
        """        
        if not self.connection:
            print("Not connected to MariaDB.")
            return None
        
        try:
            cursor = self.connection.cursor()
            for parameters in self.list_values_batchimport:
                cursor.execute(query, parameters)
            self.connection.commit()
            self.list_values_batchimport = [] # Empty list
        except mariadb.Error as err:
            print(f"Error insert tuple: {err}")
        finally:
            if cursor:
                cursor.close()   

    def add_explicit_reference(self, UAC_id, IND_id):
        """
        Adds a explicit reference.
        
        Args:
            UAC_id (int): The ID of the UAC.
            IND_id (int): The ID of the IND.
        """
        insert_query = """
            INSERT INTO explicit_references (UAC_id, IND_id)
            VALUES (%s, %s);
        """
        new_entry_id = self.query_insert(insert_query, (UAC_id, IND_id))

    def add_implicitly_reference(self, UAC_id, IND_id):
        """
        Adds a implicitly reference.

        Args:
            UAC_id (int): The ID of the UAC.
            IND_id (int): The ID of the IND.
        """
        insert_query = """
            INSERT INTO implicitly_references (UAC_id, IND_id)
            VALUES (%s, %s);
        """
        new_entry_id = self.query_insert(insert_query, (UAC_id, IND_id))

    def add_primarykey(self, UAC_id):
        """
        Adds a primarykey.
        
        Args:
            UAC_id (int): The ID of the UAC.
        """
        insert_query = """
            INSERT INTO primarykeys (UAC_id)
            VALUES (%s);
        """
        new_entry_id = self.query_insert(insert_query, ([UAC_id]))

    # Functions to get entries

    def get_number_of_valueentries(self, attribute_id):
        """
        Returns the number of entires for the selected attribute ID.

        Args:
            attribute_id(int): The ID of the attribute.
        """
        # Insert new entry into servers table
        query = f"""
            SELECT COUNT(*) AS cnt FROM loaded_values WHERE attribute_id = {attribute_id};
        """  
        
        query_result = self.query(query)
        result = query_result[0][0]
        return int(result)

    def get_values_for_attribute(self, attribute_id):
        """
        Returns the values for the selected attribute ID.

        Args:
            attribute_id(int): The ID of the attribute.

        Return:
            list of string: List with the values.
        """
        # Insert new entry into servers table
        query = f"""
            SELECT value FROM loaded_values WHERE attribute_id = {attribute_id};
        """  
        
        query_result = self.query(query)
        result = [item[0] for item in query_result]
        return result
  
    def get_UACs(self):
        """
        Returns all Unique AttributeCombinations as a list with dictionarys.

        Return:
            dic: Dictionary with the UACs.
        """       
        list_UACs = []

        query = f"""
            SELECT id, server_id, db_id, datastorage_id, attribute_ids, pk_score_hopf, pk_score_iris FROM unique_attributecombinations;
        """  
        query_result = self.query(query)
        for entry in query_result:
            # Split attributes to list
            attributes = [int(x) for x in entry[4].split(',')]
            # Build UAC entry
            UAC_entry = {
                "UAC_id": entry[0],
                "server_id": entry[1],
                "database_id": entry[2],
                "datastorage_id": entry[3],
                "attributes": attributes,
                "hopf_score": entry[5],
                "iris_score": entry[6]
            }
            list_UACs.append(UAC_entry)
        return list_UACs

    def get_UAC_via_id(self, UAC_id):
        """
        Returns the Unique AttributeCombinations as a dictionarys for the selected id.

        Args:
            UAC_id(int): The ID of the UAC.

        Return:
            dic: Dictionary with the UACs.
        """
        # Insert new entry into servers table
        query = f"""
            SELECT id, server_id, db_id, datastorage_id, attribute_ids FROM unique_attributecombinations
            WHERE id = {UAC_id};
        """  
        
        query_result = self.query(query)
        entry = query_result[0]
        # Split attributes to list
        attributes = [int(x) for x in entry[4].split(',')]
        # Build UAC entry
        UAC = {
            "UAC_id": entry[0],
            "server_id": entry[1],
            "database_id": entry[2],
            "datastorage_id": entry[3],
            "attributes": attributes
        }
        return UAC

    def get_INDs(self):
        """
        Returns all InclusionDependencies as a list with dictionarys.

        Returns:
            dic: With the INDs.
        """       
        list_INDs = []

        # Insert new entry into servers table
        query = f"""
            SELECT id, UAC_id, child_server_id, child_db_id, child_datastorage_id, child_attribute_ids, 
            HoPF_probability, IRIS_probability, hybrid_only_name_probability, hybrid_probability
            FROM inclusion_dependencies;
        """  
        query_result = self.query(query)
        for entry in query_result:
            # Split attributes to list
            attributes = [int(x) for x in entry[5].split(',')]
            # Build UAC entry
            IND_entry = {
                "IND_id":entry[0],
                "UAC_id": entry[1],
                "server_id": entry[2],
                "database_id": entry[3],
                "datastorage_id": entry[4],
                "attributes": attributes,
                "hopf_probability": entry[6],
                "iris_probability": entry[7],
                "hybrid_only_name_probability": entry[8],
                "hybrid_probability": entry[9]
            }
            list_INDs.append(IND_entry)
        return list_INDs

    def get_servers(self):
        """
        Private method to retrieve a list of server IDs from the database.

        Returns:
          list: List of server IDs.        
        """
        query = "SELECT id FROM servers;"
        query_result = self.query(query)
        result = [item[0] for item in query_result]
        return result

    def get_databases(self, server_id):
        """
        Private method to retrieve a list of database IDs for a given server ID.

        Attributes:
          server_id (int): The ID of the server.

        Returns:
          list: List of database IDs.        
        """
        query = f"SELECT id FROM loaded_databases WHERE server_id={server_id};"
        query_result = self.query(query)
        result = [item[0] for item in query_result]
        return result

    def get_datastorages(self, database_id):
        """
        Private method to retrieve a list of data storage IDs for a given database ID.

        Attributes:
          database_id (int): The ID of the database.

        Returns:
          list: List of data storage IDs.        
        """
        query = f"SELECT id FROM datastorage WHERE db_id={database_id};"
        query_result = self.query(query)
        result = [item[0] for item in query_result]
        return result
    
    def get_datastorage_id_for_attribute_id(self, attribute_id):
        """
        Function to retrieve the ID of a data storage for a given attribute ID.

        Attributes:
          attribute_id (int): The ID of the attribute.

        Returns:
          int: ID of the data storage.        
        """
        query = f"SELECT datastorage_id FROM loaded_attributes  WHERE id={attribute_id};"
        query_result = self.query(query)
        result = query_result[0][0]
        return int(result)

    def get_attributes(self, datastorage_id):
        """
        Private method to retrieve a list of attribute IDs for a given data storage ID.

        Attributes:
          datastorage_id (int): The ID of the data storage.

        Returns:
          list: List of attribute IDs.
        """
        query = f"SELECT id FROM loaded_attributes WHERE datastorage_id={datastorage_id};"
        query_result = self.query(query)
        result = [item[0] for item in query_result]
        return result

    def get_attribute_types(self, attribute_id):
        """
        Method to retrieve a list of attribute IDs for a given data storage ID.

        Attributes:
          attribute_id (int): The ID of the attribute.

        Returns:
          list of strings: The attribute types.        
        """
        query = f"SELECT DISTINCT(value_type) FROM loaded_values WHERE attribute_id = {attribute_id};"
        query_result = self.query(query)
        result = query_result[0]
        return result

    def get_attribute_min(self, attribute_id):
        """
        Loads the minimum value for a given attribute from the database.

        Parameters:
          attribute_id (int): The ID of the attribute.

        Returns:
          str: The minimum value for the attribute.
        """
        query = f"SELECT MIN(value) FROM loaded_values WHERE attribute_id = {attribute_id};"
        query_result = self.query(query)
        result = query_result[0][0]
        return result

    def get_attribute_max(self, attribute_id):
        """
        Loads the maximum value for a given attribute from the database.

        Parameters:
          attribute_id (int): The ID of the attribute.

        Returns:
          str: The maximum value for the attribute.
        """
        query = f"SELECT MAX(value) FROM loaded_values WHERE attribute_id = {attribute_id};"
        query_result = self.query(query)
        result = query_result[0][0]
        return result

    def get_max_entry_number(self, attribute_ids):
        """
        Gets the maximal entry number for the given attributes.

        Args:
            attribute_ids (list of ints): List with attribute IDs.

        Returns:
            int: Maximum entry number,
        """
        combination_string = ", ".join(map(str, attribute_ids))
        query = f"""
          SELECT MAX(entry_no) FROM loaded_values WHERE attribute_id IN ({combination_string});
        """
        query_result = self.query(query)
        number_of_entries = query_result[0][0]   
        return int(number_of_entries)     

    def get_number_of_entries(self, attribute_id):
        """
        Gets the number of entries for the given attribute.

        Args:
            attribute_id: The ID of the attribute.

        Returns:
            int: Number of entries.        
        """
        query = f"""
            SELECT COUNT(*) FROM loaded_values WHERE attribute_id = {attribute_id};
        """
        query_result = self.query(query)
        entries = query_result[0][0]
        return int(entries)

    def get_attribute_position(self, attribute_id):
        """
        Gets the average position of the attribute in the documents or table for the given attribute.

        Args:
            attribute_id (int): The ID of the attribute.

        Returns:
            float: The average postion.             
        """
        query = f"""
          SELECT AVG(position) AS average_position FROM loaded_values  WHERE attribute_id = ({attribute_id})
        """
        query_result = self.query(query)
        result = query_result[0][0]
        return float(result)           

    def get_max_value_length_of_attribute(self, attribute_id):
        """
        Gets the maximal value length of the value from the given attribute ID.

        Args:
            attribute_id (int): The ID of the attribute.

        Returns:
            int: Length of the longest value.            
        """
        query = f"""
          SELECT MAX(length) FROM loaded_values WHERE attribute_id = ({attribute_id})
        """
        query_result = self.query(query)
        result = query_result[0][0]
        return result

    def get_number_of_unique_entries_for_attributes(self, list_attribute_ids):
        """
        Gets the number of unqiue entires for the given attributes.

        Args:
            list_attribute_ids (list): List with Attribute IDs to test.
        Returns:
            int: Number of unqiue entries.         
        """
        combination_string = ", ".join(map(str, list_attribute_ids))
        if self.DBType == "MariaDB":
            query = f"""
                SELECT COUNT(DISTINCT combined_values) AS cnt
                FROM (
                    SELECT GROUP_CONCAT(attribute_id, value) AS combined_values
                    FROM loaded_values
                    WHERE attribute_id IN ({combination_string})
                    GROUP BY entry_no
                ) q;
            """
        elif self.DBType == "PostgreSQL":
            query = f"""
                SELECT COUNT(DISTINCT combined_values) AS cnt
                FROM (
                    SELECT STRING_AGG(CONCAT(attribute_id, value), '') AS combined_values
                    FROM loaded_values
                    WHERE attribute_id IN ({combination_string})
                    GROUP BY entry_no
                ) AS subquery;
            """
        query_result = self.query(query)
        number_of_unique_entries = query_result[0][0]
        return int(number_of_unique_entries)

    def get_value_at_specific_position(self, attribute_id, position):
        """
        This function sorts the values und selects the value at a specific position.

        Args:
            attribute_id (int): The ID of the attribute.
            postion (int): Postion of the value.

        Returns:
            str: Value at the position.
        """
        query = f"""
            SELECT value FROM loaded_values WHERE attribute_id = {attribute_id} ORDER BY value LIMIT 1 OFFSET {position};
        """
        query_result = self.query(query)
        value = query_result[0][0]
        return value
    
    def count_values_to_specific_value(self, attribute_id, value):
        """
        Counts the number of values until the given value.

        Args:
            attribute_id (int): The ID of the attribute.
            value (str): The value.

        Returns:
            int: The count of values until the given value.        
        """
        query = f"""
            SELECT COUNT(*) AS cnt
            FROM loaded_values WHERE attribute_id = {attribute_id}
            AND value BETWEEN '' AND '{value}';
        """
        query_result = self.query(query)
        result = query_result[0][0]
        return result

    def get_list_of_entry_nos_for_attribute(self, attribute_id):
        """
        Returns the list of entry numbers for the given attribute ID.

        Args:
            attribute_id (int): The ID of the attribute.

        Returns:
            list: List of intergers, with the entry numbers.        
        """
        query = f"""
            SELECT entry_no FROM loaded_values WHERE attribute_id = {attribute_id};
        """
        query_result = self.query(query)
        list = [item[0] for item in query_result]
        return list
    
    def get_related_entry_numbers(self, parent_attribute_id, child_attribute_id, child_entry_no):
        """"
        Selects the value of a specific entry from the child. Then returns all entry numbers from the
        parent attribute, that contain the value.

        Args:
            parent_attribute_id (int): The ID of the parent attribute.
            child_attribute_id (int): The ID of the child attribute.
            child_entry_no (int): The entry number of the child to test.

        Returns:
            list: List of intergers, with the entry numbers.            
        """
        query = f"""
            SELECT entry_no
            FROM loaded_values
            WHERE attribute_id = {parent_attribute_id}
            AND value = (
                SELECT value
                FROM loaded_values
                WHERE entry_no = {child_entry_no}
                AND attribute_id = {child_attribute_id}
            );
        """
        query_result = self.query(query)  
        result = [item[0] for item in query_result]
        return result

    def get_maximal_inclusion_dependencies(self):
        """
         Loads all maximal inculsion dependencies from the database and returns them as a dictionary.

        Returns:
            List: List of dictionaries with the maximal inculsion dependencies.       
        """
        query = f"""
            SELECT * FROM max_inclusion_dependencies;
        """
        query_result = self.query(query)
        result = []
        for entry in query_result:
            dic = {
                "parent_server_host": entry[2],
                "parent_server_port": entry[3],
                "parent_server_type": entry[4],
                "parent_db_name": entry[6],
                "parent_datastorage_name": entry[8],
                "parent_attribute_names": entry[10],
                "child_server_host": entry[12],
                "child_server_port": entry[13],
                "child_server_type": entry[14],
                "child_db_name": entry[16],
                "child_datastorage_name": entry[18],
                "child_attribute_names": entry[20]
            }
            result.append(dic)
        return result

    def get_implicite_references(self):
        """
        Loads all implicite references from the database and returns them as a dictionary.

        Returns:
            List: List of dictionaries with the implicite References.
        """
        query = f"""
            SELECT * FROM view_implicitly_reference;
        """
        query_result = self.query(query)
        result = []
        for entry in query_result:
            dic = {
                "primarykey_database_type": entry[3],
                "primarykey_host": entry[4],
                "primarykey_port": entry[5],
                "primarykey_database": entry[6],
                "primarykey_datastorage": entry[7],
                "primarykey_attributes": entry[8],
                "foreignkey_database_type": entry[9],
                "foreignkey_host": entry[10],
                "foreignkey_port": entry[11],
                "foreignkey_database": entry[12],
                "foreignkey_datastorage": entry[13],
                "foreignkey_attributes": entry[14],
                "datatypes": entry[15]
            }
            result.append(dic)
        return result

    def get_explicite_references(self):
        """
        Loads all explicite refences from the database and returns them as a dictionary.

        Returns:
            List: List of dictionaries with the explicite References.
        """
        query = f"""
            SELECT * FROM view_explicite_references;
        """
        query_result = self.query(query)
        result = []
        for entry in query_result:
            dic = {
                "primarykey_database_type": entry[3],
                "primarykey_host": entry[4],
                "primarykey_port": entry[5],
                "primarykey_database": entry[6],
                "primarykey_datastorage": entry[7],
                "primarykey_attributes": entry[8],
                "foreignkey_database_type": entry[9],
                "foreignkey_host": entry[10],
                "foreignkey_port": entry[11],
                "foreignkey_database": entry[12],
                "foreignkey_datastorage": entry[13],
                "foreignkey_attributes": entry[14],
                "datatypes": entry[15]
            }
            result.append(dic)
        return result

    def get_primarykeys(self):
        """
        Loads all primarykeys from the database and returns them as a dictionary.

        Returns:
            List: List of dictionaries with the primarykeys.
        """
        query = f"""
            SELECT * FROM  view_primarykeys ;
        """
        query_result = self.query(query)
        result = []
        for entry in query_result:
            dic = {
                "database_type": entry[2],
                "host": entry[3],
                "port": entry[5],
                "database": entry[5],
                "datastorage": entry[6],
                "attributes": entry[7],
                "datatypes": entry[8]
            }
            result.append(dic)
        return result

    def get_server_host(self, server_id):
        """
        Method to retrieve the host of a server for the given server ID.

        Attributes:
          server_id (int): The ID of server.

        Returns:
          string: The host of server.
        """
        query = f"SELECT host FROM servers  WHERE id = {server_id}"
        query_result = self.query(query)
        result = query_result[0][0]
        return result

    def get_server_port(self, server_id):
        """
        Method to retrieve the port of a server for the given server ID.

        Attributes:
          server_id (int): The ID of server.

        Returns:
          int: The port of server.
        """
        query = f"SELECT port FROM servers  WHERE id = {server_id}"
        query_result = self.query(query)
        result = query_result[0][0]
        return result

    def get_server_type(self, server_id):
        """
        Method to retrieve the type of a server for the given server ID.

        Attributes:
          server_id (int): The ID of server.

        Returns:
          str: The type of server.
        """
        query = f"SELECT server_type FROM servers  WHERE id = {server_id}"
        query_result = self.query(query)
        result = query_result[0][0]
        return result

    def get_database_name(self, database_id):
        """
        Function to retrieve the name of a database for a given database ID.

        Attributes:
          attribute_id (int): The ID of the attribute.

        Returns:
          string: The name of attribute.
        """
        query = f"SELECT db_name FROM loaded_databases  WHERE id = {database_id}"
        query_result = self.query(query)
        result = query_result[0][0]
        return result

    def get_datastorage_name(self, datastorage_id):
        """
        Function to retrieve the name of a datastorage for a given datastorage ID.

        Attributes:
          datastorage_id (int): The ID of the datastorage.

        Returns:
          string: Name of the data storage.        
        """
        query = f"SELECT storage_name FROM datastorage WHERE id={datastorage_id}"
        query_result = self.query(query)
        result = query_result[0][0]
        return result

    def get_attribute_name(self, attribute_id):
        """
        Function to retrieve the name of a attribute for a given attribute ID.

        Attributes:
          attribute_id (int): The ID of the attribute.

        Returns:
          string: The name of attribute.
        """
        query = f"SELECT attribute_name FROM loaded_attributes WHERE id = {attribute_id}"
        query_result = self.query(query)
        result = query_result[0][0]
        return result

    def get_datastorage_embedded_in(self, datastorage_id):
        """
        Function to check if the datastorage is embedded in another datastorage.

        Attributes:
          datastorage_id (int): The ID of the datastorage.

        Returns:
          string: Name of the data storage. "not_embedded" if its not embedded.      
        """
        query = f"SELECT parent_id FROM datastorage WHERE id={datastorage_id}"
        query_result = self.query(query)
        result = query_result[0][0]
        if result:
            return self.get_datastorage_name(datastorage_id)
        else:
            return "not_embedded"
    
    # Functions to check something

    def check_if_value_exist(self, value, attribute_id):
        """
        Checks if the given value exist in the value list for the given Attribute ID.

        Attributes:
            value(any): Value to search.
            attribute_id(int): The Attribute ID of the Attribute to check.
        Returns:
            boolean: Ture if the value exist.
        """
        query =f"""SELECT COUNT(1) FROM loaded_values WHERE attribute_id = {attribute_id} AND value = %s;"""
        query_result = self.query(query, (value,))
        result = query_result[0][0]
        if result > 0:
            return True
        else:
            return False

    def check_if_UAC_id_for_attributes(self, attribute_ids):
        """
        Checks if a UAC for the given attributes exist. If it exist it returns the id. Else it returns None.

        Args:
            attribute_ids (list of int): List with the attribute ids.

        Returns:
            int: The ID of the UAC. Returns None if the there is no UAC.
        """
        attribute_ids= [str(x) for x in attribute_ids]
        attribute_string = ', '.join(attribute_ids)
        query = f"""
            SELECT id FROM unique_attributecombinations 
            WHERE attributes  = '{attribute_string}';
        """
        query_result = self.query(query)
        if query_result: # Checks if result is empty
            result = query_result[0][0]
        else:
            result = None
        return result      

    def check_if_attribute_contains_array(self, attribute_id):
        """
        Checks if attribute contains an array.
        
        Args:
            attribute_id (int) The ID of the attribute.
        Returns:
            bool: True if there is an array.
        """
        query = f"""
              SELECT entry_no, COUNT(*) AS count FROM loaded_values WHERE attribute_id = {attribute_id} 
              GROUP BY entry_no ORDER BY count DESC LIMIT 1;
        """
        query_result = self.query(query)
        if 1 < query_result[0][1]:   
            return True
        else:
            return False     

    def check_if_attribut_has_a_appropriate_entry_no(self,list_attribute_ids):
        """
        Test if every attribute in the list has matching entry_no for every entry.

        Args:
            list_attribute_ids (list): List with Attribute IDs to test.
        Returns:
            bool: True if every attribute has a appropriate entry_no.        
        """
        count_entries = len(list_attribute_ids)
        string_attribute_ids = ", ".join(map(str, list_attribute_ids))
        query = f"""
            SELECT COUNT(*) AS count
            FROM loaded_values WHERE attribute_id IN ({string_attribute_ids})
            GROUP BY entry_no
            HAVING COUNT(DISTINCT attribute_id) < {count_entries}
            LIMIT 1;
        """
        query_result = self.query(query)
        if not query_result:
            return True
        else:
            return False    

    def Nary_IND_test(self, list_parent_ids, list_child_ids):
        """
        Checks for N-ary INDs.

        Args:
            parent_id (list): The Attribute IDs of the parent Attributes to check.        
            child_id (list): The Attribute IDs of the child Attributes to check. 
        Returns:
            boolean: True if its a N-ary IND.        
        """
        string_parent_ids = ", ".join(map(str, list_parent_ids))
        string_child_ids =  ", ".join(map(str, list_child_ids))
        if self.DBType == "MariaDB":
            query = f"""
                SELECT COUNT(*) AS unmatched
                FROM (
                    SELECT GROUP_CONCAT(DISTINCT value) AS child_values
                    FROM loaded_values
                    WHERE attribute_id IN ({string_child_ids})
                    GROUP BY entry_no
                ) child
                WHERE NOT EXISTS (
                    SELECT *
                    FROM (
                        SELECT GROUP_CONCAT(DISTINCT value) AS parent_values
                        FROM loaded_values
                        WHERE attribute_id IN ({string_parent_ids})
                        GROUP BY entry_no
                    ) parent
                    WHERE child.child_values = parent.parent_values
                );            
            """
        elif self.DBType == "PostgreSQL":
            query = f"""
                SELECT COUNT(*) AS unmatched
                FROM (
                    SELECT STRING_AGG(DISTINCT value, ',') AS child_values
                    FROM loaded_values
                    WHERE attribute_id IN ({string_child_ids})
                    GROUP BY entry_no
                ) child
                WHERE NOT EXISTS (
                    SELECT *
                    FROM (
                        SELECT STRING_AGG(DISTINCT value, ',') AS parent_values
                        FROM loaded_values
                        WHERE attribute_id IN ({string_parent_ids})
                        GROUP BY entry_no
                    ) parent
                    WHERE child.child_values = parent.parent_values
                );            
            """            
        query_result = self.query(query)
        result = query_result[0][0]
        if result == 0:
            return True
        else:
            return False

    def unary_IND_test(self, parent_id, child_id):
        """
        Checks for unary INDs.

        Attributes:
            parent_id(int): The Attribute ID of the parent Attribute to check.        
            child_id(int): The Attribute ID of the child Attribute to check. 
        Returns:
            boolean: True if its a unary IND.
        """
        query =f"""
            SELECT COUNT(*) AS count_child_only
            FROM loaded_values child
            LEFT JOIN loaded_values parent ON child.value = parent.value AND parent.attribute_id = {parent_id}
            WHERE parent.value IS NULL AND child.attribute_id = {child_id};
        """
        query_result = self.query(query)
        result = query_result[0][0]
        if result == 0:
            return True
        else:
            return False

    # Functions to calculate

    def calculate_PKscores_for_datastorage(self, datastorage_id, PKmetrics):
        """
        Function to calculate the PKscores for the given metrics.

        Args:
            datastorage_id (int): The ID of the datastorage.
            PKmetrics (list of strings): The name of the metrics for the PKscore.

        Returns:
            list: Sorted list with the calculated PKscores.
        """
        metrics_string = ' + '.join(PKmetrics)
        query = f"""
            SELECT {metrics_string} AS sum 
            FROM unique_attributecombinations WHERE datastorage_id = {datastorage_id} 
            ORDER BY sum DESC;
        """
        query_result = self.query(query)
        result = [item[0] for item in query_result]
        return result
    
    def calculate_PKscore_for_id(self, UAC_id, PKmetrics):
        """
        Function to calculate the PK score for the given metrics.

        Args:
            UAC_id (int): The ID of the UAC.
            PKmetrics (list of strings): The name of the metrics for the PKscore.

        Returns:
            float: The calculated PKscore.
        """
        metrics_string = ' + '.join(PKmetrics)
        query = f"""
            SELECT {metrics_string} AS sum 
            FROM unique_attributecombinations WHERE id = {UAC_id};
        """
        query_result = self.query(query)
        result = query_result[0][0]
        return result
    
    def calculate_FKscore(self, IND_id, FKmetrics):
        """
        Function to calculate the FK score for the given metrics.

        Args:
            IND_id (int): The ID of the IND.
            PKmetrics (list of strings): The name of the metrics for the PKscore.

        Returns:
            list: Sorted list with the calculated PKscores.
        """
        metrics_string = ' + '.join(FKmetrics)
        query = f"""
            SELECT {metrics_string} AS sum 
            FROM inclusion_dependencies WHERE id = {IND_id};
        """
        query_result = self.query(query)
        result = query_result[0][0]
        return result 

    # Functions to create tables

    def _create_table_servers(self):
        """
        Creates a new "servers" table for the databases.
        """

        table_name = "servers"
        if self.DBType == "MariaDB":
            table_query = """
                id INT NOT NULL AUTO_INCREMENT,
                host VARCHAR(50) NOT NULL,
                port INT,
                server_type VARCHAR(50) NOT NULL,
                PRIMARY KEY (id)
            """
        elif self.DBType == "PostgreSQL":
            table_query = """
                id SERIAL,
                host VARCHAR(50) NOT NULL,
                port INT,
                server_type VARCHAR(50) NOT NULL,
                PRIMARY KEY (id)
            """
        elif self.DBType == "ApacheSpark":
             table_query = """
                id SERIAL,
                host VARCHAR(50) NOT NULL,
                port INT,
                server_type VARCHAR(50) NOT NULL
            """
        self._create_new_table(table_name, table_query)

    def _create_table_loadeddatabases(self):
        """
        Creates a new "loaded_databases" table for the databases.
        """

        table_name = "loaded_databases" #Name databases isn´t allowed in MariaDB
        if self.DBType == "MariaDB":
            table_query = """
                id INT NOT NULL AUTO_INCREMENT,
                db_name VARCHAR(50) NOT NULL,
                server_id INT,
                FOREIGN KEY(server_id) REFERENCES servers(id) ON DELETE CASCADE,
                PRIMARY KEY (id) 
            """
        elif self.DBType == "PostgreSQL":
            table_query = """
                id SERIAL,
                db_name VARCHAR(50) NOT NULL,
                server_id INT,
                FOREIGN KEY(server_id) REFERENCES servers(id) ON DELETE CASCADE,
                PRIMARY KEY (id) 
            """
        self._create_new_table(table_name, table_query)

    def _create_table_datastorage(self):
        """
        Creates a new "datastorage" table for the databases.
        Saves name of collection and tables.
        """

        table_name = "datastorage"
        if self.DBType == "MariaDB":
            table_query = """
                id INT NOT NULL AUTO_INCREMENT,
                storage_name VARCHAR(50) NOT NULL,
                db_id INT NOT NULL,
                parent_id INT,
                FOREIGN KEY(db_id) REFERENCES loaded_databases(id) ON DELETE CASCADE,
                FOREIGN KEY(parent_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        elif self.DBType == "PostgreSQL":
            table_query = """
                id SERIAL,
                storage_name VARCHAR(50) NOT NULL,
                db_id INT NOT NULL,
                parent_id INT,
                FOREIGN KEY(db_id) REFERENCES loaded_databases(id) ON DELETE CASCADE,
                FOREIGN KEY(parent_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        self._create_new_table(table_name, table_query)    

    def _create_table_attributes(self):
        """
        Creates a new "attributes" table for the databases.
        """

        table_name = "loaded_attributes"
        if self.DBType == "MariaDB":
            table_query = """
                id INT NOT NULL AUTO_INCREMENT,
                attribute_name VARCHAR(50) NOT NULL,
                datastorage_id INT,
                FOREIGN KEY(datastorage_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        elif self.DBType == "PostgreSQL":
            table_query = """
                id SERIAL,
                attribute_name VARCHAR(50) NOT NULL,
                datastorage_id INT,
                FOREIGN KEY(datastorage_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        self._create_new_table(table_name, table_query)   

    def _create_table_values(self):
        """
        Creates a new "loaded_values" table for the databases.
        """

        table_name = "loaded_values"
        if self.DBType == "MariaDB":
            table_query = """
                id INT NOT NULL AUTO_INCREMENT,
                attribute_id INT,
                entry_no INT NOT NULL,
                value VARCHAR(200) NOT NULL,
                value_type VARCHAR(50) NOT NULL,
                length INT NOT NULL,
                position SMALLINT NOT NULL,
                FOREIGN KEY(attribute_id) REFERENCES loaded_attributes(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        elif self.DBType == "PostgreSQL":
            table_query = """
                id SERIAL,
                attribute_id INT,
                entry_no INT NOT NULL,
                value VARCHAR(200) NOT NULL,
                value_type VARCHAR(50) NOT NULL,
                length INT NOT NULL,
                position SMALLINT NOT NULL,
                FOREIGN KEY(attribute_id) REFERENCES loaded_attributes(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        self._create_new_table(table_name, table_query)

        # Create index
        if self.DBType == "MariaDB":
            query = """
                ALTER TABLE `loaded_values` ADD INDEX IF NOT EXISTS `idx` (`id`, `attribute_id`, `entry_no`, `value_type`, `length`, `value`);
            """
        elif self.DBType == "PostgreSQL":
            query = """
                CREATE INDEX IF NOT EXISTS idx ON loaded_values (id, attribute_id, entry_no, value_type, length, value);
            """            
        self.query_wo_return(query)

    def _create_table_uniqueattributecombinations(self):
        """
        Creates a new "unique_attributecombinations" table for the databases.
        """
        table_name = "unique_attributecombinations"
        if self.DBType == "MariaDB":
            table_query = """
                id INT NOT NULL AUTO_INCREMENT,
                server_id INT NOT NULL,
                server_host VARCHAR(50),
                server_port INT,
                server_type VARCHAR(50),
                db_id INT NOT NULL,
                db_name VARCHAR(50),
                datastorage_id INT NOT NULL,
                datastorage_name VARCHAR(50),
                attribute_ids VARCHAR(50) NOT NULL,
                attribute_names TEXT,
                attribute_types TEXT,
                score_cardinality FLOAT,
                score_valuelenght FLOAT,
                score_position FLOAT,
                score_namesuffix FLOAT,
                score_datatype FLOAT,
                pk_score_hopf TINYINT,
                pk_score_iris TINYINT,
                FOREIGN KEY(server_id) REFERENCES servers(id) ON DELETE CASCADE,
                FOREIGN KEY(db_id) REFERENCES loaded_databases(id) ON DELETE CASCADE,
                FOREIGN KEY(datastorage_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        elif self.DBType == "PostgreSQL":
            table_query = """
                id SERIAL,
                server_id INT NOT NULL,
                server_host VARCHAR(50),
                server_port INT,
                server_type VARCHAR(50),
                db_id INT NOT NULL,
                db_name VARCHAR(50),
                datastorage_id INT NOT NULL,
                datastorage_name VARCHAR(50),
                attribute_ids VARCHAR(50) NOT NULL,
                attribute_names TEXT,
                attribute_types TEXT,
                score_cardinality FLOAT,
                score_valuelenght FLOAT,
                score_position FLOAT,
                score_namesuffix FLOAT,
                score_datatype FLOAT,
                pk_score_hopf SMALLINT,
                pk_score_iris SMALLINT,
                FOREIGN KEY(server_id) REFERENCES servers(id) ON DELETE CASCADE,
                FOREIGN KEY(db_id) REFERENCES loaded_databases(id) ON DELETE CASCADE,
                FOREIGN KEY(datastorage_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        self._create_new_table(table_name, table_query)  

    def _create_table_inclusiondependencies(self):
        """
        Creates a new "inclusion_dependencies" table for the databases.
        """
        table_name = "inclusion_dependencies"
        if self.DBType == "MariaDB":
            table_query = """
                id INT NOT NULL AUTO_INCREMENT,
                UAC_id INT NOT NULL,
                child_server_id INT NOT NULL,
                child_server_host VARCHAR(50),
                child_server_port INT,
                child_server_type VARCHAR(50),
                child_db_id INT NOT NULL,
                child_db_name VARCHAR(50),
                child_datastorage_id INT NOT NULL,
                child_datastorage_name VARCHAR(50),
                child_attribute_ids VARCHAR(50) NOT NULL,
                child_attribute_names TEXT,
                score_datadistribution FLOAT,
                score_syntactic_name_similarity FLOAT,
                score_hybrid_name_similarity FLOAT,
                score_IRIS_name_similarity FLOAT,
                HoPF_probability FLOAT,
                IRIS_probability FLOAT,
                hybrid_only_name_probability FLOAT,
                hybrid_probability FLOAT,
                FOREIGN KEY(UAC_id) REFERENCES unique_attributecombinations(id) ON DELETE CASCADE,
                FOREIGN KEY(child_server_id) REFERENCES servers(id) ON DELETE CASCADE,
                FOREIGN KEY(child_db_id) REFERENCES loaded_databases(id) ON DELETE CASCADE,
                FOREIGN KEY(child_datastorage_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        elif self.DBType == "PostgreSQL":
            table_query = """
                id SERIAL,
                UAC_id INT NOT NULL,
                child_server_id INT NOT NULL,
                child_server_host VARCHAR(50),
                child_server_port INT,
                child_server_type VARCHAR(50),
                child_db_id INT NOT NULL,
                child_db_name VARCHAR(50),
                child_datastorage_id INT NOT NULL,
                child_datastorage_name VARCHAR(50),
                child_attribute_ids VARCHAR(50) NOT NULL,
                child_attribute_names TEXT,
                score_datadistribution FLOAT,
                score_syntactic_name_similarity FLOAT,
                score_hybrid_name_similarity FLOAT,
                score_IRIS_name_similarity FLOAT,
                HoPF_probability FLOAT,
                IRIS_probability FLOAT,
                hybrid_only_name_probability FLOAT,
                hybrid_probability FLOAT,
                FOREIGN KEY(UAC_id) REFERENCES unique_attributecombinations(id) ON DELETE CASCADE,
                FOREIGN KEY(child_server_id) REFERENCES servers(id) ON DELETE CASCADE,
                FOREIGN KEY(child_db_id) REFERENCES loaded_databases(id) ON DELETE CASCADE,
                FOREIGN KEY(child_datastorage_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        self._create_new_table(table_name, table_query)  

    def _create_table_max_inclusiondependencies(self):
        """
        Creates a new "max_inclusion_dependencies" table for the databases.
        """
        table_name = "max_inclusion_dependencies"
        if self.DBType == "MariaDB":
            table_query = """
                id INT NOT NULL AUTO_INCREMENT,
                parent_server_id INT NOT NULL,
                parent_server_host VARCHAR(50),
                parent_server_port INT,
                parent_server_type VARCHAR(50),
                parent_db_id INT NOT NULL,
                parent_db_name VARCHAR(50),
                parent_datastorage_id INT NOT NULL,
                parent_datastorage_name VARCHAR(50),
                parent_attribute_ids VARCHAR(50) NOT NULL,
                parent_attribute_names TEXT,
                child_server_id INT NOT NULL,
                child_server_host VARCHAR(50),
                child_server_port INT,
                child_server_type VARCHAR(50),
                child_db_id INT NOT NULL,
                child_db_name VARCHAR(50),
                child_datastorage_id INT NOT NULL,
                child_datastorage_name VARCHAR(50),
                child_attribute_ids VARCHAR(50) NOT NULL,
                child_attribute_names TEXT,
                FOREIGN KEY(parent_server_id) REFERENCES servers(id) ON DELETE CASCADE,
                FOREIGN KEY(parent_db_id) REFERENCES loaded_databases(id) ON DELETE CASCADE,
                FOREIGN KEY(parent_datastorage_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                FOREIGN KEY(child_server_id) REFERENCES servers(id) ON DELETE CASCADE,
                FOREIGN KEY(child_db_id) REFERENCES loaded_databases(id) ON DELETE CASCADE,
                FOREIGN KEY(child_datastorage_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        elif self.DBType == "PostgreSQL":
            table_query = """
                id SERIAL,
                parent_server_id INT NOT NULL,
                parent_server_host VARCHAR(50),
                parent_server_port INT,
                parent_server_type VARCHAR(50),
                parent_db_id INT NOT NULL,
                parent_db_name VARCHAR(50),
                parent_datastorage_id INT NOT NULL,
                parent_datastorage_name VARCHAR(50),
                parent_attribute_ids VARCHAR(50) NOT NULL,
                parent_attribute_names TEXT,
                child_server_id INT NOT NULL,
                child_server_host VARCHAR(50),
                child_server_port INT,
                child_server_type VARCHAR(50),
                child_db_id INT NOT NULL,
                child_db_name VARCHAR(50),
                child_datastorage_id INT NOT NULL,
                child_datastorage_name VARCHAR(50),
                child_attribute_ids VARCHAR(50) NOT NULL,
                child_attribute_names TEXT,
                FOREIGN KEY(parent_server_id) REFERENCES servers(id) ON DELETE CASCADE,
                FOREIGN KEY(parent_db_id) REFERENCES loaded_databases(id) ON DELETE CASCADE,
                FOREIGN KEY(parent_datastorage_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                FOREIGN KEY(child_server_id) REFERENCES servers(id) ON DELETE CASCADE,
                FOREIGN KEY(child_db_id) REFERENCES loaded_databases(id) ON DELETE CASCADE,
                FOREIGN KEY(child_datastorage_id) REFERENCES datastorage(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        self._create_new_table(table_name, table_query)  

    def _create_table_implicitly_references(self):
        """
        Creates a new "implicitly_references" table for the databases.
        """
        table_name = "implicitly_references"
        if self.DBType == "MariaDB":
            table_query = """
                id INT NOT NULL AUTO_INCREMENT,
                UAC_id INT NOT NULL,
                IND_id INT NOT NULL,
                FOREIGN KEY(UAC_id) REFERENCES unique_attributecombinations(id) ON DELETE CASCADE,
                FOREIGN KEY(IND_id) REFERENCES inclusion_dependencies(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        elif self.DBType == "PostgreSQL":
            table_query = """
                id SERIAL,
                UAC_id INT NOT NULL,
                IND_id INT NOT NULL,
                FOREIGN KEY(UAC_id) REFERENCES unique_attributecombinations(id) ON DELETE CASCADE,
                FOREIGN KEY(IND_id) REFERENCES inclusion_dependencies(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        self._create_new_table(table_name, table_query)

    def _create_table_explicit_references(self):
        """
        Creates a new "explicit_references" table for the databases.
        """
        table_name = "explicit_references"
        if self.DBType == "MariaDB":
            table_query = """
                id INT NOT NULL AUTO_INCREMENT,
                UAC_id INT NOT NULL,
                IND_id INT NOT NULL,
                FOREIGN KEY(UAC_id) REFERENCES unique_attributecombinations(id) ON DELETE CASCADE,
                FOREIGN KEY(IND_id) REFERENCES inclusion_dependencies(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        elif self.DBType == "PostgreSQL":
            table_query = """
                id SERIAL,
                UAC_id INT NOT NULL,
                IND_id INT NOT NULL,
                FOREIGN KEY(UAC_id) REFERENCES unique_attributecombinations(id) ON DELETE CASCADE,
                FOREIGN KEY(IND_id) REFERENCES inclusion_dependencies(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        self._create_new_table(table_name, table_query)

    def _create_table_primarykeys(self):
        """
        Creates a new "primarykeys" table for the databases.
        """
        table_name = "primarykeys"
        if self.DBType == "MariaDB":
            table_query = """
                id INT NOT NULL AUTO_INCREMENT,
                UAC_id INT NOT NULL,
                FOREIGN KEY(UAC_id) REFERENCES unique_attributecombinations(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        elif self.DBType == "PostgreSQL":
            table_query = """
                id SERIAL,
                UAC_id INT NOT NULL,
                FOREIGN KEY(UAC_id) REFERENCES unique_attributecombinations(id) ON DELETE CASCADE,
                PRIMARY KEY (id)
            """
        self._create_new_table(table_name, table_query)

    # Functions to create views
        
    def _create_view_inclusionsdependencies(self):
        """
        Creates a view for the inclusionsdependencies. Used for exports.
        """
        if self.DBType == "MariaDB":
            query = f"""
                CREATE VIEW IF NOT EXISTS view_inclusionsdependencies AS
                SELECT 
                    unique_attributecombinations.id AS UAC_id,
                    inclusion_dependencies.id AS IND_id,
                    unique_attributecombinations.server_type AS parent_type,
                    unique_attributecombinations.server_host AS parent_host,
                    unique_attributecombinations.server_port AS parent_port,
                    unique_attributecombinations.db_name AS parent_database,
                    unique_attributecombinations.datastorage_name AS parent_datastorage,
                    unique_attributecombinations.attribute_names AS parent_attributes,
                    inclusion_dependencies.child_server_type AS child_type,
                    inclusion_dependencies.child_server_host  AS child_host,
                    inclusion_dependencies.child_server_port AS child_port,
                    inclusion_dependencies.child_db_name  AS child_database,
                    inclusion_dependencies.child_datastorage_name AS child_datastorage,
                    inclusion_dependencies.child_attribute_names AS child_attributes,
                    unique_attributecombinations.attribute_types AS attribute_types,
                    unique_attributecombinations.pk_score_hopf AS primarykey_hopf,
                    unique_attributecombinations.pk_score_iris AS primarykey_iris,
                    inclusion_dependencies.score_datadistribution AS score_datadistribution,
                    inclusion_dependencies.HoPF_probability  AS HoPF_probability,
                    inclusion_dependencies.IRIS_probability AS IRIS_probability,
                    inclusion_dependencies.hybrid_only_name_probability AS hybrid_only_name_probability,
                    inclusion_dependencies.hybrid_probability AS hybrid_probability
                FROM inclusion_dependencies
                INNER JOIN unique_attributecombinations ON inclusion_dependencies.UAC_id = unique_attributecombinations.id;
            """
        elif self.DBType == "PostgreSQL":
            query = f"""
                CREATE OR REPLACE VIEW view_inclusionsdependencies AS
                SELECT 
                    unique_attributecombinations.id AS UAC_id,
                    inclusion_dependencies.id AS IND_id,
                    unique_attributecombinations.server_type AS parent_type,
                    unique_attributecombinations.server_host AS parent_host,
                    unique_attributecombinations.server_port AS parent_port,
                    unique_attributecombinations.db_name AS parent_database,
                    unique_attributecombinations.datastorage_name AS parent_datastorage,
                    unique_attributecombinations.attribute_names AS parent_attributes,
                    inclusion_dependencies.child_server_type AS child_type,
                    inclusion_dependencies.child_server_host  AS child_host,
                    inclusion_dependencies.child_server_port AS child_port,
                    inclusion_dependencies.child_db_name  AS child_database,
                    inclusion_dependencies.child_datastorage_name AS child_datastorage,
                    inclusion_dependencies.child_attribute_names AS child_attributes,
                    unique_attributecombinations.attribute_types AS attribute_types,
                    unique_attributecombinations.pk_score_hopf AS primarykey_hopf,
                    unique_attributecombinations.pk_score_iris AS primarykey_iris,
                    inclusion_dependencies.score_datadistribution AS score_datadistribution,
                    inclusion_dependencies.HoPF_probability  AS HoPF_probability,
                    inclusion_dependencies.IRIS_probability AS IRIS_probability,
                    inclusion_dependencies.hybrid_only_name_probability AS hybrid_only_name_probability,
                    inclusion_dependencies.hybrid_probability AS hybrid_probability
                FROM inclusion_dependencies
                INNER JOIN unique_attributecombinations ON inclusion_dependencies.UAC_id = unique_attributecombinations.id;
            """
        self.query_wo_return(query)

    def _create_view_explicite_references(self):
        """
        Creates a view for the explicite references. Used for exports.
        """
        if self.DBType == "MariaDB":
            query = f"""
                CREATE VIEW IF NOT EXISTS view_explicite_references AS
                SELECT 
                    explicit_references.id AS reference_id,
                    explicit_references.UAC_id AS UAC_id,
                    explicit_references.IND_id AS IND_id,

                    unique_attributecombinations.server_type AS primarykey_type,
                    unique_attributecombinations.server_host AS primarykey_host,
                    unique_attributecombinations.server_port AS primarykey_port,
                    unique_attributecombinations.db_name AS primarykey_database,
                    unique_attributecombinations.datastorage_name AS primarykey_datastorage,
                    unique_attributecombinations.attribute_names AS primarykey_attributes,

                    inclusion_dependencies.child_server_type AS foreignkey_type,
                    inclusion_dependencies.child_server_host  AS foreignkey_host,
                    inclusion_dependencies.child_server_port AS foreignkey_port,
                    inclusion_dependencies.child_db_name  AS foreignkey_database,
                    inclusion_dependencies.child_datastorage_name AS foreignkey_datastorage,
                    inclusion_dependencies.child_attribute_names AS foreignkey_attributes,

                    unique_attributecombinations.attribute_types AS datatypes,

                    unique_attributecombinations.pk_score_hopf AS primarykey_hopf,
                    unique_attributecombinations.pk_score_iris AS primarykey_iris,
                    inclusion_dependencies.score_datadistribution AS score_datadistribution,
                    inclusion_dependencies.HoPF_probability  AS HoPF_probability,
                    inclusion_dependencies.IRIS_probability AS IRIS_probability,
                    inclusion_dependencies.hybrid_only_name_probability AS hybrid_only_name_probability,
                    inclusion_dependencies.hybrid_probability AS hybrid_probability
                FROM explicit_references
                INNER JOIN unique_attributecombinations ON explicit_references.UAC_id = unique_attributecombinations.id
                INNER JOIN inclusion_dependencies  ON explicit_references.IND_id = inclusion_dependencies.id
                ;
            """
        elif self.DBType == "PostgreSQL":
            query = f"""
                CREATE OR REPLACE VIEW view_explicite_references AS
                SELECT 
                    explicit_references.id AS reference_id,
                    explicit_references.UAC_id AS UAC_id,
                    explicit_references.IND_id AS IND_id,

                    unique_attributecombinations.server_type AS primarykey_type,
                    unique_attributecombinations.server_host AS primarykey_host,
                    unique_attributecombinations.server_port AS primarykey_port,
                    unique_attributecombinations.db_name AS primarykey_database,
                    unique_attributecombinations.datastorage_name AS primarykey_datastorage,
                    unique_attributecombinations.attribute_names AS primarykey_attributes,

                    inclusion_dependencies.child_server_type AS foreignkey_type,
                    inclusion_dependencies.child_server_host  AS foreignkey_host,
                    inclusion_dependencies.child_server_port AS foreignkey_port,
                    inclusion_dependencies.child_db_name  AS foreignkey_database,
                    inclusion_dependencies.child_datastorage_name AS foreignkey_datastorage,
                    inclusion_dependencies.child_attribute_names AS foreignkey_attributes,

                    unique_attributecombinations.attribute_types AS datatypes,

                    unique_attributecombinations.pk_score_hopf AS primarykey_hopf,
                    unique_attributecombinations.pk_score_iris AS primarykey_iris,
                    inclusion_dependencies.score_datadistribution AS score_datadistribution,
                    inclusion_dependencies.HoPF_probability  AS HoPF_probability,
                    inclusion_dependencies.IRIS_probability AS IRIS_probability,
                    inclusion_dependencies.hybrid_only_name_probability AS hybrid_only_name_probability,
                    inclusion_dependencies.hybrid_probability AS hybrid_probability
                FROM explicit_references
                INNER JOIN unique_attributecombinations ON explicit_references.UAC_id = unique_attributecombinations.id
                INNER JOIN inclusion_dependencies  ON explicit_references.IND_id = inclusion_dependencies.id
                ;
            """
        self.query_wo_return(query)

    def _create_view_implicitly_references(self):
        """
        Creates a view for the implicitly references. Used for exports.
        """
        if self.DBType == "MariaDB":
            query = f"""
                CREATE VIEW IF NOT EXISTS view_implicitly_reference AS
                SELECT 
                    implicitly_references.id AS reference_id,
                    implicitly_references.UAC_id AS UAC_id,
                    implicitly_references.IND_id AS IND_id,

                    unique_attributecombinations.server_type AS primarykey_type,
                    unique_attributecombinations.server_host AS primarykey_host,
                    unique_attributecombinations.server_port AS primarykey_port,
                    unique_attributecombinations.db_name AS primarykey_database,
                    unique_attributecombinations.datastorage_name AS primarykey_datastorage,
                    unique_attributecombinations.attribute_names AS primarykey_attributes,

                    inclusion_dependencies.child_server_type AS foreignkey_type,
                    inclusion_dependencies.child_server_host  AS foreignkey_host,
                    inclusion_dependencies.child_server_port AS foreignkey_port,
                    inclusion_dependencies.child_db_name  AS foreignkey_database,
                    inclusion_dependencies.child_datastorage_name AS foreignkey_datastorage,
                    inclusion_dependencies.child_attribute_names AS foreignkey_attributes,

                    unique_attributecombinations.attribute_types AS datatypes,

                    unique_attributecombinations.pk_score_hopf AS primarykey_hopf,
                    unique_attributecombinations.pk_score_iris AS primarykey_iris,
                    inclusion_dependencies.score_datadistribution AS score_datadistribution,
                    inclusion_dependencies.HoPF_probability  AS HoPF_probability,
                    inclusion_dependencies.IRIS_probability AS IRIS_probability,
                    inclusion_dependencies.hybrid_only_name_probability AS hybrid_only_name_probability,
                    inclusion_dependencies.hybrid_probability AS hybrid_probability
                FROM implicitly_references
                INNER JOIN unique_attributecombinations ON implicitly_references.UAC_id = unique_attributecombinations.id
                INNER JOIN inclusion_dependencies  ON implicitly_references.IND_id = inclusion_dependencies.id
                ;
            """
        elif self.DBType == "PostgreSQL":
            query = f"""
                CREATE OR REPLACE VIEW view_implicitly_reference AS
                SELECT 
                    implicitly_references.id AS reference_id,
                    implicitly_references.UAC_id AS UAC_id,
                    implicitly_references.IND_id AS IND_id,

                    unique_attributecombinations.server_type AS primarykey_type,
                    unique_attributecombinations.server_host AS primarykey_host,
                    unique_attributecombinations.server_port AS primarykey_port,
                    unique_attributecombinations.db_name AS primarykey_database,
                    unique_attributecombinations.datastorage_name AS primarykey_datastorage,
                    unique_attributecombinations.attribute_names AS primarykey_attributes,

                    inclusion_dependencies.child_server_type AS foreignkey_type,
                    inclusion_dependencies.child_server_host  AS foreignkey_host,
                    inclusion_dependencies.child_server_port AS foreignkey_port,
                    inclusion_dependencies.child_db_name  AS foreignkey_database,
                    inclusion_dependencies.child_datastorage_name AS foreignkey_datastorage,
                    inclusion_dependencies.child_attribute_names AS foreignkey_attributes,

                    unique_attributecombinations.attribute_types AS datatypes,

                    unique_attributecombinations.pk_score_hopf AS primarykey_hopf,
                    unique_attributecombinations.pk_score_iris AS primarykey_iris,
                    inclusion_dependencies.score_datadistribution AS score_datadistribution,
                    inclusion_dependencies.HoPF_probability  AS HoPF_probability,
                    inclusion_dependencies.IRIS_probability AS IRIS_probability,
                    inclusion_dependencies.hybrid_only_name_probability AS hybrid_only_name_probability,
                    inclusion_dependencies.hybrid_probability AS hybrid_probability
                FROM implicitly_references
                INNER JOIN unique_attributecombinations ON implicitly_references.UAC_id = unique_attributecombinations.id
                INNER JOIN inclusion_dependencies  ON implicitly_references.IND_id = inclusion_dependencies.id
                ;
            """
        self.query_wo_return(query)

    def _create_view_primarykeys(self):
        """
        Creates a view for the primarykeyss. Used for exports.
        """
        if self.DBType == "MariaDB":
            query = f"""
                CREATE VIEW IF NOT EXISTS view_primarykeys AS
                SELECT 
                    primarykeys.id AS primarykey_id,
                    primarykeys.UAC_id AS UAC_id,

                    unique_attributecombinations.server_type AS primarykey_type,
                    unique_attributecombinations.server_host AS primarykey_host,
                    unique_attributecombinations.server_port AS primarykey_port,
                    unique_attributecombinations.db_name AS primarykey_database,
                    unique_attributecombinations.datastorage_name AS primarykey_datastorage,
                    unique_attributecombinations.attribute_names AS primarykey_attributes,

                    unique_attributecombinations.attribute_types AS datatypes,

                    unique_attributecombinations.pk_score_hopf AS primarykey_hopf,
                    unique_attributecombinations.pk_score_iris AS primarykey_iris
                FROM primarykeys 
                INNER JOIN unique_attributecombinations ON primarykeys.UAC_id = unique_attributecombinations.id
                ;
            """
        elif self.DBType == "PostgreSQL":
            query = f"""
                CREATE OR REPLACE VIEW view_primarykeys AS
                SELECT 
                    primarykeys.id AS primarykey_id,
                    primarykeys.UAC_id AS UAC_id,

                    unique_attributecombinations.server_type AS primarykey_type,
                    unique_attributecombinations.server_host AS primarykey_host,
                    unique_attributecombinations.server_port AS primarykey_port,
                    unique_attributecombinations.db_name AS primarykey_database,
                    unique_attributecombinations.datastorage_name AS primarykey_datastorage,
                    unique_attributecombinations.attribute_names AS primarykey_attributes,

                    unique_attributecombinations.attribute_types AS datatypes,

                    unique_attributecombinations.pk_score_hopf AS primarykey_hopf,
                    unique_attributecombinations.pk_score_iris AS primarykey_iris
                FROM primarykeys 
                INNER JOIN unique_attributecombinations ON primarykeys.UAC_id = unique_attributecombinations.id
                ;
            """
        self.query_wo_return(query)
