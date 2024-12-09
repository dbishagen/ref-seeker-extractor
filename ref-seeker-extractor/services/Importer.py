from pymongo import MongoClient
from neo4j import GraphDatabase
from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pymongo.errors import ConnectionFailure
import psycopg2
from decimal import Decimal
from urllib.parse import urlparse


class Importer:
    """
    A class for importing database configurations and data into a system via a SQL connector.
    """
        
    def __init__(self, connector, server_type, host, port):
        """
        Initializes an Importer instance which manages the importation of databases and their components.

        Args:
            connector (DBConnector): An instance used for database connections.
            server_type (str): Type of the server (e.g., 'MySQL', 'PostgreSQL').
            host (str): Hostname or IP address of the server.
            port (int): Network port on which the server listens.
        """
        self.connector = connector
        self.server_type = server_type
        self.host = host
        self.port = port
        self.server_id = self._add_server(server_type, host, port)
        # Saves metadata for the attributes
        self.info = {}

    def _add_server(self, server_type, host, port):
        """
        Registers a new server with the connected system.

        Args:
            server_type (str): Type of the server.
            host (str): Hostname or IP address.
            port (int): Port number.

        Returns:
            int: The unique ID of the newly added server.
        """
        return self.connector.add_server(server_type, host, port)
    
    def _add_database(self, database_name):
        """
        Adds a new database to the managed server.

        Args:
            database_name (str): The name of the database to add.

        Returns:
            int: The unique ID of the newly added database.
        """
        database_id = self.connector.add_database(database_name, self.server_id)
        self.info[database_name] = {"database_id": database_id, "datastorages": {}}
        return database_id

    def _add_datastorage(self, storage_name, database_name):
        """
        Adds a new data storage unit (such as a table or collection) under a specified database.

        Args:
            storage_name (str): The name of the data storage unit to add.
            database_name (str): The name of the database under which the data storage is added.

        Returns:
            int: The unique ID of the newly added data storage unit.
        """
        database_id = self.info[database_name]["database_id"]
        datastorage_id = self.connector.add_datastorage(storage_name, database_id)
        self.info[database_name]["datastorages"][storage_name] = {"datastorage_id": datastorage_id, "attributes": {}}
        return datastorage_id

    def _add_entry(self, database, datastorage, attribute_name, entry_number, value, value_type, position):
        """
        Adds an entry to a specified attribute in a database and data storage.

        Args:
            database (str): The name of the database.
            datastorage (str): The name of the datastorage.
            attribute_name (str): The name of the attribute.
            entry_number (int): The sequence number of the entry.
            value: The actual data value.
            value_type (str): The data type of the value.
            position (int): The position or index of the entry in the data storage.
        """
        if database in self.info:
            if datastorage in self.info[database]["datastorages"]:
                if attribute_name in self.info[database]["datastorages"][datastorage]["attributes"]:
                    attribute_id = self.info[database]["datastorages"][datastorage]["attributes"][attribute_name]
                else:
                    self.info[database]["datastorages"][datastorage]["datastorage_id"]
                    attribute_id = self._add_attribute(attribute_name, datastorage, database)
            else:
                self._add_datastorage(datastorage, database)
                attribute_id = self._add_attribute(attribute_name, datastorage, database)
        else:
            self._add_database(database)
            self._add_datastorage(datastorage, database)
            attribute_id = self._add_attribute(attribute_name, datastorage, database)

        value_string = str(value)
        value_lenght = len(value_string)
        # Check value length -> The value can only have 200 Chars
        if value_lenght > 200: 
            value_string = "longString"   
        self.connector.add_value_batchimport(attribute_id, entry_number, value_string, value_type, value_lenght, position)

    def _end_bachtimport(self):
        """
        Finalizes the batch import process by signaling the end of data importation.
        """
        self.connector.add_value_batchimport_end()

    def _add_attribute(self, attribut_name, datastorage_name, database_name):
        """
        Adds a new attribute to a specified data storage unit.

        Args:
            attribute_name (str): The name of the attribute to add.
            datastorage_name (str): The name of the data storage unit.
            database_name (str): The name of the database.

        Returns:
            int: The unique ID of the newly added attribute.
        """
        datastorage_id = self.info[database_name]["datastorages"][datastorage_name]["datastorage_id"]
        attribute_id = self.connector.add_attribute(attribut_name, datastorage_id)
        self.info[database_name]["datastorages"][datastorage_name]["attributes"][attribut_name] = attribute_id
        return attribute_id

    def get_datastorage_id(self, database_name, datastorage_name):
        """
        Retrieves the ID of a specific data storage unit within a database.

        Args:
            database_name (str): The name of the database.
            datastorage_name (str): The name of the data storage.

        Returns:
            int: The ID of the specified data storage unit.
        """
        datastorage_id = self.info[database_name]["datastorages"][datastorage_name]["datastorage_id"]
        return datastorage_id
    
    def get_database_id(self, database_name):
        """
        Retrieves the ID of a specific database.

        Args:
            database_name (str): The name of the database.

        Returns:
            int: The ID of the specified database.
        """
        database_id = self.info[database_name]["database_id"]
        return database_id
    
    def load_all(self):
        """
        Placeholder for a function to load the database.
        """
        pass

    def close_connection(self):
        """
        Placeholder for a function to close the connection to the database.
        """
        pass

class ImporterCassandra(Importer):
    """
    Class for importing data from Apache Cassandra databases.
    This class extends the generic Importer to handle connections and data importing specific to Apache Cassandra.
    """

    def __init__(self, connector, list_uris, username=None, password=None):
        """
        Initializes a connection to an Apache Cassandra database and sets up the environment for data import.

        Args:
            connector (SQLConnector): An instance of SQLConnector used for database connections.
            list_uris (list[str]): List of URIs containing credentials and connection details.
            username (Optional[str]): Username for Cassandra authentication.
            password (Optional[str]): Password for Cassandra authentication.

        Raises:
            ConnectionFailure: If the connection to the Cassandra cluster fails.
        """
        list_hosts = []
        for uri in list_uris:
            parsed_uri = urlparse(uri)
            host = parsed_uri.hostname
            list_hosts.append(host)
            port = parsed_uri.port
            self.keyspace = parsed_uri.path[1:]        

        str_host = ', '.join(list_hosts)
        super().__init__(connector, "Cassandra", str_host, port)   
        if username is None:
            self.cluster = Cluster(contact_points=list_hosts, port=port)
            self.cluster.connection_class = LibevConnection
            self.session = self.cluster.connect(self.keyspace)
        else:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            self.cluster = Cluster(contact_points=list_hosts, port=port, auth_provider=auth_provider)
            self.cluster.connection_class = LibevConnection
            self.session = self.cluster.connect(self.keyspace)

        self.load_all()
        self.close_connection()

    def close_connection(self):
        """
        Shuts down the connection to the Cassandra cluster.
        """
        self.cluster.shutdown()

    def load_all(self):
        """
        Retrieves and loads all data from each table within the specified keyspace.
        Iterates over each row in each table, processing and storing data as configured.
        """
        tables_names = self._get_tables()
        for table_name in tables_names:
            result = self.session.execute(f"SELECT * FROM {table_name}")
            entry_no = 1
            for row in result:
                position = 1
                for attribute_name, value_entry in row._asdict().items():
                    value_type = type(value_entry).__name__
                    self._add_entry(self.keyspace, table_name, attribute_name, entry_no, value_entry, value_type, position)
                    position += 1
                entry_no += 1
        self._end_bachtimport()

    def _get_tables(self):
        """
        Retrieves a list of table names from the specified keyspace.

        Returns:
            list[str]: A list of table names within the keyspace.
        """
        tables = self.session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{self.keyspace}'")
        list_tables = []
        for table_row in tables:
            table_name = table_row.table_name
            list_tables.append(table_name)
        return list_tables

class ImporterPostgreSQL(Importer):
    """
    A class for importing data from PostgreSQL databases.
    This class extends the generic Importer to handle connections and data importing specific to PostgreSQL.
    """

    def __init__(self, connector, uri, user, password):
        """
        Initializes a connection to a PostgreSQL database and verifies the connection by querying the database version.

        Args:
            connector (SQLConnector): An instance of SQLConnector used for database connections.
            uri (str): Complete URI containing credentials and connection details.
            user (str): Username for the database authentication.
            password (str): Password for the database authentication.

        Raises:
            ConnectionFailure: If the connection fails.
        """
        parsed_uri = urlparse(uri)
        self.host = parsed_uri.hostname
        self.port = parsed_uri.port
        self.database = parsed_uri.path[1:]

        super().__init__(connector, "PostgreSQL", self.host, self.port)

        try:
            self.conn = psycopg2.connect(
                        database=self.database,
                        host=self.host,
                        user=user,
                        password=password,
                        port=self.port)
            
            cursor = self.conn.cursor()
            cursor.execute("SELECT version();")
            record = cursor.fetchone()
        except (Exception, psycopg2.Error) as error:
            print("Cant connect to PostgreSQL-Datenbank:", error)

        finally:
            # Verbindung schließen
            if self.conn:
                cursor.close()

        self.load_all()
        self.close_connection()

    def close_connection(self):
        """
        Closes the connection to the PostgreSQL database.
        """
        self.conn.close()   

    def load_all(self):
        """
        Retrieves all data from each table within the database schema, except system tables, and loads it into the system.
        """
        # tables = self._get_tables()
        cursor = self.conn.cursor()

        # SQL Retrieve schema names
        query = "SELECT schema_name FROM information_schema.schemata;"
        cursor.execute(query)
        schema_rows = cursor.fetchall()
        schema_names = [row[0] for row in schema_rows]
        schema_names.remove("information_schema")
        schema_names.remove("pg_catalog")
        schema_names.remove("pg_toast")
        for schema in schema_names:
            tables = []
            # SQL query to retrieve all tables
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
            for table_name in tables:
                # Get attribute names from the information schema
                query = """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position;
                """
                cursor.execute(query, (table_name,))
                result = cursor.fetchall()
                attribute_names = [row[0] for row in result]
                position = 1
                for attribute_name in attribute_names:
                    # SQL query to select all data from the specified table and attribute
                    query = f"SELECT \"{attribute_name}\" FROM {schema}.{table_name};"
                    cursor.execute(query)
                    entry_no = 1
                    for value in cursor.fetchall():
                        value = value[0]
                        if isinstance(value, Decimal):
                            # Cast decimal to float
                            value = float(value)
                        if value is None: continue # Skips empty values
                        value_type = type(value).__name__
                        if value_type == "list":
                            # Entry is array
                            for value_entry in value:
                                if isinstance(value, Decimal):
                                    # Cast decimal to float
                                    value = float(value)
                                value_type = type(value_entry).__name__
                                self._add_entry(self.database, table_name, attribute_name, entry_no, value_entry, value_type, position)
                        else:
                            self._add_entry(self.database, table_name, attribute_name, entry_no, value, value_type, position)
                        entry_no += 1
                    position += 1
        self._end_bachtimport()
        cursor.close()

    def _get_tables(self):
        """
        Retrieves the list of all tables in the connected PostgreSQL database that are not system tables.

        Returns:
            list[str]: A list of table names.
        """
        tables = []

        try:
            # Establish connection to the PostgreSQL database
            cursor = self.conn.cursor()

            # SQL Retrieve schema names
            query = "SELECT schema_name FROM information_schema.schemata;"
            cursor.execute(query)
            schema_rows = cursor.fetchall()
            schema_names = [row[0] for row in schema_rows]
            schema_names.remove("information_schema")
            schema_names.remove("pg_catalog")
            schema_names.remove("pg_toast")
            for schema in schema_names:
                # SQL query to retrieve all tables
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

        except (Exception, psycopg2.Error) as error:
            print("Error retrieving tables:", error)

        finally:
            # Close connection
            if self.conn:
                cursor.close()

        return tables

class ImporterNeo4j(Importer):
    """
    Class to import data from neo4j. This includes both nodes and relationships.
    This class extends the generic Importer to handle connections and data importing specific to neo4j.
    """
    def __init__(self, connector, uri, user, password):
        """
        Initializes an ImporterNeo4j object to manage the connection to a Neo4j database.

        Args:
            connector (SQLConnector): An instance of SQLConnector used for database connections.
            uri (str): Complete URI containing credentials and connection details.
            user (str): Username for database authentication.
            password (str): Password for database authentication.
        """
        parsed_uri = urlparse(uri)
        self.host = parsed_uri.hostname
        self.port = parsed_uri.port
        self.uri = uri
        self.database = "neo4j" # Needed for DBConnector
        super().__init__(connector, "neo4j", self.host, self.port)

        self.driver = GraphDatabase.driver(self.uri, auth=(user, password))

        self.load_all()

    def load_all(self):
        """
        Loads all nodes and relationships from the Neo4j database.
        """
        self._load_nodes()
        self._load_relationships()

    def _load_nodes(self):
        """
        Retrieves all nodes from the database and processes them for import.
        """
        # Get labels
        session = self.driver.session()
        result = session.run("CALL db.labels()")
        labels = [record["label"] for record in result]

        for label in labels:
            # Get nodes
            query = f"""MATCH (n:{label}) RETURN 
                elementid(n) AS elementId,
                properties(n) AS properties"""
            records, summary, keys = self.driver.execute_query(query)
            # Retrieve values
            entry_no = 1
            for entry in records:
                elementId = entry["elementId"]
                properties = entry["properties"]
                datastorage_name = label
                self._add_entry(self.database, datastorage_name, "elementId", entry_no, elementId, "elementId", 1)
                position = 2
                properties = entry["properties"]
                for property in properties:
                    attribute_name = property
                    value = properties[attribute_name]
                    if isinstance(value, list):
                        # Attribute is an array
                        for value_entry in value:
                            value_type = type(value_entry).__name__
                            self._add_entry(self.database, datastorage_name, attribute_name, entry_no, value_entry, value_type, position)
                    else:
                        value_type = type(value).__name__
                        self._add_entry(self.database, datastorage_name, attribute_name, entry_no, value, value_type, position)
                    position += 1
                entry_no += 1        
            self._end_bachtimport()

    def _load_relationships(self):
        """
        Retrieves all relationships from the database and processes them for import.
        """
        # Get relation types
        session = self.driver.session()
        result = session.run("CALL db.relationshipTypes()")
        types = [record["relationshipType"] for record in result]
        for relation_type in types:
            # Get relations
            query = f""" MATCH ()-[r:{relation_type}]->() RETURN
                elementid(r) AS elementId, 
                elementId(startNode(r)) AS startNodeElementId, 
                elementId(endNode(r)) AS endNodeElementId, 
                properties(r) AS properties"""
            records, summary, keys = self.driver.execute_query(query)
            # Retrieve values
            entry_no = 1
            for entry in records:
                datastorage_name = relation_type
                elementId = entry["elementId"]
                self._add_entry(self.database, datastorage_name, "elementId", entry_no, elementId, "elementId", 1)
                startNodeElementId = entry["startNodeElementId"]
                self._add_entry(self.database, datastorage_name, "startNodeElementId", entry_no, startNodeElementId, "elementId", 2)
                endNodeElementId = entry["endNodeElementId"]
                self._add_entry(self.database, datastorage_name, "endNodeElementId", entry_no, endNodeElementId, "elementId", 3)
                position = 4
                properties = entry["properties"]
                for property in properties:
                    attribute_name = property
                    value = properties[attribute_name]
                    if isinstance(value, list):
                        # Attribute is an array
                        for value_entry in value:
                            value_type = type(value_entry).__name__
                            self._add_entry(self.database, datastorage_name, attribute_name, entry_no, value_entry, value_type, position)
                    else:
                        value_type = type(value).__name__
                        self._add_entry(self.database, datastorage_name, attribute_name, entry_no, value, value_type, position)
                    position += 1
                entry_no += 1
            self._end_bachtimport()

class ImporterMongoDB(Importer):
    """
    Class for import of MongoDB-databases. It is used to get the data for the analyzes from the MongoDB.
    This class extends the generic Importer to handle connections and data importing specific to MongoDB.
    """
    def __init__(self, connector, uri, user, password):
        """
        Initializes a connection to a MongoDB database using a URI and tests the connection. The URI is used because it's easier to use
        for different MangoDB versions.

        Args:
            connector (SQLConnector): An instance of SQLConnector used for database connections.
            uri (str): Complete MongoDB URI containing credentials and connection details.
            user (str): Username for database authentication.
            password (str): Password for database authentication.

        Raises:
            ConnectionFailure: If the connection to MongoDB cannot be established.
        """

        try:
            if not user:
                self.client = MongoClient(uri)
            else:
                self.client = MongoClient(uri, username=user, password=password)
            self.db = self.client.get_database()

            # Get host, port and databasename from uri
            parsed_uri = urlparse(uri)
            self.host = parsed_uri.hostname
            self.port = parsed_uri.port
            self.database = self.db.name

        except ConnectionFailure as e:
            print(f"Error connecting to MongoDB: {e}")
            raise

        super().__init__(connector, "MongoDB", self.host, self.port)

        self.importerEmbeddedObject = []

        self.load_all()
        self.close_connection()

    def close_connection(self):
        """
        Closes the MongoDB client connection.
        """
        self.client.close()
    
    def get_collections(self):
        """
        Retrieves a list of all collections within a specified database.

        Returns:
            list: A list of collection names.
        """
        collections = self.db.list_collection_names()
        return collections

    def load_all(self):
        """
        Loads all documents from each collection in the specified database and processes each field.
        """
        list_collections = self.get_collections()
        for collection in list_collections:
            entry_number = 1
            cursor = self.db[collection].find({})
            for document in cursor:
                position = 1
                for property in document:
                    value = document[property]
                    self._load_entry(self.database, collection, property, value, entry_number, position)
                    position += 1
                entry_number += 1
        self._end_bachtimport()
            
    def _load_entry(self, database_name, collection_name, property_name, value, entry_number, position):
        """
        Handles the loading of each entry based on its data type. Supports complex data types by recursive processing.

        Args:
            database_name (str): Name of the database.
            collection_name (str): Name of the collection.
            property_name (str): The field name in the document.
            value (variable): The value associated with the field.
            entry_number (int): Document number in the sequence.
            position (int): The position of the field within the document.
        """
        value_type = type(value).__name__ 
        # Convert type of property to string. Source: https://stackoverflow.com/questions/5008828/convert-a-python-type-object-to-a-string
        if value_type == 'list':
            # MongoDB-type: Array
            for entry in value:
                self._load_entry(database_name, collection_name, property_name, entry, entry_number, position)
        elif value_type == 'dict':
            # MongoDB-type: Object
            target_entry = {
                "database_name":database_name, 
                "collection_name":collection_name, 
                "ebbededObject_name":property_name
            }
            # Checks if entry exist
            matching_entry_index = next(
                (index for index, entry in enumerate(self.importerEmbeddedObject) 
                if all(entry[key] == value for key, value in target_entry.items())), None
            )
            if matching_entry_index is not None:
                matching_entry = self.importerEmbeddedObject[matching_entry_index]
                importer = matching_entry.get("importer")
            else:
                # Adds new entry
                database_id = self.get_database_id(database_name)
                datastorage_id = self.get_datastorage_id(database_name, collection_name)
                importer = ImporterEmbeddedObject(database_id, datastorage_id, property_name, self.connector)
                new_entry = {
                    "database_name":database_name, 
                    "collection_name":collection_name, 
                    "ebbededObject_name":property_name,
                    "importer":importer
                }
                self.importerEmbeddedObject.append(new_entry)
            importer.add_embeddedObject(value)
        elif value_type == 'DBRef':
            # MongoDB-type: DBRef
            # Source: https://pymongo.readthedocs.io/en/stable/api/bson/dbref.html   
            value = value.id
            self._add_entry(database_name, collection_name, property_name, entry_number, value, value_type, position)
        elif value_type == 'ObjectId':
            # MongoDB-type: ObjectId
            self._add_entry(database_name, collection_name, property_name, entry_number, value, value_type, position)
        else:
            # MongoDB-type: Integer, Float, Symbol, String, Boolean, Date, Binary   
            self._add_entry(database_name, collection_name, property_name, entry_number, value, value_type, position)

class ImporterEmbeddedObject():
    """
    Manages the import of embedded objects from a database into a structured format, handling nested data and attributes.
    """

    def __init__(self, database_id, datastorage_id, embeddedObject_name, connector):
        """
        Initializes an ImporterEmbeddedObject instance for managing the import of nested data structures.

        Args:
            database_id (int): The ID of the database where the data resides.
            datastorage_id (int): The ID of the parent data storage.
            embedded_object_name (str): The name of the embedded object to manage.
            connector (SQLConnector): An instance used for executing database operations.
        """
        self.database_id = database_id
        self.parent_datastorage_id = datastorage_id
        self.name = embeddedObject_name
        self.connector = connector #MariaDBConnector
        self.entry_number = 1 # Number of the document
        self.importerEmbeddedObject = []
        self.attributes = {}
        self.datastorage_id = self.connector.add_datastorage(self.name, database_id, self.parent_datastorage_id)

    def add_embeddedObject(self, value):
        """
        Adds a new embedded object to the database by processing its attributes.

        Args:
            value (dict): The dictionary representing the embedded object with its properties.
        """
        position = 1
        for entry in value:
            attribute_name = entry
            self._load_entry(attribute_name, value[entry], position)
            position += 1
        self.entry_number += 1

    def _load_entry(self, property_name, value, position):
        """
        Processes and loads an individual entry from the embedded object into the database.

        Args:
            property_name (str): The name of the property in the object.
            value (variable): The value of the property, can be of any type supported by MongoDB.
            position (int): The ordinal position of the property in the embedded object.
        """
        value_type = type(value).__name__ 
        # Convert type of property to string. Source: https://stackoverflow.com/questions/5008828/convert-a-python-type-object-to-a-string
        if value_type == 'list':
            # MongoDB-type: Array
            for entry in value:
                self._load_entry(property_name, entry, position)
        elif value_type == 'dict':
            # MongoDB-type: Object
            #TODO: Explizite Referenz
            target_entry = {
                "ebbededObject_name":property_name
            }
            # Checks if entry exist
            matching_entry_index = next(
                (index for index, entry in enumerate(self.importerEmbeddedObject) 
                if all(entry[key] == value for key, value in target_entry.items())), None
            )
            if matching_entry_index is not None:
                matching_entry = self.importerEmbeddedObject[matching_entry_index]
                importer = matching_entry.get("importer")
            else:
                # Adds new entry
                importer = ImporterEmbeddedObject(self.database_id, self.datastorage_id, property_name, self.connector)
                new_entry = {
                    "ebbededObject_name":property_name,
                    "importer":importer
                }
                self.importerEmbeddedObject.append(new_entry)
            importer.add_embeddedObject(value)
        elif value_type == 'DBRef':
            # MongoDB-type: DBRef
            # Source: https://pymongo.readthedocs.io/en/stable/api/bson/dbref.html   
            pass
        elif value_type == 'ObjectId':
            # MongoDB-type: ObjectId
            self._add_entry(property_name, value, value_type, position)
        else:
            # MongoDB-type: Integer, Float, Symbol, String, Boolean, Date, Binary   
            self._add_entry(property_name, value, value_type, position)

    def _add_entry(self, attribute_name, value, value_type, position):
        """
        Adds an entry to the import queue after ensuring the attribute is registered in the database.

        Args:
            attribute_name (str): The name of the attribute.
            value (variable): The value of the attribute.
            value_type (str): The data type of the value.
            position (int): The position of the attribute within the object.
        """
        if attribute_name in self.attributes:
            attribute_id = self.attributes[attribute_name]
        else:
            attribute_id = self._add_attribute(attribute_name)
        value_string = str(value)
        value_lenght = len(value_string)
        # Check value length -> The value can only have 200 Chars
        if value_lenght > 200: 
            value_string = "longString"
        self.connector.add_value(attribute_id, self.entry_number, value_string, value_type, value_lenght, position) 

    def _add_attribute(self, attribut_name):
        """
        Registers a new attribute within the data storage if not already present and returns its ID.

        Args:
            attribute_name (str): The name of the attribute to add.

        Returns:
            int: The ID of the newly added attribute in the database.
        """
        attribute_id = self.connector.add_attribute(attribut_name, self.datastorage_id)
        self.attributes[attribut_name] = attribute_id
        return attribute_id