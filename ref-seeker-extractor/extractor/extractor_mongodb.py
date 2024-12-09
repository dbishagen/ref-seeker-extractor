from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

class ExtractorMongodb:
    def __init__(self, primarykey_writer, foreignkey_writer, host, port, username, password, database):
        self.primarykey_writer = primarykey_writer
        self.foreignkey_writer = foreignkey_writer

        self.type = "MongoDB"
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

        uri = f"mongodb://{username}:{password}@{host}:{port}/?directConnection=true"
        try:
            # Test the connection
            self.client = MongoClient(uri)
            self.client.server_info()
        except ConnectionFailure as e:
            print(f"Error connecting to MongoDB: {e}")
            raise

    def extract(self):
        # Connect ot database
        list_collections = self.get_collections(self.database)
        for collection in list_collections:
            cursor = self.client[self.database][collection].find({})
            self.primarykey_writer.add_primarykey(self.type, self.host, self.port, self.database, collection, "_id")
            dbref_list =[]
            for document in cursor:
                for property in document:
                    value = document[property]
                    value_type = type(value).__name__
                    if value_type == 'DBRef':
                        # MongoDB-type: DBRef
                        # Source: https://pymongo.readthedocs.io/en/stable/api/bson/dbref.html   
                        primary_database = value.database
                        if primary_database is None:
                            primary_database = self.database
                        primary_collection = value.collection
                        dic ={
                            "primary_database":primary_database,
                            "primary_collection":primary_collection
                        }
                        check = self.is_dict_in_list(dic, dbref_list)
                        if not check:
                            dbref_list.append(dic)
                            self.foreignkey_writer.add_forgeinkey(
                                self.type, self.host, self.port, primary_database, primary_collection, "_id",
                                self.type, self.host, self.port, self.database, collection, property
                            )

        self.close_connection()
        self.primarykey_writer.write()
        self.foreignkey_writer.write()

    def get_collections(self, database_name):
        """
        Gets list of collections in database.

        Args:
            database_name (str): Name of the database.

        Returns:
            list: List of strings, with collection names.
        """
        list = self.client[database_name].list_collection_names()
        return list
    
    def close_connection(self):
        """Closes the connection to MongoDB."""
        self.client.close()

    def is_dict_in_list(self, dictionary, dictionary_list):
        """
        Überprüft, ob ein Dictionary bereits in einer Liste von Dictionaries vorhanden ist.

        Args:
            dictionary (dict): Das zu überprüfende Dictionary.
            dictionary_list (list): Die Liste von Dictionaries.

        Returns:
            bool: True, wenn das Dictionary bereits in der Liste vorhanden ist, sonst False.
        """
        for item in dictionary_list:
            if item == dictionary:
                return True
        return False