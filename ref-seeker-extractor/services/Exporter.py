import json

class Exporter:
    """
    A class designed to export results from a database to a JSON file.
    It gathers various database details and outputs them to a structured JSON format.
    """


    def __init__(self, connector, find_max_ind, file_path, runtime_metrics=None):
        """
        Initializes the Exporter with a database connector and a file path for the output.

        Args:
            connector (DBConnector): The database connector instance used to access database information.
            find_max_ind (bool): If true the prototyp will search for all maximal inlusion dependencies.
            file_path (str): The file path where the JSON file will be saved.
        """      
        self.connector = connector
        self.file_path = file_path
        self.results = {}
        if runtime_metrics is not None:
            self.results["runtime_metrics"] = runtime_metrics
        self.start_export(find_max_ind)

    def start_export(self, find_max_ind):
        """
        Executes the export process by gathering database information and writing it to a JSON file.
        The method organizes data into a dictionary and then serializes it into JSON format, saving it to the specified file path.

        Args:
            find_max_ind (bool): If true the prototyp will search for all maximal inlusion dependencies.
        """
        self._append_implicite_references()
        self._append_explicite_refences()
        self._append_primarykeys()
        self._append_schema()
        if find_max_ind:
            self._append_max_inds()

        # Export to JSON
        json_string = json.dumps(self.results, indent=4)
        with open(self.file_path, "w") as f:
            f.write(json_string)

    def _append_max_inds(self):
        """
        Gathers and appends maximal inculsion dependencies details from the database to the results dictionary.
        """
        max_inds = self.connector.get_maximal_inclusion_dependencies()
        self.results["maximal_inclusion_dependencies"] = max_inds

    def _append_schema(self):
        """
        Gathers schema information from the database and appends it to the results dictionary.
        It includes server types, databases, data storages, and attribute details within each storage.
        """
        databases = []
        server_ids = self.connector.get_servers()
        for server_id in server_ids:
            server_type = self.connector.get_server_type(server_id)
            database_ids = self.connector.get_databases(server_id)
            for database_id in database_ids:
                database_name = self.connector.get_database_name(database_id)
                datastorages = []
                datastorage_ids = self.connector.get_datastorages(database_id)
                for datastorage_id in datastorage_ids:
                    datastorage_name = self.connector.get_datastorage_name(datastorage_id)
                    datastorage_embedded_in = self.connector.get_datastorage_embedded_in(datastorage_id)
                    attributes = []
                    attribute_ids = self.connector.get_attributes(datastorage_id)
                    for attribute_id in attribute_ids:
                        attribute_name = self.connector.get_attribute_name(attribute_id)
                        attribute_types = self.connector.get_attribute_types(attribute_id)
                        number_of_entries = self.connector.get_number_of_entries(attribute_id)
                        is_array = self.connector.check_if_attribute_contains_array(attribute_id)
                        dic = {
                            "attribute_name": attribute_name,
                            "attribute_types": attribute_types,
                            "number_of_entries": number_of_entries,
                            "is_array": is_array
                        }
                        attributes.append(dic)
                    dic = {
                        "datastorage_name": datastorage_name,
                        "datastorage_embedded_in": datastorage_embedded_in,
                        "attributes": attributes
                    }
                    datastorages.append(dic)
                dic = {
                        "database_name": database_name,
                        "database_type": server_type,
                        "datastorages": datastorages
                       }
                databases.append(dic)
        self.results["databases"] = databases

    def _append_implicite_references(self):
        """
        Gathers and appends implicit reference details from the database to the results dictionary.
        """
        implicite_refences = self.connector.get_implicite_references()
        self.results["implicite_refences"] = implicite_refences

    def _append_explicite_refences(self):
        """
        Gathers and appends explicit reference details from the database to the results dictionary.
        """
        explicite_refences = self.connector.get_explicite_references()
        self.results["explicite_refences"] = explicite_refences

    def _append_primarykeys(self):
        """
        Gathers and appends primary key details from the database to the results dictionary.
        """
        primarykeys = self.connector.get_primarykeys()
        self.results["primarykeys"] = primarykeys