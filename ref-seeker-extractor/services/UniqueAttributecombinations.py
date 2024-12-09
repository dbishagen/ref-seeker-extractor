import itertools

class UACFinder:
    """
    UACFinder class for searching and handling Unique Attributecombinations (UACs) in a MariaDB database.
    """
    def __init__(self, connector, max_UAC_attibutes):
        """
        Initializes a new instance of the UACFinder class.

        Args:
          connector (DBConnector): An instance of DBConnector used for database connections.     
        """
        self.connector = connector #MariaDBConnector
        self.max_UAC_attibutes = max_UAC_attibutes
        self.start_search()

    def start_search(self):
        """
        Searches for all UACs in database.
        """
        servers = self.connector.get_servers()
        for server in servers:
            databases = self.connector.get_databases(server)
            for database in databases:
                datastorages = self.connector.get_datastorages(database)
                for datastorage in datastorages:
                    attributes = self.connector.get_attributes(datastorage)
                    self._find_UACs(server, database, datastorage, attributes)

    def _find_UACs(self, server, database, datastorage, attributes):
        """
        Private method for finding User Attribute Combinations (UACs) based on various criteria.

        Args:
          server (int): The ID of the server.
          database (int): The ID of the database.
          datastorage (int): The ID of the data storage.
          attributes (list[int]): List of attribute IDs.        
        """
        attributes_to_remove = []

        # Determine number of entries
        number_of_entries = self.connector.get_max_entry_number(attributes)

        # Check for arrays
        for attribute in attributes:  
            check = self.connector.check_if_attribute_contains_array(attribute)         
            if check:
                attributes_to_remove.append(attribute)
        # Remove attributes from list
        for item in attributes_to_remove:
            attributes.remove(item)
        attributes_to_remove = []

        # Check for empty entries
        for attribute in attributes:
            entries = self.connector.get_number_of_entries(attribute)
            if entries < number_of_entries:
                attributes_to_remove.append(attribute)
        # Remove attributes from list
        for item in attributes_to_remove:
            attributes.remove(item)
        attributes_to_remove = []   

        # Check for UACs
        combination_size = 1
        while combination_size <= len(attributes):
            attribut_combinations = itertools.combinations(attributes, combination_size)
            for combination in attribut_combinations:
                number_of_unique_entries = self.connector.get_number_of_unique_entries_for_attributes(combination)
                if number_of_entries == number_of_unique_entries:
                    #print(f"UAC found, attribute: {combination_string}")
                    self.connector.add_UAC(server, database, datastorage, combination)
                    # Adds entrys to the remove-list, that are not in the list. It is possible that the same id is in more than one combination
                    attributes_to_remove.extend(x for x in combination if x not in attributes_to_remove)
             # Remove attributes from list
            for item in attributes_to_remove:
                attributes.remove(item)
            attributes_to_remove = []  
            combination_size += 1
            if combination_size > self.max_UAC_attibutes: break