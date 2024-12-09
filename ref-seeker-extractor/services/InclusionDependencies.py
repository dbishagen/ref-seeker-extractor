from itertools import product
import itertools
from services.Containers import ContainerAttributes
from services.Containers import ContainerSingleAttributeFromUAC
from services.Containers import ContainerUACs
import random
import time

class INDFinder:
    """
    INDFinder class for searching and handling Inclusion Dependencies (INDs) in a SQL-database.
    """

    def __init__(self, connector, find_max_ind, speed_mode=0):
        """
        Initializes a new instance of the INDFinder class.

        Args:
          connector (DBConnector): An instance of DBConnector used for database connections.
          find_max_ind (bool): If true the prototyp will search for all maximal inlusion dependencies.
          speed_mode (int): Must be between 0 and 10. If greeter than 0 it uses heuristic methode to find N-ary INDs.
        """
        self.connector = connector
        self.find_max_ind = find_max_ind
        self.speed_mode = speed_mode


    def find_inds(self):
        
        time_metrics = {
            "time_init": -1,
            "time_find_unary": -1,
            "time_find_max_inds": -1
        }

        start_time = time.time()
        
        self.containerAttributes = ContainerAttributes()
        self._load_attributes()





        # NOTE: comment out if only max INDs are needed
        self.containerPartUACs = ContainerSingleAttributeFromUAC()
        self.containerUACs = ContainerUACs(self.containerPartUACs)
        self._load_UACs()





        time_metrics["time_init"] = time.time() - start_time



        # NOTE: comment out if only max INDs are needed
        start_time = time.time()
        self.start_search()
        time_metrics["time_find_unary"] = time.time() - start_time




        
        if self.find_max_ind:
            start_time = time.time()
            self.search_max_inds()
            time_metrics["time_find_max_inds"] = time.time() - start_time
        return time_metrics


    def search_max_inds(self):
        """
        Initiates the search for maximal Inclusion Dependencies (INDs) in the connected SQL-database.
        """
        # Check for unary Inclusiondependencies
        for parent in self.containerAttributes:
            parent_datastorage_id = parent.get_datastorage_id()
            parent_id = parent.get_attribute_id()
            parent_values = self.connector.get_values_for_attribute(parent_id)
            parent_values = set(parent_values)
            for child in self.containerAttributes:
                child_datastorage_id = child.get_datastorage_id()
                child_id = child.get_attribute_id()
                if parent_datastorage_id != child_datastorage_id:
                    child_min = child.get_min()
                    child_max = child.get_max()
                    # Checks min and max
                    check = self.connector.check_if_value_exist(child_min, parent_id)
                    if not check: continue
                    check = self.connector.check_if_value_exist(child_max, parent_id)
                    if not check: continue
                    # Check for unary INDs
                    child_values = self.connector.get_values_for_attribute(child_id)
                    child_values = set(child_values)
                    check = child_values.issubset(parent_values)
                    if check:
                        parent.add_IND(child)

        # Check for arrays
        for attribute in self.containerAttributes:
            attribute_id = attribute.get_attribute_id()
            is_array = self.connector.check_if_attribute_contains_array(attribute_id)
            attribute.set_is_array(is_array)

        # Check for INDs with array
        # The INDs will be removed
        for parent in self.containerAttributes:
            parent_server_id = parent.get_server_id()
            parent_database_id = parent.get_database_id()
            parent_datastorage_id = parent.get_datastorage_id()
            parent_id = parent.get_attribute_id()
            # Check if parent is array
            if parent.get_is_array():
                for child in parent.get_INDs():
                    child_server_id = child.get_server_id()
                    child_database_id = child.get_database_id()                    
                    child_datastorage_id = child.get_datastorage_id()
                    child_id = child.get_attribute_id()
                    self.connector.add_maxIND(parent_server_id, parent_database_id, parent_datastorage_id, [parent_id],
                                              child_server_id, child_database_id, child_datastorage_id, [child_id])
                    parent.remove_IND(child)
            # Check if child is array
            for child in parent.get_INDs():
                if child.get_is_array():
                    child_server_id = child.get_server_id()
                    child_database_id = child.get_database_id()                    
                    child_datastorage_id = child.get_datastorage_id()
                    child_id = child.get_attribute_id()
                    self.connector.add_maxIND(parent_server_id, parent_database_id, parent_datastorage_id, [parent_id],
                                                child_server_id, child_database_id, child_datastorage_id, [child_id])
                    parent.remove_IND(child)

        # Get datastorages
        list_datastorage_ids = []
        for server_id in self.connector.get_servers():
            for database_id in self.connector.get_databases(server_id):
                for datastorage_id in self.connector.get_datastorages(database_id):
                    list_datastorage_ids.append(datastorage_id)

        # Set number of test for heuristic mode
        if self.speed_mode == 0:
            num_of_tests = 3
        else:
            num_of_tests = (11 - self.speed_mode) * 3

        # Check for max INDs
        list_nary_inds = []
        for datastorage_id in list_datastorage_ids:
            list_parent_attributes = []
            for attribute in self.containerAttributes:
                if datastorage_id == attribute.get_datastorage_id():
                    if attribute.get_INDs():
                        list_parent_attributes.append(attribute)
            # Check if list is empty
            if not list_parent_attributes: continue
            # Check for n-ary INDs
            for i in range(len(list_parent_attributes), 0, -1):
                parent_combinations_to_test = list(itertools.combinations(list_parent_attributes, i))
                for parent_combination in parent_combinations_to_test:
                    # Generat candidats
                    list_unary_INDs_ids = []
                    list_parent_ids = []
                    for parent_attribute in parent_combination:
                        list_parent_ids.append(parent_attribute.get_attribute_id())
                        unary_inds = parent_attribute.get_INDs()
                        unary_ind_ids = []
                        for ind in unary_inds:
                            unary_ind_ids.append(ind.get_attribute_id())
                        list_unary_INDs_ids.append(unary_ind_ids)
                    combinations_to_test = self._generate_combinations(list_unary_INDs_ids)
                    # Test candidats
                    for combination in combinations_to_test:
                        # Test if there is an attribute more than one time in the list
                        check = self._has_duplicates(combination)
                        if check: continue
                        # Test if combination is in the same datastorage
                        check = self._test_if_attributes_in_same_datastorage(combination)
                        if not check: continue
                         # Test if a part of the N-ary IND candidate has a missing value
                        check = self.connector.check_if_attribut_has_a_appropriate_entry_no(combination)
                        if not check: continue
                        # Test if combination is part of an greater IND
                        check = False
                        if list_nary_inds:
                            for ind in list_nary_inds:
                                if self._is_subsequence(list_parent_ids, ind["parent"]):
                                    if self._is_subsequence(combination, ind["child"]):
                                        check = True
                                        break
                        if check: continue
                        #Test IND
                        check = self._fast_Nary_IND_test(list_parent_ids, combination, num_of_tests)
                        if not check: continue
                        if self.speed_mode == 0:
                            check = self.connector.Nary_IND_test(list_parent_ids, combination)
                        # Add IND
                        if check:
                            result = {"parent": list_parent_ids, "child": combination}
                            list_nary_inds.append(result)
        # Write result
        for ind in list_nary_inds:
            parent_attribute_ids = ind["parent"]
            child_attribute_ids = ind["child"]
            # Get server_id, database_id and datastorage_id of the IND
            for attribute in self.containerAttributes:
                if parent_attribute_ids[0] == attribute.get_attribute_id():
                    parent_server_id = attribute.get_server_id()
                    parent_database_id = attribute.get_database_id()
                    parent_datastorage_id = attribute.get_datastorage_id()
                if child_attribute_ids[0] == attribute.get_attribute_id():
                    child_server_id = attribute.get_server_id()
                    child_database_id = attribute.get_database_id()
                    child_datastorage_id = attribute.get_datastorage_id()
            self.connector.add_maxIND(parent_server_id, parent_database_id, parent_datastorage_id, parent_attribute_ids,
                   child_server_id, child_database_id, child_datastorage_id, child_attribute_ids)
            
    def start_search(self):
        """
        Initiates the search for Inclusion Dependencies (INDs) in the connected SQL-database.
        """
        # Check for unary Inclusiondependencies
        for parent in self.containerPartUACs:
            parent_datastorage_id = parent.get_datastorage_id()
            parent_id = parent.get_attribute_id()
            parent_values = self.connector.get_values_for_attribute(parent_id)
            parent_values = set(parent_values)
            for child in self.containerAttributes:
                child_datastorage_id = child.get_datastorage_id()
                child_id = child.get_attribute_id()
                if parent_datastorage_id != child_datastorage_id:
                    child_min = child.get_min()
                    child_max = child.get_max()
                    # Checks min and max
                    check = self.connector.check_if_value_exist(child_min, parent_id)
                    if not check: continue
                    check = self.connector.check_if_value_exist(child_max, parent_id)
                    if not check: continue
                    # Check for unary INDs
                    child_values = self.connector.get_values_for_attribute(child_id)
                    child_values = set(child_values)
                    check = child_values.issubset(parent_values)
                    if check:
                        parent.add_IND(child)

        # Check UACs
        for UAC in self.containerUACs:
            list_UAC_attributes = UAC.get_attributes()
            # Unary UAC, with only one attribute
            if len(list_UAC_attributes) == 1:
                UAC_attribute_id = list_UAC_attributes[0]
                # Search for the part of UAC with the same ID
                for partUAC in self.containerPartUACs:
                    if UAC_attribute_id == partUAC.get_attribute_id():
                        list_INDs = partUAC.get_INDs()
                        # Checks if there is no IND
                        if not list_INDs: break
                        UAC_id = UAC.get_UAC_id()
                        for IND in list_INDs:
                            IND_server_id = IND.get_server_id()
                            IND_database_id = IND.get_database_id()
                            IND_datastorage_id = IND.get_datastorage_id()
                            IND_attribute_id = IND.get_attribute_id()
                            self.connector.add_IND(UAC_id, IND_server_id, IND_database_id, IND_datastorage_id, [IND_attribute_id])
                        break # It´s not necessary to search the other parts
            # N-ary UAC, with more than one attribute
            else:
                # Build list with attribut IDs of the unary INDs
                list_IND_attributes = [] # These list will contain lists with attributes
                # Itterates every attribut of the UAC
                for single_attribut_id_from_UAC in list_UAC_attributes:
                    # Search for the part of UAC with the same ID
                    for partUAC in self.containerPartUACs:
                        if single_attribut_id_from_UAC == partUAC.get_attribute_id():
                            list_attribute_id_IND_part_UAC = []
                            list_INDs = partUAC.get_INDs()
                            # Checks if there is no IND
                            if not list_INDs: break
                            # Build list with attribute IDs of the INDs for the part of the UAC
                            for IND in list_INDs:
                                IND_attribute_id = IND.get_attribute_id()
                                # Check if IND contains an array
                                is_array = IND.get_is_array()
                                if is_array is None:
                                    # Needs to be tested if its a array
                                    is_array = self.connector.check_if_attribute_contains_array(IND_attribute_id)
                                    IND.set_is_array(is_array)
                                if not is_array:
                                    list_attribute_id_IND_part_UAC.append(IND_attribute_id)
                            break # It´s not necessary to search the other parts
                    # Skip empty IND list
                    if not list_attribute_id_IND_part_UAC: break
                    list_IND_attributes.append(list_attribute_id_IND_part_UAC)
                # Check if there was a part of the UAC that doenst have INDs
                if len(list_IND_attributes) != len(list_UAC_attributes): continue
                # Build list combinations
                combination_INDs_to_test = self._generate_combinations(list_IND_attributes)
                # Test if combination is in the same datastorage
                copied_list = combination_INDs_to_test.copy() # Its needed to make a copy of the list, because its not possible to delete an entry in a for-loop
                for list_to_check in copied_list:
                    check = self._test_if_attributes_in_same_datastorage(list_to_check)
                    if not check:
                        combination_INDs_to_test.remove(list_to_check)               
                if not combination_INDs_to_test: continue # Next UAC if list of INDs is empty
                # Check if a part of the N-ary IND candidate has a missing value
                copied_list = combination_INDs_to_test.copy() # Its needed to make a copy of the list, because its not possible to delete an entry in a for-loop
                for list_to_check in copied_list:
                    check = self.connector.check_if_attribut_has_a_appropriate_entry_no(list_to_check)
                    if not check:
                        combination_INDs_to_test.remove(list_to_check)               
                if not combination_INDs_to_test: continue # Next UAC if list of INDs is empty
                # Check if its a N-ary IND
                for combination_to_test in combination_INDs_to_test:
                    if self.speed_mode == 0:
                        num_of_tests = 3
                    else:
                        num_of_tests = (11 - self.speed_mode) * 3
                    check = self._fast_Nary_IND_test(list_UAC_attributes, combination_to_test, num_of_tests)
                    if not check: continue
                    if self.speed_mode == 0:
                        check = self.connector.Nary_IND_test(list_UAC_attributes, combination_to_test)
                    if check:
                        UAC_id = UAC.get_UAC_id()
                        # Get server_id, database_id and datastorage_id of the IND
                        for attribute in self.containerAttributes:
                            if combination_to_test[0] == attribute.get_attribute_id():
                                IND_server_id = attribute.get_server_id()
                                IND_database_id = attribute.get_database_id()
                                IND_datastorage_id = attribute.get_datastorage_id()
                                break
                        self.connector.add_IND(UAC_id, IND_server_id, IND_database_id, IND_datastorage_id, combination_to_test)

    def _test_if_attributes_in_same_datastorage(self, list_attribute_ids):
        """
        Test if the attributes in the list are in the same datastorage.

        Args:
            list_attribute_ids (list): List with Attribute IDs to test.
            
        Returns:
            bool: True if the attributes are in the same datastorage.        
        """
        list_datasotrage_ids = []
        for attribute_id in list_attribute_ids:
            for attribute in self.containerAttributes:                
                if attribute_id == attribute.get_attribute_id():
                    datastorage_id = attribute.get_datastorage_id()
                    list_datasotrage_ids.append(datastorage_id)
                    break
        return all(element == list_datasotrage_ids[0] for element in list_datasotrage_ids)

    def _generate_combinations(self, input_list):
        """
        Generate all combinations of numbers from the input list without repeating numbers within each combination.

        Args:
            input_list (list of lists): A list containing sublists with numbers.

        Returns:
            list of lists: A list of all combinations without repeating numbers within each combination.
        """
        result = list(product(*input_list))
        unique_combinations = [list(combination) for combination in result if len(set(combination)) == len(combination)]
        return unique_combinations

    def _is_subsequence(self, sub_list, main_list):
        """
        Check if all elements of sub_list are contained in main_list in the same order,
        allowing for elements in between.

        Args:
            sub_list (list): The list of elements to check for as a subsequence.
            main_list (list): The list in which to check for the subsequence.

        Returns:
            bool: True if sub_list is a subsequence of main_list, False otherwise.
        """
        it = iter(main_list)
        return all(any(x == y for y in it) for x in sub_list)

    def _has_duplicates(self, lst):
        """
        Check if a list contains any duplicate values.

        Args:
            lst (list): The list of integers to check for duplicates.

        Returns:
            bool: True if there are duplicate values, False otherwise.
        """
        seen = set()
        for element in lst:
            if element in seen:
                return True
            seen.add(element)
        return False

    def _load_attributes(self):
        """
        Loads attribute metadata from the database. And safes them to ManagerAttribute-class.
        """
        servers = self.connector.get_servers()
        for server in servers:
            databases = self.connector.get_databases(server)
            for database in databases:
                datastorages = self.connector.get_datastorages(database)
                for datastorage in datastorages:
                    attributes = self.connector.get_attributes(datastorage)
                    for attribute in attributes:
                        min = self.connector.get_attribute_min(attribute)
                        max = self.connector.get_attribute_max(attribute)                        
                        self.containerAttributes.add_attribute(server, database, datastorage, attribute, min, max)     

    def _load_UACs(self):
        """
        Loads Unique Attribute Combinations (UACs) from the database.
        """
        list_UACs = self.connector.get_UACs()
        for entry in list_UACs:
            UAC_id = entry["UAC_id"]
            server_id = entry["server_id"]
            database_id = entry["database_id"]
            datastorage_id = entry["datastorage_id"]
            attributes = entry["attributes"]
            self.containerUACs.add_UAC(UAC_id, server_id, database_id, datastorage_id, attributes)
    
    def _unary_IND_test_in_memory(self, parent_id, child_id):
        """
        Test for unary inclusiondependencies in memory.

        Attributes:
            parent_id(int): The Attribute ID of the parent Attribute to check.        
            child_id(int): The Attribute ID of the child Attribute to check. 

        Returns:
            boolean: True if its a unary IND.        
        """
        parent_values = self.connector.get_values_for_attribute(parent_id)
        child_values = self.connector.get_values_for_attribute(child_id)
        # Cast values to set
        parent_values = set(parent_values)
        child_values = set(child_values)
        if child_values.issubset(parent_values):
            return True
        else:
            return False
        
    def _fast_Nary_IND_test(self, list_UAC_attributes, combination_to_test, num_of_tests):
        """
        Fast check for inclusions dependencis. It only checks some of the entries.

        Args:
            list_UAC_attributes (list): The Attribute IDs of the parent Attributes to check. 
            combination_to_test (list): The Attribute IDs of the child Attributes to check. 

        Returns:
            boolean: True if it may be a N-ary IND.   
        """
        def common_integer_exists(list_of_lists):
            # Initialize a set with the elements of the first list
            common_elements = set(list_of_lists[0])            
            # Iterate over the remaining lists and update the set to keep only common elements
            for lst in list_of_lists[1:]:
                common_elements.intersection_update(lst)                
            # Check if the set contains at least one element
            return len(common_elements) > 0

        count_attributes = len(list_UAC_attributes)
        entry_nos_to_test = self.connector.get_list_of_entry_nos_for_attribute(combination_to_test[0])
        if len(entry_nos_to_test) < num_of_tests:
            num_of_tests = len(entry_nos_to_test)
        entry_nos_to_test = random.sample(entry_nos_to_test, num_of_tests)
        for entry_no in entry_nos_to_test:
            result_list = []
            # Selects the child values for the given entry_no and returns the entry_nos where the value is in the parent attribute
            for i in range(count_attributes):
                result = self.connector.get_related_entry_numbers(list_UAC_attributes[i], combination_to_test[i], entry_no)
                result_list.append(result) # Builds a list of lists with the entry_nos
            check = common_integer_exists(result_list)
            if not check:
                return False
        return True