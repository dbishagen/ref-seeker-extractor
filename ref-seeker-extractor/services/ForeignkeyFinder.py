import os
import re
import math
from services.Containers import ContainerUACs
from services.Containers import ContainerINDs
from services.Containers import ContainerAttributes
from rapidfuzz import process, fuzz
from dictances import bhattacharyya_coefficient
import nltk
# Set path for nltk corpus
path = os.path.join(".", "nltk_data")
nltk.data.path.append(path)
from nltk.corpus import wordnet # Need to install cospus: https://www.nltk.org/data.html

class ForeignkeyFinder:
    """
    A class dedicated to evaluating the likelihood that an Inclusion Dependency (IND) represents a foreign key.
    """

    def __init__(self, connector):
        """
        Initializes the ForeignKeyFinder with a database connector.

        Args:
            connector (DBConnector): An instance used for database connections.
        """
        self.connector = connector
        self.containerINDs = ContainerINDs()
        self.containerAttributes = ContainerAttributes()
        self.start_calculating()

    def start_calculating(self):
        """
        Initiates the process of computing foreign key probabilities by loading necessary data,
        tokenizing attributes and data storage names, and calculating similarity measures.
        """
        self._load_INDS()
        self._load_UACs()
        self._load_attributes()
        # Load tokens
        token_count = {}  # Initialize an empty dictionary to store token counts
        for attribute in self.containerAttributes:
            name = attribute.get_name()
            tokens = self._split_to_token(name)
            for token in tokens:
                # Increment the count for each token
                token_count[token] = token_count.get(token, 0) + 1
            # Count datastorage names
            datastorage_id = attribute.get_datastorage_id()
            datastorage_name = self.connector.get_datastorage_name(datastorage_id)
            tokens = self._split_to_token(datastorage_name)
            for token in tokens:
                # Increment the count for each token
                token_count[token] = token_count.get(token, 0) + 1            
        # Calculate token weight
        token_weights = self._calculate_weights(token_count)
        # Calculation
        for IND in self.containerINDs:
            UAC = IND.get_UAC()
            IND_id = IND.get_IND_id()
            child_attribute_ids = IND.get_child_attributes()
            parent_attribute_ids = UAC.get_attributes()
            count_attributes = len(child_attribute_ids)
            # Calculate syntactic name similarity
            name_weighted_similarity = 0.0
            for i in range(count_attributes):
                # Load tokens
                parent_id = parent_attribute_ids[i]
                parent_name = self.connector.get_attribute_name(parent_id)
                parent_tokens = self._split_to_token(parent_name)  
                parent_datastorage_id = self.connector.get_datastorage_id_for_attribute_id(parent_id)
                parent_datastorage_name = self.connector.get_datastorage_name(parent_datastorage_id)
                parent_tokens.extend(self._split_to_token(parent_datastorage_name))
                child_id = child_attribute_ids[i]
                child_name = self.connector.get_attribute_name(child_id)
                child_tokens = self._split_to_token(child_name)
                child_datastorage_id = self.connector.get_datastorage_id_for_attribute_id(child_id)
                child_datastorage_name = self.connector.get_datastorage_name(child_datastorage_id)
                child_tokens.extend(self._split_to_token(child_datastorage_name))
                name_weighted_similarity += self._calculate_similarity(parent_tokens, child_tokens, token_weights)
            name_weighted_similarity /= count_attributes
            # Calculate data distribution
            bhattacharyya = 0.0
            num_buckets = 20
            for i in range(count_attributes):
                parent_id = parent_attribute_ids[i]
                child_id = child_attribute_ids[i]        
                # Test types
                parent_types = self.connector.get_attribute_types(parent_id)
                if all(item in ("int") for item in parent_types):
                    # Contains only integer
                    values_set = set(int(value) for value in self.connector.get_values_for_attribute(parent_id))
                    minimum = min(values_set)
                    maximum = max(values_set)
                    parent_buckets = self._calculate_buckets_for_number(values_set, minimum, maximum, num_buckets)
                    values_set = set(int(value) for value in self.connector.get_values_for_attribute(child_id))
                    child_buckets = self._calculate_buckets_for_number(values_set, minimum, maximum, num_buckets)
                elif all(item in ("int", "float") for item in parent_types):
                    # Contains only numbers
                    values_set = set(float(value) for value in self.connector.get_values_for_attribute(parent_id))
                    minimum = min(values_set)
                    maximum = max(values_set)
                    parent_buckets = self._calculate_buckets_for_number(values_set, minimum, maximum, num_buckets)
                    values_set = set(float(value) for value in self.connector.get_values_for_attribute(child_id))
                    child_buckets = self._calculate_buckets_for_number(values_set, minimum, maximum, num_buckets)
                else:
                    # Contains other types
                    parent_count_entries = self.connector.get_number_of_valueentries(parent_id)
                    steps = parent_count_entries / num_buckets
                    parent_buckets = {"bucket" + str(i): 0.0 for i in range(1, num_buckets + 1)}
                    child_count_entries = self.connector.get_number_of_valueentries(child_id)
                    child_buckets = {"bucket" + str(i): 0.0 for i in range(1, num_buckets + 1)}
                    for i in range(1, num_buckets):
                        # Counts values for the buckets, skips the last one
                        offset = round(steps * i)
                        if offset >= parent_count_entries:
                            # Its possible that the round function produces a greater value
                            offset = parent_count_entries - 1
                        # Selects the value at a specific position
                        word = self.connector.get_value_at_specific_position(parent_id, offset)
                        word = self._alter_quotation_marks(word)
                        # Calculate parent buckets
                        result = self.connector.count_values_to_specific_value(parent_id, word)
                        parent_buckets["bucket" + str(i)] = result                        
                        # Calculate child buckets
                        result = self.connector.count_values_to_specific_value(child_id, word)
                        child_buckets["bucket" + str(i)] = result
                    parent_buckets["bucket" + str(num_buckets)] += parent_count_entries # Builds the last bucket
                    child_buckets["bucket" + str(num_buckets)] += child_count_entries # Builds the last bucket
                    for i in range(num_buckets, 1, -1):
                        # Calculates the values for the buckets, skips the first bucket
                        parent_buckets["bucket" + str(i)] -= parent_buckets["bucket" + str(i - 1)]
                        parent_buckets["bucket" + str(i)] /= parent_count_entries
                        child_buckets["bucket" + str(i)] -= child_buckets["bucket" + str(i - 1)]
                        child_buckets["bucket" + str(i)] /= child_count_entries
                    parent_buckets["bucket1"] /= parent_count_entries # Calculates the first bucket    
                    child_buckets["bucket1"] /= child_count_entries # Calculates the first bucket
                # Calculate bhattacharyya coefficient
                bhattacharyya += bhattacharyya_coefficient(parent_buckets, child_buckets)
            bhattacharyya /= count_attributes
            # Calculate IRIS similarity
            iris_similarity = 0.0
            for i in range(count_attributes):
                # Load tokens
                parent_id = parent_attribute_ids[i]
                parent_name = self.connector.get_attribute_name(parent_id)
                parent_tokens = self._split_to_token(parent_name)  
                child_id = child_attribute_ids[i]
                child_name = self.connector.get_attribute_name(child_id)
                child_tokens = self._split_to_token(child_name)
                parent_result = 0.0
                for parent_token in parent_tokens:
                    maximum = 0.0
                    # Calculate syntactic similarity
                    maximum_syn = 0.0
                    for child_token in child_tokens:
                        result = fuzz.ratio(parent_token, child_token)
                        result /= 100 # Result is in perecentage
                        if result > maximum_syn:
                            maximum_syn = result
                    # Calculate semantic similarity if syntactic similarity is below 0.5
                    if maximum_syn >= 0.5:
                        maximum = maximum_syn
                    else:
                        # Calculate semantic similarity
                        maximum_sem = 0.0
                        for child_token in child_tokens:
                            # Load tokens to wordnet
                            syn1 = wordnet.synsets(parent_token)
                            syn2 = wordnet.synsets(child_token)
                            if syn1 and syn2: # Test if tokens are in wordnet
                                syn1 = syn1[0]
                                syn2 = syn2[0]
                                result = syn1.wup_similarity(syn2)
                                result /= 100 # Result is in perecentage
                            else:
                                # Set result to 0 if tokens are not in wordnet
                                result = 0
                            if result > maximum_sem:
                                maximum_sem = result
                        # Use semantic similarity if its 0.7 or higher
                        if maximum_sem >= 0.7:
                            maximum = maximum_sem
                        else:
                            maximum = maximum_syn
                    parent_result += maximum
                parent_result /= len(parent_tokens)
                iris_similarity += parent_result
            iris_similarity /= count_attributes
            # Calculate Hybrid similarity
            hybrid_similarity = 0.0
            for i in range(count_attributes):
                # Load tokens
                parent_id = parent_attribute_ids[i]
                parent_name = self.connector.get_attribute_name(parent_id)
                parent_tokens = self._split_to_token(parent_name)
                parent_datastorage_id = self.connector.get_datastorage_id_for_attribute_id(parent_id)
                parent_datastorage_name = self.connector.get_datastorage_name(parent_datastorage_id)
                parent_tokens.extend(self._split_to_token(parent_datastorage_name))
                child_id = child_attribute_ids[i]
                child_name = self.connector.get_attribute_name(child_id)
                child_tokens = self._split_to_token(child_name)
                child_datastorage_id = self.connector.get_datastorage_id_for_attribute_id(child_id)
                child_datastorage_name = self.connector.get_datastorage_name(child_datastorage_id)
                child_tokens.extend(self._split_to_token(child_datastorage_name))                
                parent_result = 0.0
                sum_weight = 0.0
                for parent_token in parent_tokens:
                    maximum = 0.0
                    # Calculate syntactic similarity
                    maximum_syn = 0.0
                    for child_token in child_tokens:
                        result = fuzz.ratio(parent_token, child_token)
                        result /= 100 # Result is in perecentage
                        if result > maximum_syn:
                            maximum_syn = result
                    # Calculate semantic similarity if syntactic similarity is below 0.5
                    if maximum_syn >= 0.5:
                        maximum = maximum_syn
                    else:
                        # Calculate semantic similarity
                        maximum_sem = 0.0
                        for child_token in child_tokens:
                            # Load tokens to wordnet
                            syn1 = wordnet.synsets(parent_token)
                            syn2 = wordnet.synsets(child_token)
                            if syn1 and syn2: # Test if tokens are in wordnet
                                syn1 = syn1[0]
                                syn2 = syn2[0]
                                result = syn1.wup_similarity(syn2)
                                result /= 100 # Result is in perecentage
                            else:
                                # Set result to 0 if tokens are not in wordnet
                                result = 0
                            if result > maximum_sem:
                                maximum_sem = result
                        # Use semantic similarity if its 0.7 or higher
                        if maximum_sem >= 0.7:
                            maximum = maximum_sem
                        else:
                            maximum = maximum_syn
                    weight_for_token = token_weights.get(parent_token, None)
                    parent_result += maximum * weight_for_token
                    sum_weight += weight_for_token
                parent_result /= sum_weight
                hybrid_similarity += parent_result
            hybrid_similarity /= count_attributes

            # Calculate probabilities
            # HoPF probability
            hopf_probability = (bhattacharyya + name_weighted_similarity) / 2
            # IRIS-DS probability
            iris_probability = iris_similarity
            # Hybrid probability
            hybrid_only_name_probability = hybrid_similarity
            hybrid_probability = (bhattacharyya + hybrid_similarity) / 2

            # Write result to IND
            self.connector.add_IND_FKscores(IND_id, 
                                            name_weighted_similarity, bhattacharyya, iris_similarity, hybrid_similarity, 
                                            hopf_probability, iris_probability,hybrid_only_name_probability, hybrid_probability)

    def _calculate_buckets_for_number(self, value_set, minimum, maximum, num_buckets):
        """
        Calculates the buckets for the bhattacharyya coefficient.

        Args:
            value_set (set): The set with the values, values must be int or float.
            minimun (int, float): The minimum value.
            maximum (int, float): The maximum value.
            num_buckets (int): The number of buckets.
        Returns:
            dictionary: Dictionary for the bhattacharyya coefficient.
        """
        steps = (maximum - minimum) / num_buckets
        count_entries = len(value_set)
        # Creates dictionary with the buckts.
        buckets = {"bucket" + str(i): 0.0 for i in range(1, num_buckets + 1)}
        threshold = minimum # To count values <= threshold
        for i in range(1, num_buckets):
            # Counts values for the buckets, skips the last one
            threshold += steps
            count_smaller_or_equal = sum(1 for entry in value_set if entry <= threshold)
            buckets["bucket" + str(i)] = count_smaller_or_equal
        buckets["bucket" + str(num_buckets)] += count_entries # Builds the last bucket
        for i in range(num_buckets, 1, -1):
            # Calculates the values for the buckets, skips the first bucket
            buckets["bucket" + str(i)] -= buckets["bucket" + str(i - 1)]
            buckets["bucket" + str(i)] /= count_entries
        buckets["bucket1"] /= count_entries # Calculates the first bucket

        return buckets

    def _split_to_token(self, string):
        """
        Splits a string into tokens based on delimiters and casing.

        Args:
            string (str): The string to be tokenized.

        Returns:
            list: A list of tokens derived from the string.
        """
        # Clean String
        string = re.sub(r'[^a-zA-Z0-9 _]', '', string)
        if ' ' in string:
            # If the string contains an space, split at the space and filter out empty strings.
            return [token for token in string.split(' ') if token]   
        elif '_' in string:
            # If the string contains an underscore, split at the underscore and filter out empty strings.
            return [token for token in string.split('_') if token]
        elif any(c.isupper() for c in string):
            # If the string contains uppercase letters, split before each uppercase letter.
            result = [string[0]]
            for char in string[1:]:
                if char.isupper():
                    result.append('_')
                result.append(char)
            return ''.join(result).split('_')
        else:
            # If neither underscore nor uppercase letters are present, return the original string as a single-element list.
            return [string]

    def _calculate_weights(self, token_frequencies):
        """
        Calculates weights for tokens based on their frequencies using the inverse document frequency method.

        Args:
            token_frequencies (dict): A dictionary with tokens as keys and their frequencies as values.

        Returns:
            dict: A dictionary with tokens as keys and their calculated weights as values.
        """
        count = 0
        for token, frequency in token_frequencies.items():
            count += frequency
        weights = {}
        for token, frequency in token_frequencies.items():
            weight = math.log(1 / (frequency/count))
            weights[token] = weight
        return weights

    def _calculate_similarity(self, parent_tokens, child_tokens, token_weights = None):
        """
        Calculates the name similarity with optional with weights.

        Args:
            parent_tokens (list): List of strings with the parent tokens.
            child_tokens (list): List of strings with the child tokens.
            token_weights (Optional[dict]): Dictionary with weights.
        Returns:
            float: Similarty, between 0 and 1
        """
        if token_weights is None:
            sum_token_results = 0
            count_tokens = 0
            while parent_tokens:
                max_score = 0
                selected_child_token = None
                selected_parent_token = None
                for token in parent_tokens:
                    # Checks if there are child tokens
                    if not child_tokens:
                        max_score = 0
                        selected_parent_token = token
                        selected_child_token = None
                        break
                    # Calculates the fuzzy matching score for the best match
                    token_result = process.extract(token, child_tokens, scorer=fuzz.ratio, limit=1)   
                    score_result = token_result[0][1] # Selects the similarity
                    score_result = score_result / 100 # Normalizes between 0 and 1
                    if max_score <= score_result:
                        max_score = score_result
                        selected_child_token = token_result[0][0]
                        selected_parent_token = token
                # Calculates score
                count_tokens += 1
                # Sums everything up
                sum_token_results += max_score
                # Removes Token
                parent_tokens.remove(selected_parent_token)
                if selected_child_token:
                    child_tokens.remove(selected_child_token)
            # Calclates the over all similarity
            result = sum_token_results / count_tokens
        else:
            sum_token_results = 0
            sum_token_weights = 0
            while parent_tokens:
                max_score = 0
                selected_child_token = None
                selected_parent_token = None
                for token in parent_tokens:
                    # Checks if there are child tokens
                    if not child_tokens:
                        max_score = 0
                        selected_parent_token = token
                        selected_child_token = None
                        break
                    # Calculates the fuzzy matching score for the best match
                    token_result = process.extract(token, child_tokens, scorer=fuzz.ratio, limit=1)   
                    score_result = token_result[0][1] # Selects the similarity
                    score_result = score_result / 100 # Normalizes between 0 and 1
                    if max_score <= score_result:
                        max_score = score_result
                        selected_child_token = token_result[0][0]
                        selected_parent_token = token
                # Calculates score with token weight
                weight_for_token = token_weights.get(selected_parent_token, None)
                sum_token_weights += weight_for_token
                # Sums everything up
                sum_token_results += max_score * weight_for_token
                # Removes Token
                parent_tokens.remove(selected_parent_token)
                if selected_child_token:
                    child_tokens.remove(selected_child_token)
            # Calclates the over all similarity
            result = sum_token_results / sum_token_weights

        return result

    def _load_INDS(self):
        """
        Loads InclusionsDependencies (INDs) from the database.
        """
        list_INDs = self.connector.get_INDs()
        for entry in list_INDs:
            IND_id = entry["IND_id"]
            UAC_id = entry["UAC_id"]
            server_id = entry["server_id"]
            database_id = entry["database_id"]
            datastorage_id = entry["datastorage_id"]
            attributes = entry["attributes"]
            self.containerINDs.add_IND(IND_id, UAC_id, server_id, database_id, datastorage_id, attributes)

    def _load_UACs(self):
        """
        Loads Unique AttributeCombinations (UACs) from the database.
        """
        for entry in self.containerINDs:
            UAC_id = entry.get_UAC_id()
            UAC = self.connector.get_UAC_via_id(UAC_id)
            UAC_server_id = UAC["server_id"]
            UAC_database_id = UAC["database_id"]
            UAC_datastorage_id = UAC["datastorage_id"]
            UAC_attributes = UAC["attributes"]
            entry.set_UAC(UAC_server_id, UAC_database_id, UAC_datastorage_id, UAC_attributes)

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
                        min = None
                        max = None
                        name = self.connector.get_attribute_name(attribute)       
                        self.containerAttributes.add_attribute(server, database, datastorage, attribute, min, max, name)   

    def _alter_quotation_marks(self, input_string):
        """
        Alters single quotation marks in the input string by adding a backslash before them. It's needed for SQL queries.

        Args:
            input_string (str): The input string containing single quotation marks.

        Returns:
            str: The output string with single quotation marks altered by adding a backslash before them.
        """
        if "'" in input_string:
            idx = input_string.index("'")
            input_string = input_string[:idx] + "\\" + input_string[idx:]
        return input_string
