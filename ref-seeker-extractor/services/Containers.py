# This is a collection of different containers to store informations

class ContainerUACs:
    """
    Manages a collection of Unique Attribute Combinations (UACs).
    """
    def __init__(self, containerPartUACs = None):
        """
        Initializes the container with an optional part container for UACs.

        Args:
            containerPartUACs (Optional[ContainerSingleAttributeFromUAC]): Part container to store attributes of the UACs.
        """
        self.containerPartUACs = containerPartUACs
        self.items = []

    def add_UAC(self, UAC_id, server_id, database_id, datastorage_id, list_attributes, hopf_score=None, iris_score=None):
        """
        Adds a new Unique Attribute Combination (UAC) to the container.

        Args:
            UAC_id (int): Unique identifier for the UAC.
            server_id (int): Identifier for the server.
            database_id (int): Identifier for the database.
            datastorage_id (int): Identifier for the data storage.
            list_attributes (list of int): List of attribute identifiers in the UAC.
            hopf_score (Optional[int]): Optional HoPF-Score associated with the UAC.
            iris_score (Optional[int]): Optional IRIS-DS-Score associated with the UAC.
        """
        UAC = UniqueAttributeCombination(UAC_id, server_id, database_id, datastorage_id, list_attributes, hopf_score, iris_score)
        self.items.append(UAC)
        # Adds attributes from UACs
        if self.containerPartUACs is not None:
            for attribute_id in list_attributes:
                self.containerPartUACs.add_partUAC(server_id, database_id, datastorage_id, attribute_id)

    def __iter__(self):
        """
        Yields each UAC stored in the container.

        Yields:
            UniqueAttributeCombination: The next UAC in the container.
        """
        for item in self.items:
            yield item

    def __len__(self):
        """
        Returns the number of UACs in the container.

        Returns:
            int: Number of UACs.
        """
        return len(self.items)

class UniqueAttributeCombination:
    """
    Represents a single Unique Attribute Combination (UAC) with optional scoring metrics.
    """
    def __init__(self, UAC_id, server_id, database_id, datastorage_id, list_attributes, hopf_score, iris_score):
        """
        Initializes a UAC with identifiers and scores.

        Args:
            UAC_id (int): Unique identifier for the UAC.
            server_id (int): Identifier for the server.
            database_id (int): Identifier for the database.
            datastorage_id (int): Identifier for the data storage.
            list_attributes (list of int): List of attribute identifiers in the UAC.
            hopf_score (int): HoPF-Score for the UAC.
            iris_score (int): IRIS-DS-Score for the UAC.
        """
        self.UAC_id = UAC_id
        self.server_id = server_id
        self.database_id = database_id
        self.datastorage_id = datastorage_id
        self.list_attributes = list_attributes
        self.hopf_score = hopf_score
        self.iris_score = iris_score

    def get_UAC_id(self):
        """
        Returns the ID of the UAC.

        Returns:
            int: The UAC ID.
        """
        return self.UAC_id

    def get_server_id(self):
        """
        Returns the server ID.

        Returns:
            int: The server ID.
        """
        return self.server_id

    def get_database_id(self):
        """
        Returns the database ID.

        Returns:
            int: The database ID.
        """
        return self.database_id

    def get_datastorage_id(self):
        """
        Returns the datastorage ID.

        Returns:
            int: The datastorage ID.
        """
        return self.datastorage_id

    def get_attributes(self):
        """
        Returns the list of attributes.

        Returns:
            list: The attribute IDs.
        """
        return self.list_attributes

    def get_hopf_score(self):
        """
        Returns the HoPF-Score.

        Returns:
            int: The HopF-Score.
        """
        return self.hopf_score

    def get_iris_score(self):
        """
        Returns the IRIS-DS.

        Returns:
            int: The IRIS-DS-Score.
        """
        return self.iris_score

class ContainerSingleAttributeFromUAC:
    """
    Manages a collection of single attributes extracted from UACs.
    """
    def __init__(self):
        """
        Initializes the container for storing single attributes.
        """
        self.items = []
    
    def add_partUAC(self, server_id, database_id, datastorage_id, attribute_id):
        """
        Adds a single attribute extracted from a UAC to the container if not already present.

        Args:
            server_id (int): Server identifier.
            database_id (int): Database identifier.
            datastorage_id (int): Data storage identifier.
            attribute_id (int): Attribute identifier.
        """     
        if len(self.items) == 0:
            new_partUAC = SingleAttributeFromUAC(server_id, database_id, datastorage_id, attribute_id)
            self.items.append(new_partUAC)
        else:
            # Check if attribute is already in the list
            already_in_list = any(attribute_id == partUAC.get_attribute_id() for partUAC in self.items)
            if not already_in_list:
                new_partUAC = SingleAttributeFromUAC(server_id, database_id, datastorage_id, attribute_id)
                self.items.append(new_partUAC)

    def __iter__(self):
        """
        Function to iterrate the entries.

        Returns:
            item (object): Stored object.
        """
        for item in self.items:
            yield item

    def __len__(self):
        """
        Standard len-function.
        """
        return len(self.items)

class SingleAttributeFromUAC:
    """
    Represents a single attribute extracted from a UAC, including methods to manage related inclusions.
    """
    def __init__(self, server_id, database_id, datastorage_id, attribute_id):
        """
        Initializes a single attribute from a UAC.

        Args:
            server_id (int): Server identifier.
            database_id (int): Database identifier.
            datastorage_id (int): Data storage identifier.
            attribute_id (int): Attribute identifier.
        """
        self.server_id = server_id
        self.database_id = database_id
        self.datastorage_id = datastorage_id
        self.attribute_id = attribute_id
        self.list_INDs = []

    def add_IND(self, class_attribute):
        """
        Adds a unary Inclusion Dependency (IND) to this attribute's list.

        Args:
            class_attribute (Attribute): The attribute object to add.
        """
        self.list_INDs.append(class_attribute)

    def get_INDs(self):
        """
        Returns the unary Inclusion Dependencies (INDs).

        Returns:
            list (objects): Returns a list with attributs.
        """
        return self.list_INDs

    def get_server_id(self):
        """
        Returns the server ID.

        Returns:
            int: The server ID.
        """
        return self.server_id

    def get_database_id(self):
        """
        Returns the database ID.

        Returns:
            int: The database ID.
        """
        return self.database_id

    def get_datastorage_id(self):
        """
        Returns the datastorage ID.

        Returns:
            int: The datastorage ID.
        """
        return self.datastorage_id

    def get_attribute_id(self):
        """
        Returns the attribute ID.

        Returns:
            int: The attribute ID.
        """
        return self.attribute_id

class ContainerAttributes:
    """
    This class manages a collection of attributes, providing functionalities to add and iterate over them.
    """
    def __init__(self):
        """
        Initializes an empty list to store attributes.
        """
        self.items = []

    def add_attribute(self, server_id, database_id, datastorage_id, attribute_id, min = None, max = None, name = None):
        """
        Adds an attribute with specified details to the collection.

        Args:
            server_id (int): Identifier for the server on which the attribute is stored.
            database_id (int): Identifier for the database containing the attribute.
            datastorage_id (int): Identifier for the data storage location of the attribute.
            attribute_id (int): Unique identifier for the attribute.
            min (optional): Minimum value of the attribute, can be any type that is comparable.
            max (optional): Maximum value of the attribute, can be any type that is comparable.
            name (Optional[str]): Name for the attribute.
        """
        attribute = Attribute(server_id, database_id, datastorage_id, attribute_id, min, max, name)
        self.items.append(attribute)

    def __iter__(self):
        """
        Allows iteration over the stored attributes.

        Yields:
            Attribute: The next attribute in the collection.
        """
        for item in self.items:
            yield item

    def __len__(self):
        """
        Returns the number of attributes in the collection.

        Returns:
            int: Number of stored attributes.
        """
        return len(self.items)

class Attribute:
    """
    Represents a single attribute, encapsulating details like identifier, server, database, and data storage location.
    """
    def __init__(self, server_id, database_id, datastorage_id, attribute_id, min = None, max = None, name = None):
        """
        Initializes the Attribute instance with the provided details.

        Args:
            server_id (int): Identifier for the server.
            database_id (int): Identifier for the database.
            datastorage_id (int): Identifier for the data storage.
            attribute_id (int): Unique identifier for the attribute.
            min (optional): Minimum value for the attribute.
            max (optional): Maximum value for the attribute.
            name (Optional[str]): Name of the attribute.
        """
        self.server_id = server_id
        self.database_id = database_id
        self.datastorage_id = datastorage_id
        self.attribute_id = attribute_id 
        self.min = min
        self.max = max
        self.name = name
        self.is_array = None  # Initializes to None, indicating it's not yet determined if this attribute is an array.
        self.list_INDs = []

    def add_IND(self, class_attribute):
        """
        Adds a unary Inclusion Dependency (IND) to this attribute's list.

        Args:
            class_attribute (Attribute): The attribute object to add.
        """
        self.list_INDs.append(class_attribute)

    def remove_IND(self, attribute_to_remove):
        """
        Removes a unary Inclusion Dependency (IND) from this attribute's list.

        Args:
            attribute_to_remove (Attribute): The attribute object to remove.
        """
        if attribute_to_remove in self.list_INDs:
            self.list_INDs.remove(attribute_to_remove)
        else:
            print(f"Attribute {attribute_to_remove} not found in the list of INDs.")

    def get_INDs(self):
        """
        Returns the unary Inclusion Dependencies (INDs).

        Returns:
            list (objects): Returns a list with attributs.
        """
        return self.list_INDs

    def set_is_array(self, state):
        """
        Sets the attribute's 'is_array' flag after checking its type.

        Args:
            state (bool): True if the attribute is an array, False otherwise.
        """
        if isinstance(state, bool):
            self.is_array = state
        else:
            print("Error state of (is_array) is not a boolean")

    def get_name(self):
        """
        Returns the name of the attribute.

        Returns:
            string: The name .
        """
        return self.name

    def get_is_array(self):
        """
        Returns the "true" if the attribute contains a array. And "none" if its necessary to test it.

        Return:
            boolean: State of the test.
        """
        return self.is_array

    def get_server_id(self):
        """
        Returns the server ID.

        Returns:
            int: The server ID.
        """
        return self.server_id

    def get_database_id(self):
        """
        Returns the database ID.

        Returns:
            int: The database ID.
        """
        return self.database_id

    def get_datastorage_id(self):
        """
        Returns the datastorage ID.

        Returns:
            int: The datastorage ID.
        """
        return self.datastorage_id

    def get_attribute_id(self):
        """
        Returns the attribute ID.

        Returns:
            int: The attribute ID.
        """
        return self.attribute_id
    
    def get_min(self):
        """
        Returns the minimum.

        Returns:
            any: Minimum of the attribute.
        """
        return self.min
    
    def get_max(self):
        """
        Returns the maximum.

        Returns:
            any: Maximum of the attribute.
        """
        return self.max

class ContainerINDs:
    """
    Manages a collection of Inclusion Dependency (IND) objects, providing functionalities to add and iterate over them.
    """
    def __init__(self):
        """
        Initializes an empty list to store INDs.
        """
        self.items = []

    def add_IND(self, IND_id, UAC_id, child_server_id, child_db_id, child_datastorage_id, child_attributes, 
                hopf_probability=None, iris_probability=None, hybrid_only_name_probability=None, hybrid_probability=None):
        """
        Adds an Inclusion Dependency (IND) with specified probabilities and identifiers.

        Args:
            IND_id (int): Unique identifier for the IND.
            UAC_id (int): Identifier of the associated Unique Attribute Combination (UAC).
            child_server_id (int): Server identifier where the child attributes are located.
            child_db_id (int): Database identifier where the child attributes are located.
            child_datastorage_id (int): Data storage identifier where the child attributes are located.
            child_attributes (list of int): List of attribute identifiers in the IND.
            hopf_probability (Optional[float]): Probability measure from the HoPF algorithm.
            iris_probability (Optional[float]): Probability measure from the IRIS-DS algorithm.
            hybrid_only_name_probability (Optional[float]): Hybrid probability excluding data distribution factors.
            hybrid_probability (Optional[float]): Combined hybrid probability including all factors.
        """   
        IND = InclusionDenpendcy(IND_id, UAC_id, child_server_id, child_db_id, child_datastorage_id, child_attributes, 
                                 hopf_probability, iris_probability, hybrid_only_name_probability, hybrid_probability)
        self.items.append(IND)

    def __iter__(self):
        """
        Function to iterrate the entries.

        Yields:
            IND: The next IND in the collection.
        """
        for item in self.items:
            yield item

    def __len__(self):
        """
        Returns the number of INDs in the collection.

        Returns:
            int: Number of stored INDs.
        """
        return len(self.items)

class InclusionDenpendcy:
    """
    Represents an Inclusion Dependency (IND) linking child attributes to a parent attribute set known as a Unique Attribute Combination (UAC).
    """
    def __init__(self, IND_id, UAC_id, child_server_id, child_db_id, child_datastorage_id, child_attributes, 
                 hopf_probability=None, iris_probability=None, hybrid_only_name_probability=None, hybrid_probability=None):
        """
        Initializes the IND with identifiers, attributes, and associated probability metrics.

        Args:
            IND_id (int): Unique identifier for the IND.
            UAC_id (int): Identifier of the associated UAC.
            child_server_id (int): Server identifier for the child attributes.
            child_db_id (int): Database identifier for the child attributes.
            child_datastorage_id (int): Data storage identifier for the child attributes.
            child_attributes (list of int): List of attribute identifiers that form part of this IND.
            hopf_probability (Optional[float]): Probability score from HoPF evaluation.
            iris_probability (Optional[float]): Probability score from IRIS-DS evaluation.
            hybrid_only_name_probability (Optional[float]): Probability score from a hybrid model excluding data distribution.
            hybrid_probability (Optional[float]): Overall probability score from a hybrid model including all factors.
        """
        self.IND_id = IND_id
        self.UAC_id = UAC_id
        self.child_server_id = child_server_id
        self.child_db_id = child_db_id
        self.child_datastorage_id = child_datastorage_id
        self.child_attributes = child_attributes
        self.hopf_probability = hopf_probability
        self.iris_probability = iris_probability
        self.hybrid_only_name_probability = hybrid_only_name_probability
        self.hybrid_probability = hybrid_probability
        self.UAC = None

    def set_UAC(self, server_id, database_id, datastorage_id, list_attributes, hopf_score=None, iris_score=None):
        """
        Sets the unique attributecombination of the IND.

        Args:
            server_id (int): The ID of the server.
            database_id (int): The ID of the database.
            datastorage_id (int): The ID of the data storage.
            list_attributes (list): List of attribute IDs in the UAC. 
        """
        UAC_id = self.UAC_id
        self.UAC = UAC = UniqueAttributeCombination(UAC_id, server_id, database_id, datastorage_id, list_attributes, hopf_score, iris_score)

    def get_IND_id(self):
        """
        Gets the ID of the IND.

        Returns:
            int: The ID of the IND.
        """
        return self.IND_id

    def get_UAC_id(self):
        """
        Gets the ID of the associated UAC.

        Returns:
            int: The ID of the UAC.
        """
        return self.UAC_id

    def get_child_server_id(self):
        """
        Gets the ID of the child server.

        Returns:
            int: The ID of the child server.
        """
        return self.child_server_id

    def get_child_db_id(self):
        """
        Gets the ID of the child database.

        Returns:
            int: The ID of the child database.
        """
        return self.child_db_id

    def get_child_datastorage_id(self):
        """
        Gets the ID of the child data storage.

        Returns:
            int: The ID of the child data storage.
        """
        return self.child_datastorage_id

    def get_child_attributes(self):
        """
        Gets the list of attribute IDs in the IND.

        Returns:
            list: List of attribute IDs in the IND.
        """
        return self.child_attributes

    def get_hopf_probability(self):
        """
        Geht the HoPF probability.

        Return:
            float: the probability.
        """
        return self.hopf_probability

    def get_iris_probability(self):
        """
        Gets the IRIS-DS probability.

        Return:
            float: the probability.        
        """
        return self.iris_probability
    
    def get_hybrid_only_name_probability(self):
        """
        Gets the Hybrid probability without datadistribution.
        
        Return:
            float: the probability.        
        """
        return self.hybrid_only_name_probability

    def get_hybrid_probability(self):
        """
        Gets the Hybrid probability.
        
        Return:
            float: the probability.
        """
        return self.hybrid_probability

    def get_UAC(self):
        """
        Gets the associated UAC.

        Returns:
            UniqueAttributeCombination: The associated UAC.
        """
        return self.UAC