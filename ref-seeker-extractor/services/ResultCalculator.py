from services.Containers import ContainerUACs
from services.Containers import ContainerINDs

class ResultCalculator:
    """
    Class to calculate the result from the scores.
    """
    def __init__(self, connector, pk_metric, fk_metric):
        """
        Initializes the ResultCalculator object with the parameters.

        Args:
          connector (DBConnector): An instance of DBConnector used for database connections.
          pk_metric (strings): Name of the primarykey scores to use for the calculation  
          k_metric (strings): Name of the foreignkey scores to use for the calculation  
        """
        self.connector = connector
        self.pk_metric = pk_metric
        self.fk_metric = fk_metric
        self.containerINDs = ContainerINDs()
        self.containerUACs = ContainerUACs()
        self._load_INDS()
        self._load_UACs()
        self.find_references()
        self.find_primarykeys()

    def find_primarykeys(self):
        """
        Function to identify the primarykeys and writes the to the database.
        """
        for UAC in self.containerUACs:
            # Select score
            if self.pk_metric == "pk_score_hopf":
                pk_score = UAC.get_hopf_score()
            elif self.pk_metric == "pk_score_iris":
                pk_score = UAC.get_iris_score()
            # Add reference to database
            if pk_score >= 1:
                UAC_id = UAC.get_UAC_id()
                self.connector.add_primarykey(UAC_id)       
            
    def find_references(self):
        """
        Function to identify the references and writes the to the database.
        """
        # Builds a list with all probabilities
        probability_list = []
        for IND in self.containerINDs:
            # Select probabiity
            if self.fk_metric == "hybrid_probability":
                probability = IND.get_hybrid_probability()
            elif self.fk_metric == "iris_probability":
                probability = IND.get_iris_probability()
            elif self.fk_metric == "hybrid_only_name_probability":
                probability = IND.get_hybrid_only_name_probability()
            elif self.fk_metric == "hopf_probability":
                probability = IND.get_hopf_probability()
            probability_list.append(probability)
        # Selects the references
        target_probability = self._plateau_end(probability_list)
        for IND in self.containerINDs:
            # Select probabiity
            if self.fk_metric == "hybrid_probability":
                probability = IND.get_hybrid_probability()
            elif self.fk_metric == "iris_probability":
                probability = IND.get_iris_probability()
            elif self.fk_metric == "hybrid_only_name_probability":
                probability = IND.get_hybrid_only_name_probability()
            elif self.fk_metric == "hopf_probability":
                probability = IND.get_hopf_probability()
            probability_list.append(probability)
            if probability >= target_probability:
                UAC_id = IND.get_UAC_id()
                IND_id = IND.get_IND_id()
                for UAC in self.containerUACs:
                    # Select pk score
                    if UAC_id == UAC.get_UAC_id():
                        if self.pk_metric == "pk_score_hopf":
                            pk_score = UAC.get_hopf_score()
                        elif self.pk_metric == "pk_score_iris":
                            pk_score = UAC.get_iris_score()
                        break
                # Add reference to database
                if pk_score >= 1:
                    self.connector.add_explicit_reference(UAC_id, IND_id)
                else:
                    self.connector.add_implicitly_reference(UAC_id, IND_id)
            
    def _plateau_end(self, values):
        """
        Function to dectect the end of a plateau.  It uses the differences of the values.

        Args:
            values (list[floats]): The values.

        Returns:
            float: The last value of the plateau.
        """
        values.sort(reverse=True)
        # Check the number of values
        if len(values) <= 4:
            return values[-1]
        differences = [values[i] - values[i + 1] for i in range(len(values) - 1)]
        # Define the size of the moving averages
        n = round(len(differences) / 200) # Moving averages with 0.5% of the values
        if n < 3: n = 3 # Minimum of 3 values for the moving averages
        moving_averages = []
        for i in range(len(differences) - n + 1):
            window = differences[i:i+n]
            average = sum(window) / n
            moving_averages.append(average)
        max_moving_averages = max(moving_averages)
        target_value = max_moving_averages / 2
        position = -1
        for i in range(len(moving_averages)):
            if moving_averages[i] >= target_value:
                position = i + (n - 1)
                break
        target_value = values[position]
        return target_value

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
            hopf_probability = entry["hopf_probability"]
            iris_probability = entry["iris_probability"]
            hybrid_only_name_probability = entry["hybrid_only_name_probability"]
            hybrid_probability = entry["hybrid_probability"]
            self.containerINDs.add_IND(IND_id, UAC_id, server_id, database_id, datastorage_id, attributes, 
                                       hopf_probability, iris_probability, hybrid_only_name_probability, hybrid_probability)

    def _load_UACs(self):
        """
        Loads Unique AttributeCombinations (UACs) from the database.
        """
        list_UACs = self.connector.get_UACs()
        for entry in list_UACs:
            UAC_id = entry["UAC_id"]
            server_id = entry["server_id"]
            database_id = entry["database_id"]
            datastorage_id = entry["datastorage_id"]
            attributes = entry["attributes"]
            hopf_score = entry["hopf_score"]
            iris_score = entry["iris_score"]
            self.containerUACs.add_UAC(UAC_id, server_id, database_id, datastorage_id, attributes, hopf_score, iris_score)
            