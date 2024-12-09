from services.Containers import ContainerUACs
from scipy import stats


class PrimarykeyFinder:
    """
    Class to calculate the probability that a UAC is a Primarykey.
    """

    def __init__(self, connector, max_value_length, name_suffix):
        """
        Initializes a new instance of the PrimarykeyFinder class.

        Args:
          connector (DBConnector): An instance of DBConnector used for database connections.
          max_value_length (int): Max value to calculate "value length".
          name_suffix (list[str]): List of name suffixes in primarykey name.
        """
        self.connector = connector
        self.max_value_length = max_value_length
        self.name_suffix = name_suffix
        self.containerUACs = ContainerUACs()
        self._load_UACs()
        self.start_calculating()
        self.possibility_calculation()

    def start_calculating(self):
        """
        
        """
        for UAC in self.containerUACs:
            UAC_id = UAC.get_UAC_id()
            list_attribute_ids = UAC.get_attributes()
            # Cardinality
            cardinality = 1 / len(list_attribute_ids)
            # Value length
            sum = 0  
            for attribute_id in list_attribute_ids:
                length = self.connector.get_max_value_length_of_attribute(attribute_id)
                sum += length
            average = sum / len(list_attribute_ids)
            average -= self.max_value_length
            maximum = max(1, average)
            value_length = 1 / maximum
            # Position
            list_positions = []
            for attribut_id in list_attribute_ids:
                actual_postion = self.connector.get_attribute_position(attribut_id)
                list_positions.append(actual_postion)
            first_position = min (list_positions)
            last_position = max(list_positions)
            no_attributes_between = last_position - first_position - (len(list_attribute_ids) - 1)
            # no_attributes_between can be less then 0 if there are arrays involved
            if no_attributes_between < 0:
                no_attributes_between = 0
            position = ((1 / first_position) + (1 / (no_attributes_between + 1))) / 2
            # Name suffix
            count_suffixes = 0
            for attribute_id in list_attribute_ids:
                name = self.connector.get_attribute_name(attribute_id)
                name =  name.lower()
                for suffix in self.name_suffix:
                    check = name.endswith(suffix)
                    if check:
                        count_suffixes += 1
                        break
            name_suffix = count_suffixes / len(list_attribute_ids)
            # Datatype
            datatype = 0
            for attribute_id in list_attribute_ids:
                data_types = self.connector.get_attribute_types(attribute_id)
                if all(item in ("int") for item in data_types):
                    # Contains only integer
                    datatype = 1
                elif all(item in ("str") for item in data_types):
                    # Contains only string
                    datatype = 1
                elif all(item in ("elementId") for item in data_types):
                    # Contains only elementId
                    datatype = 1
                    value_length = 1
                elif all(item in ("DBRef") for item in data_types):
                    # Contains only DBRef
                    datatype = 1
                    value_length = 1
                elif all(item in ("ObjectId") for item in data_types):
                    # Contains only ObjectId
                    datatype = 1
                    value_length = 1
                else:
                    # One attribute in the list contains a type other than string or integer
                    datatype = 0
                    break
            # Write result to UAC
            self.connector.add_UAC_PKscores(UAC_id, cardinality, value_length, position, name_suffix, datatype)

    def possibility_calculation(self):
        """
        Calculates the primarykey possibility for all UACs. Uses the HopF- and IRIS-DS-Calculation.
        """
        PK_metrics_hopf = ["score_cardinality", "score_valuelenght", "score_position", "score_namesuffix"]
        PK_metrics_iris = ["score_cardinality", "score_valuelenght", "score_position", "score_namesuffix", "score_datatype"]
        for UAC in self.containerUACs:
            UAC_id = UAC.get_UAC_id()
            UAC_datastorage_id = UAC.get_datastorage_id()
            # IRIS-Score
            UAC_PK_score_iris = self.connector.calculate_PKscore_for_id(UAC_id, PK_metrics_iris)
            PK_scores_iris = self.connector.calculate_PKscores_for_datastorage(UAC_datastorage_id, PK_metrics_iris)
            PK_scores_max_iris = max(PK_scores_iris)
            plateau_iris = self._detect_first_plateau(PK_scores_iris)
            if UAC_PK_score_iris == PK_scores_max_iris:
                iris_score = 2
            elif UAC_PK_score_iris >= plateau_iris:
                iris_score = 1
            else:
                iris_score = 0
            # HoPF-Score
            UAC_PK_score_hopf = self.connector.calculate_PKscore_for_id(UAC_id, PK_metrics_hopf)
            PK_scores_hopf = self.connector.calculate_PKscores_for_datastorage(UAC_datastorage_id, PK_metrics_hopf)
            PK_scores_max_hopf = max(PK_scores_hopf)
            plateau_hopf = self._detect_first_plateau(PK_scores_hopf)
            if UAC_PK_score_hopf == PK_scores_max_hopf:
                hopf_score = 2
            elif UAC_PK_score_hopf >= plateau_hopf:
                hopf_score = 1
            else:
                hopf_score = 0
            # Write result to UAC
            self.connector.add_UAC_possibility(UAC_id, hopf_score, iris_score)

    def _detect_first_plateau(self, values):
        """
        Detects the the end of the plateau. Uses the Z-score for detection.

        Args:
            values (list[float]): List with the values.
        
        Returns:
            float: Last value of the plateau.        
        """
        values.sort(reverse=True)
        if len(values) == 1: return values[0] # Not enough values
        if len(values) == 2: return values[0] # Not enough values
        differences = []
        for i in range(1, len(values)):
            difference = values[i - 1] - values[i]
            differences.append(difference)
        # Calculate mean and standard deviation
        mean = sum(differences) / len(differences)
        std_deviation = stats.tstd(differences)
        # Calculate Z-Scores
        z_scores = [(abs(x - mean) / std_deviation) for x in differences]
        plateau = -1
        for z_score in z_scores:
            if z_score >= 1:
                position = z_scores.index(z_score)
                plateau = values[position]
                break
        if plateau == -1:
            # There is no plateau
            return self._calculate_cliff(values)
        else:
            return plateau

    def _calculate_cliff(self, values):
        """
        Calculates the cliff (see HoPF-Paper), with is used to decide if a primarykey candidate a mostlikely primarykey.
        
        Args:
            values (list[float]): Sorted list with the values.
        
        Returns:
            float: Value for the last Upper value.
        """
        cliff_value = 0.0
        maximum_difference = 0.0
        if len(values) == 1:
            # If the list contains only one value it returns the value.
            return values[0]
        else:
            for i in range(1, len(values)):
                diff = values[i - 1] - values[i]
                if diff > maximum_difference:
                    cliff_value = values[i - 1]
                    maximum_difference = diff
            return cliff_value

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
            