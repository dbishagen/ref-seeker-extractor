import yaml

class SettingsLoader:
    """A class for loading settings from a YAML file and retrieving values."""

    def __init__(self, file_path):
        """
        Initializes the SettingsLoader instance.

        Args:
            file_path (str): The path to the YAML file containing settings.
        """
        self.file_path = file_path
        self.settings = self.load_settings()

    def load_settings(self):
        """
        Loads settings from the YAML file.

        Returns:
            dict: Loaded settings.
        """
        with open(self.file_path, 'r') as yaml_file:
            return yaml.safe_load(yaml_file)

    def get_value(self, key_chain):
        """
        Retrieves a value from the settings using a key chain.

        Args:
            key_chain (str): The key chain to access the desired value.

        Returns:
            Any: The retrieved value.
        """
        keys = key_chain.split('.')
        current_level = self.settings

        for key in keys:
            if key in current_level:
                current_level = current_level[key]
            else:
                return None  # Maybe use a default value

        return current_level