

class FileWriter:
    def __init__(self, header, filepath):
        self.filepath = filepath
        self.header = header
        self.content = []

    def write(self):
        self.header = self.header + ",\"in_schema\""
        content = [s + ",\"yes\"" for s in self.content]
        content = "\n".join(content)
        content = self.header + "\n" + content

        try:
            with open(self.filepath, 'w') as file:
                file.write(content)
            print(f"Die Datei '{self.filepath}' wurde erfolgreich erstellt.")
        except Exception as e:
            print(f"Fehler beim Erstellen der Datei: {e}")

class PrimarykeyWriter(FileWriter):
    def __init__(self, filepath):
        header = """\"primarykey_type\",\"primarykey_host\",\"primarykey_port\",\"primarykey_database\",\"primarykey_datastorage\",\"primarykey_attributes\""""
        super().__init__(header, filepath)

    def add_primarykey(self, 
            primarykey_type, primarykey_host, primarykey_port, primarykey_database, primarykey_datastorage, primarykey_attributes):
                
        string = "\"" + primarykey_type + "\",\"" + primarykey_host + "\",\"" + primarykey_port + "\",\"" + primarykey_database + "\",\"" + primarykey_datastorage + "\",\"" + primarykey_attributes + "\""
        self.content.append(string)   

class ForeignkeyWriter(FileWriter):
    def __init__(self, filepath):
        header = """\"primarykey_type\",\"primarykey_host\",\"primarykey_port\",\"primarykey_database\",\"primarykey_datastorage\",\"primarykey_attributes\",\"foreignkey_type\",\"foreignkey_host\",\"foreignkey_port\",\"foreignkey_database\",\"foreignkey_datastorage\",\"foreignkey_attributes\""""
        super().__init__(header, filepath)

    def add_forgeinkey(self, 
            primarykey_type, primarykey_host, primarykey_port, primarykey_database, primarykey_datastorage, primarykey_attributes, 
            foreignkey_type, foreignkey_host, foreignkey_port, foreignkey_database, foreignkey_datastorage, foreignkey_attributes):
        
        string = "\"" + primarykey_type + "\",\"" + primarykey_host + "\",\"" + primarykey_port + "\",\"" + primarykey_database + "\",\"" + primarykey_datastorage + "\",\"" + primarykey_attributes + "\",\"" + foreignkey_type + "\",\"" + foreignkey_host + "\",\"" + foreignkey_port + "\",\"" + foreignkey_database + "\",\"" + foreignkey_datastorage + "\",\"" + foreignkey_attributes +"\""
        self.content.append(string)