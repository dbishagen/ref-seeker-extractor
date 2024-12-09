from urllib.parse import urlparse

from services.SettingsLoader import SettingsLoader
from services.DBConnector import DBConnector
from services.Importer import ImporterMongoDB
from services.Importer import ImporterPostgreSQL
from services.Importer import ImporterNeo4j
from services.Importer import ImporterCassandra
from services.UniqueAttributecombinations import UACFinder
from services.InclusionDependencies import INDFinder
from services.PrimarykeyFinder import PrimarykeyFinder
from services.ForeignkeyFinder import ForeignkeyFinder
from services.ResultCalculator import ResultCalculator
from services.Exporter import Exporter
from services.Timer import Timer # Function to test the performance


if __name__ == "__main__":

    # Load seetings
    settings_loader = SettingsLoader('settings.yaml')
    sql_type = settings_loader.get_value('database.type')
    sql_host = settings_loader.get_value('database.host')
    sql_port = settings_loader.get_value('database.port')
    sql_user = settings_loader.get_value('database.user')
    sql_password = settings_loader.get_value('database.password')
    sql_database_name = settings_loader.get_value('database.database_name')
    max_UAC_attibutes = settings_loader.get_value('primarykeys.max_UAC_attibutes')
    pk_max_value_length = settings_loader.get_value('primarykeys.max_value_length')
    pk_name_suffix = settings_loader.get_value('primarykeys.name_suffix')
    ind_speed_mode = settings_loader.get_value('inclusion_dependencies.speed_mode')
    find_max_ind = settings_loader.get_value('inclusion_dependencies.find_max_ind')
    pk_metric = settings_loader.get_value('metrics.pk_metric')
    fk_metric = settings_loader.get_value('metrics.fk_metric')
    export_file_path = settings_loader.get_value('dataexport.filepath')

    print("Connect to DB:")
    with Timer():
        DBConnector = DBConnector(sql_type, sql_host, sql_port, sql_user, sql_password, sql_database_name)
    DBConnector.connect()    
    DBConnector.delete_everything()  

    # print("Import MongoDB:")
    # with Timer():
    #     uri = "mongodb://192.168.178.6:27017/Uni-testdb?authSource=admin&directConnection=true"
    #     user = "xxx"
    #     password = "xxx"
    #     ImporterMongoDB = ImporterMongoDB(DBConnector, uri, user, password)

    # print("Import Cassandra:")
    # with Timer():
    #     uri = "cassandra://192.168.178.6:9042/polystore_testdb"
    #     user = None
    #     password = None   
    #     ImporterCassandra = ImporterCassandra(DBConnector, uri, user, password)

    # print("Import PostgreSQL:")
    # with Timer():
    #     uri = "postgresql://192.168.178.6:5432/northwind"
    #     user = "xxx"
    #     password = "xxx"
    #     ImporterPorstgreSQL = ImporterPostgreSQL(DBConnector, uri, user, password)

    # print("Import neo4j:")
    # with Timer():
    #     uri = "neo4j://192.168.178.6:7687"
    #     user = "xxx"
    #     password = "xxx"
    #     ImporterNeo4j = ImporterNeo4j(DBConnector, uri, user, password)

    print("Find UACs:")
    with Timer():   
        UACFinder(DBConnector, max_UAC_attibutes)

    print("Find PKs:")
    with Timer():
        PrimarykeyFinder(DBConnector, pk_max_value_length, pk_name_suffix)

    print("Find INDs:")
    with Timer():
        INDFinder(DBConnector, find_max_ind, ind_speed_mode)

    print("Find Foreignkey:")
    with Timer():
        ForeignkeyFinder(DBConnector)
        
    print("Calculate Results:")
    with Timer():
        ResultCalculator(DBConnector, pk_metric, fk_metric)

    print("Export Results:")
    with Timer():
        Exporter(DBConnector, find_max_ind, export_file_path)

    DBConnector.close()
