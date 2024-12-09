from extractor_postgresql import ExtractorPostgresql
from extractor_mongodb import ExtractorMongodb
from filewriter import PrimarykeyWriter
from filewriter import ForeignkeyWriter
import pandas as pd
import csv
import mariadb

def export_tables_and_views_to_csv(host, user, password, database, tables_or_views, output_directory="./"):
    # Verbindung zur Datenbank herstellen
    try:
        conn = mariadb.connect(
            user=user,
            password=password,
            host=host,
            database=database
        )
    except mariadb.Error as e:
        print(f"Fehler bei der Verbindung zur MariaDB: {e}")
        return

    cur = conn.cursor()

    try:
        # Exportieren der Tabellen
        for table in tables_or_views:
            table_name = table["table"]
            file_name = table["file"]
            cur.execute(f"SELECT * FROM `{table_name}`")
            result = cur.fetchall()
            # CSV-Datei für die Tabelle erstellen und schreiben
            with open(f"{output_directory}{file_name}.csv", "w", newline="") as csvfile:
                csv_writer = csv.writer(csvfile)
                # Überschriften schreiben
                csv_writer.writerow([i[0] for i in cur.description])
                # Daten schreiben
                csv_writer.writerows(result)


    except mariadb.Error as e:
        print(f"Fehler beim Exportieren der Daten: {e}")
    finally:
        # Verbindung schließen
        cur.close()
        conn.close()

def merge_csv(file1, file2, output_file):    
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    common_columns = list(set(df1.columns).intersection(df2.columns))
    
    merged_df = pd.merge(df1, df2, how="outer", on=common_columns)
    merged_df.to_csv(output_file, index=False)

def export(database_name, database_type):
    # Lade Infos aus Schema
    primarykey_path = f"exports\\{database_name}_primarykeys_schema.csv"
    foreignkey_path = f"exports\\{database_name}_foreignkeys_schema.csv"
    primarykey_writer = PrimarykeyWriter(primarykey_path)
    foreignkey_writer = ForeignkeyWriter(foreignkey_path)
    if database_type == "PostgreSQL":
        extractor = ExtractorPostgresql(primarykey_writer, foreignkey_writer, "192.168.178.6", "5432", "postgres", "taurec", database_name)
    elif database_type == "MongoDB":
        extractor = ExtractorMongodb(primarykey_writer, foreignkey_writer, "192.168.178.6", "27017", "root", "password", database_name)
    extractor.extract()

    # Lade Tables und Views von Maria DB
    tables_or_views = [
        {"table":"unique_attributecombinations", "file":"UACs"}, 
        {"table":"view_primarykeys", "file":"primarykeys_RefSeeker"}, 
        {"table":"view_inclusionsdependencies", "file":"INDs"}, 
        {"table":"view_implicitly_reference", "file":"ImplicitlyReferences"}, 
        {"table":"view_explicite_references", "file":"foreignkeys_RefSeeker"}]
    output_directory = f"exports\\{database_name}_"
    export_tables_and_views_to_csv("192.168.178.6", "RefSeeker", "Bczjjbd9n7gbxC5b", "RefSeeker", tables_or_views, output_directory)

    # Merge files
    file1 = f"exports\\{database_name}_primarykeys_schema.csv"
    file2 = f"exports\\{database_name}_primarykeys_RefSeeker.csv"
    output_file = f"exports\\{database_name}_primarykeys_merge.csv"
    merge_csv(file1, file2, output_file)
    file1 = f"exports\\{database_name}_foreignkeys_schema.csv"
    file2 = f"exports\\{database_name}_foreignkeys_RefSeeker.csv"
    output_file = f"exports\\{database_name}_foreignkeys_merge.csv"
    merge_csv(file1, file2, output_file)


if __name__ == "__main__":
    # database_name = "adventureworks"
    # database_name = "sakila"
    # database_name = "northwind"
    # database_type = "PostgreSQL"


    # database_name = "Fachpraktikum"
    database_name = "Uni-testdb"
    database_type = "MongoDB"

    export(database_name, database_type)
    





