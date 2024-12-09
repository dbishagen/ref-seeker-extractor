#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys, os
import ref_seeker_extractor_api as sec
from time import sleep
import json
import argparse


def get_schema():
    server_status = sec.get_status()
    if server_status is None:
        print("Something went wrong with Server")
        sys.exit(1)

    if server_status["status"] != "running":
        print("Server is not running")
        print(f"Server status: {server_status["status"]}")
        sys.exit(1)

    print("Server is running")
    print("Extracting schema")
    job_id = sec.extract()

    if job_id is None:
        print("Something went wrong with schema extraction")
        sys.exit(1)
    
    print("Schema extraction job submitted")
    print(job_id)

    job_status = sec.get_job_status(job_id["job_id"])

    if job_status is None:
        print("Something went wrong with job status")
        sys.exit(1)

    while job_status["status"] == "running":
        print("Job is running...")
        job_status = sec.get_job_status(job_id["job_id"])
        sleep(1)
    
    if job_status["status"] == "finished":
        print("Schema extraction completed successfully")
    else:
        print(f"Schema extraction failed with status: {job_status["status"]}")
        sys.exit(1)
    
    job_result = sec.get_job_result(job_id["job_id"])

    if job_result is None:
        print("Something went wrong with job result")
        sys.exit(1)

    print("Writing schema to file")
    with open("schema.json", "w") as f:
        f.write(json.dumps(job_result, indent=4))
    
    print("All done!")




def print_ind_schema_from_file():
    try:
        with open("schema.json", "r") as f:
            job_result = json.load(f)

        neo4j_internal_id_attributes = ["endNodeElementId", "startNodeElementId", "elementId"]

        print("")
        print("## Primary Keys:")
        for pk in job_result["json_schema"][0]["primarykeys"]:
            print(f"{pk['database_type']}.{pk['datastorage']}.[{pk['attributes']}]")

        print("")
        print("## Implicite References:")
        for imp_ref in job_result["json_schema"][0]["implicite_refences"]:
            if imp_ref["foreignkey_attributes"] in neo4j_internal_id_attributes or imp_ref["primarykey_attributes"] in neo4j_internal_id_attributes:
                continue
            print(f"{imp_ref['foreignkey_database_type']}.{imp_ref['foreignkey_datastorage']}.[{imp_ref['foreignkey_attributes']}] --> {imp_ref['primarykey_database_type']}.{imp_ref['primarykey_datastorage']}.[{imp_ref['primarykey_attributes']}]")

        
        print("")
        print("## Explicite References:")
        for imp_ref in job_result["json_schema"][0]["explicite_refences"]:
            if imp_ref["foreignkey_attributes"] in neo4j_internal_id_attributes or imp_ref["primarykey_attributes"] in neo4j_internal_id_attributes:
                continue
            print(f"{imp_ref['foreignkey_database_type']}.{imp_ref['foreignkey_datastorage']}.[{imp_ref['foreignkey_attributes']}] --> {imp_ref['primarykey_database_type']}.{imp_ref['primarykey_datastorage']}.[{imp_ref['primarykey_attributes']}]")

        if "maximal_inclusion_dependencies" in job_result["json_schema"][0]:
            print("")
            print("## Maximal Inclusion Dependencies:")
            for mid in job_result["json_schema"][0]["maximal_inclusion_dependencies"]:
                splitted = mid["parent_attribute_names"].split(",")
                splitted.extend(mid["child_attribute_names"].split(","))
                skipt = False
                for s in splitted:
                    if s in neo4j_internal_id_attributes:
                        skipt = True
                        break
                if skipt:
                    continue
                print(f"{mid['child_server_type']}.{mid['child_datastorage_name']}.[{mid['child_attribute_names']}] --> {mid['parent_server_type']}.{mid['parent_datastorage_name']}.[{mid['parent_attribute_names']}]")

    except Exception as e:
        print("Error reading schema file")
        print(e)




def main(argv):
    argparser = argparse.ArgumentParser(description="Ref-Seeker Schema Extraction Client")
    argparser.add_argument("-e", "--extract", help="Get schema from server", action="store_true")
    argparser.add_argument("-p", "--print", help="Print schema from file", action="store_true")

    args = argparser.parse_args()

    if len(argv) == 0:
        argparser.print_help()
        sys.exit(0)

    if args.extract:
        get_schema()
    if args.print:
        print_ind_schema_from_file()




if __name__ == '__main__':
    main(sys.argv[1:]) 
