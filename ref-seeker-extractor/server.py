from urllib.parse import urlparse
import os
import time
import queue
import threading
import json
import time
from typing import Any
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import traceback

from dtos_and_models import (ExtractionRequest, ExtractionResponse, ResultResponse,
                  StatusResponse, ExtractionQueueItem)

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

# Constants
JOB_ID_NOT_FOUND_TEXT = "unknown job id"
STATE_WAITING = "waiting"
STATE_RUNNING = "running"
STATE_FINISHED = "finished"
STATE_ABORTED = "aborted"
# Variables
extraction_request_states: dict[str, str] = {}
extraction_queue = queue.Queue()

# Server

app = FastAPI(title="HTTP Interaction Service for RefSeeker",
              version="1.0.0")

@app.on_event("startup")
async def startup_event():
    """Start the extraction worker to handle the extraction_queue."""
    threading.Thread(target=extraction_worker, daemon=True).start()

@app.on_event("shutdown")
def shutdown_event():
    """Ensure that the execution queue is empty before the shutdown."""
    extraction_queue.join()

@app.get("/status", summary="returns the service status")
async def read_status() -> StatusResponse:
    """Return the current status of the service."""
    return StatusResponse(status="running")

@app.post("/extract", summary="starts the schema extraction process")
async def start_schema_extraction(req_in: ExtractionRequest) -> ExtractionResponse:
    """Start the extraction process and return the job ID.
    
    Args:
        req_in (ExtractionRequest): The extraction request containing the entries.
    
    Returns:
        ExtractionResponse: The response containing the job ID.
    """    
    entries_array = [entry.dict() for entry in req_in.entries] # Converts ExtractionRequest to a dictionary
    uri = entries_array[0]["uri"]
    job_id = generate_job_id(uri)
    queue_request = ExtractionQueueItem(
        job_id, entries_array)
    update_request_state(job_id, STATE_WAITING)
    extraction_queue.put(queue_request)
    return ExtractionResponse(job_id=job_id)

@app.get("/jobs/{job_id}", summary="read job status", response_model=StatusResponse, responses={404: {"model": StatusResponse}})
async def read_job_status(job_id: str):
    """Read the status of a specific job by job ID.
    
    Args:
        job_id (str): The job ID to query.
    
    Returns:
        StatusResponse: The status of the job.
    """
    state = extraction_request_states.get(job_id, JOB_ID_NOT_FOUND_TEXT)
    if state == JOB_ID_NOT_FOUND_TEXT:
        return JSONResponse(status_code=404, content=jsonable_encoder(StatusResponse(status=state)))
    return StatusResponse(status=state)

@app.get("/jobs/{job_id}/results", summary="read job results", response_model=ResultResponse, responses={409: {"model": StatusResponse}})
async def read_job_results(job_id: str):
    """Read the results of a specific job by job ID.
    
    Args:
        job_id (str): The job ID to query.
    
    Returns:
        ResultResponse: The results of the job.
    """
    state = extraction_request_states.get(job_id, JOB_ID_NOT_FOUND_TEXT)
    if state != STATE_FINISHED:
        return JSONResponse(status_code=409, content=jsonable_encoder(StatusResponse(status=state)))
    return ResultResponse(json_schema=get_results_for_job(job_id))

# Worker for extraction
def extraction_worker():
    """This function is executed in a separate thread and works on the request queue."""
    while True:
        print("Worker is running")
        item: ExtractionQueueItem = extraction_queue.get()
        update_request_state(item.job_id, STATE_RUNNING)
        extract_schema(item)
        extraction_queue.task_done()

# Functions

def generate_job_id(uri: str):
    """Generate a somewhat unique job ID which is safe to be used as a directory name.
    
    Args:
        uri (str): The URI to generate the job ID from.
    
    Returns:
        str: The generated job ID.
    """
    alnum_chars = "".join(x for x in uri if x.isalnum())
    return f"job_{int(time.time())}_{alnum_chars}"

def update_request_state(job_id: str, new_state: str):
    """Log and change the extraction request state.
    
    Args:
        job_id (str): The job ID.
        new_state (str): The new state of the job.
    """
    print(f"job '{job_id}' is now {new_state}")
    extraction_request_states[job_id] = new_state

def extract_schema(req: ExtractionQueueItem):
    """Create and start the extraction for a queue item.
    
    Args:
        req (ExtractionQueueItem): The extraction queue item.
    """
    try:
        dbs_to_extract = req.dbs_to_extract
        job_id = req.job_id
        results_dir_path = get_results_dir_path(job_id)
        start_analysing(dbs_to_extract, results_dir_path)
        update_request_state(req.job_id, STATE_FINISHED)
    except Exception as e:
        update_request_state(req.job_id, STATE_ABORTED)
        print(f"{req.job_id}: {e}")
        # print traceback
        print(traceback.format_exc())

def get_results_dir_path(job_id: str) -> str:
    """Return a valid system path to the job results.
    
    Args:
        job_id (str): The job ID.
    
    Returns:
        str: The system path to the job results.
    """
    file_name = job_id + ".json"
    return os.path.join(".", "results", file_name)

def get_results_for_job(job_id: str) -> list[dict[str, Any]]:
    """Search for the results of the job and returns result file.
    
    Args:
        job_id (str): The job ID.
    
    Returns:
        list[dict[str, Any]]: The merged results of the job.
    """
    results_dir_path = get_results_dir_path(job_id)
    all_results = []
    with open(results_dir_path) as f:
        all_results.append(json.loads(f.read()))    
    return all_results

def start_analysing(databases_to_import, results_dir_path):
    """Start analysis.
    
    Args:
        databases_to_import (list): The databases to import.
        results_dir_path (str): The path to store the results.
    """
    # Start time measurement
    start_time = time.time()

    runtime_metrics = {
        "overall_time": -1,
        "time_load_settings": -1,
        "time_import_mongodb": -1,
        "time_import_cassandra": -1,
        "time_import_postgresql": -1,
        "time_import_neo4j": -1,
        "time_data_import": -1,
        "time_UACFinder": -1,
        "time_PKFinder": -1,
        "time_INDFinder": -1,
        "time_FKFinder": -1,
        "time_ResultCalculator": -1
    }

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

    # Connect to database
    dbConnector = DBConnector(sql_type, sql_host, sql_port, sql_user, sql_password, sql_database_name)
    dbConnector.connect()
    dbConnector.delete_everything()

    runtime_metrics["time_load_settings"] = time.time() - start_time

    start_time_import = time.time()
    # Import Data
    for db in databases_to_import:
        uri = db["uri"]
        user = db["user"]
        password = db["password"]
        parser = urlparse(uri)
        server_type = parser.scheme
        if "mongodb" in server_type:
            start_time_import_mongodb = time.time()
            ImporterMongoDB(dbConnector, uri, user, password)
            # get time at the end of the import
            runtime_metrics["time_import_mongodb"] = time.time() - start_time_import_mongodb
        elif "cassandra" in server_type:
            list_uris = [uri]
            start_time_import_cassandra = time.time()
            ImporterCassandra(dbConnector, list_uris, user, password)
            # get time at the end of the import
            runtime_metrics["time_import_cassandra"] = time.time() - start_time_import_cassandra
        elif "postgresql" in server_type:
            start_time_import_postgresql = time.time()
            ImporterPostgreSQL(dbConnector, uri, user, password)
            # get time at the end of the import
            runtime_metrics["time_import_postgresql"] = time.time() - start_time_import_postgresql
        elif "neo4j" in server_type:
            start_time_import_neo4j = time.time()
            ImporterNeo4j(dbConnector, uri, user, password)
            # get time at the end of the import
            runtime_metrics["time_import_neo4j"] = time.time() - start_time_import_neo4j
        else:
            continue
    
    runtime_metrics["time_data_import"] = time.time() - start_time_import
    





    # NOTE: comment out if only max INDs are needed
    start_time_UACFinder = time.time()
    UACFinder(dbConnector, max_UAC_attibutes)
    runtime_metrics["time_UACFinder"] = time.time() - start_time_UACFinder

    start_time_PKFinder = time.time()
    PrimarykeyFinder(dbConnector, pk_max_value_length, pk_name_suffix)
    runtime_metrics["time_PKFinder"] = time.time() - start_time_PKFinder






    # NOTE: comment out if only max INDs are needed
    ind_finder = INDFinder(dbConnector, find_max_ind, ind_speed_mode)
    ind_finder_time_metrics = ind_finder.find_inds()
    runtime_metrics["time_INDFinder"] = ind_finder_time_metrics






    start_time_FKFinder = time.time()
    ForeignkeyFinder(dbConnector)
    runtime_metrics["time_FKFinder"] = time.time() - start_time_FKFinder

    start_time_ResultCalculator = time.time()
    ResultCalculator(dbConnector, pk_metric, fk_metric)
    runtime_metrics["time_ResultCalculator"] = time.time() - start_time_ResultCalculator






    runtime_metrics["overall_time"] = time.time() - start_time

    Exporter(dbConnector, find_max_ind, results_dir_path, runtime_metrics)

    # Disconnect from database
    dbConnector.close()


