from typing import List, Any, Optional, Union
from pydantic import BaseModel

"""
Data Transfer Objects for FastAPI 
"""

class ExtractionEntry(BaseModel):
    """
    Model to represent an entry for data extraction.

    Attributes:
        uri (str): The URI of the database to extract from.
        user (str): The username for the database.
        password (str): The password for the database.
    """
    uri: str
    user: str
    password: str

class ExtractionRequest(BaseModel):
    """
    Model to represent a request for data extraction.

    Attributes:
        entries (List[ExtractionEntry]): A list of extraction entries.
    """
    entries: List[ExtractionEntry]

class ExtractionResponse(BaseModel):
    """
    Model to represent a response for an extraction request.

    Attributes:
        job_id (str): The job ID of the extraction request.
    """
    job_id: str

class StatusResponse(BaseModel):
    """
    Model to represent a status response.

    Attributes:
        status (str): The status of the job.
    """
    status: str

class ResultResponse(BaseModel):
    """
    Model to represent a response containing the result schema.

    Attributes:
        json_schema (list[dict[str, Any]]): A list of dictionaries representing the JSON of the result.
    """
    json_schema: list[dict[str, Any]]


"""
Queue items
"""

class ExtractionQueueItem(object):
    """
    Internal queue item for the extraction process.

    Attributes:
        job_id (str): The job ID for the extraction.
        dbs_to_extract (dict): The databases to extract from.
    """
    job_id: str
    dbs_to_extract: dict

    def __init__(self, job_id: str, dbs_to_extract: dict):
        """
        Initialize an ExtractionQueueItem.

        Args:
            job_id (str): The job ID for the extraction.
            dbs_to_extract (dict): The databases to extract from.
        """
        self.job_id = job_id
        self.dbs_to_extract = dbs_to_extract
