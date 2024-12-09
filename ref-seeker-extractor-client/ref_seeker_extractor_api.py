#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import requests
import json


data_stores = {
  "entries": [
    {
      "uri": "mongodb://mongodb:27017/unibench",
      "user": "",
      "password": ""
    },
    {
      "uri": "postgresql://postgres:5432/unibench",
      "user": "postgres",
      "password": "root"
    },
    {
      "uri": "neo4j://neo4j:7687",
      "user": "neo4j",
      "password": "neo4jpassword"
    }
  ]
}


def get_status():
    try:
        with requests.Session() as s:
            res = s.get(
                "http://127.0.0.1:8001/status",
                verify=False
            )
            return json.loads(res.text)
    except Exception as e:
        print("ERROR - getting status")
        print(e)
        return None
            

def extract(): 
    try:
        with requests.Session() as s:
            res = s.post(
                "http://127.0.0.1:8001/extract",
                json=data_stores,
                verify=False,
                timeout=360
            )
            return res.json()
    except Exception as e:
        print("ERROR - extracting schema")
        print(e)
        return None


def get_job_status(job_id):
    try:
        with requests.Session() as s:
            res = s.get(
                f"http://127.0.0.1:8001/jobs/{job_id}",
                verify=False
            )
            return res.json()
    except Exception as e:
        print("ERROR - getting job status")
        print(e)
        return None


def get_job_result(job_id):
    try:
        with requests.Session() as s:
            res = s.get(
                f"http://127.0.0.1:8001/jobs/{job_id}/results",
                verify=False
            )
            return res.json()
    except Exception as e:
        print("ERROR - getting job result")
        print(e)
        return None