import google.auth
from typing import Any
from google.auth.transport.requests import AuthorizedSession
import requests
import logging
import re
import google.cloud.logging

# Initialize Cloud Logging
client = google.cloud.logging.Client()
client.setup_logging()

# Define the scope and credentials
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

def trigger_dag(data, context=None):
    
    web_server_url = "https://d503eb15288e44b0961c69cb09b73c7b-dot-us-central1.composer.googleusercontent.com"

    object_name = data['name']
    logging.info('Triggering object Name: {}'.format(object_name))


    # Endpoint for triggering the DAG run
    endpoint = f"api/v1/dags/daily_pms_table_pipeline_v1/dagRuns"
    url = f"{web_server_url}/{endpoint}"

    # Send the request to trigger the DAG
    response = make_composer2_web_server_request(url, method='POST', json={"conf": data})

    # Check the response to confirm whether the request was successful
    logging.info(f"Response status: {response.status_code}")
    logging.info(f"Response body: {response.text}")

    return response


def make_composer2_web_server_request(
    url: str, method: str = "GET", **kwargs: Any
) -> google.auth.transport.Response:
    """
    Make an authenticated request to the Composer 2 (Airflow 2.x) web server.
    """
    # Create an authenticated session with the default credentials
    authed_session = AuthorizedSession(CREDENTIALS)

    # Set the default timeout if not provided
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    # Make the HTTP request
    response = authed_session.request(method, url, **kwargs)

    # Log the response status and body for debugging purposes
    logging.info(f"Response status: {response.status_code}")
    logging.info(f"Response body: {response.text}")
    
    return response
