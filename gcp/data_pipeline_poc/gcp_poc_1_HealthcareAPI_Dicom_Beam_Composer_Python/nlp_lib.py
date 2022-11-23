import json
import google.auth
from google.auth.transport import requests
from google.oauth2 import service_account
from google.api_core import retry
from ratelimit import limits, sleep_and_retry

_base_url = "https://healthcare.googleapis.com/v1/"
_region = "us-central1"
_creadentials, _project_id = google.auth.default()
_json_headers = {"Content-Type": "application/dicom+json;charset=utf-8"}

'''
from dotenv import load_dotenv
# please populate .env file with
GOOGLE_APPLICATION_CREDENTIALS = "<service_key.json>"
load_dotenv()
'''

def get_session():
    session = requests.AuthorizedSession(_creadentials)
    return session

@sleep_and_retry
@limits(calls=100, period=60)
def call_healthcare_nlp(medical_text):
    session = get_session()
    nlp_request = {
        "documentContent": medical_text,
        "licenseVocabularies": ["SNOMEDT_US"]
    }
    resourse_path = f"{_base_url}projects/{_project_id}/locations/{_region}/services/nlp:analyzeEntities"
    response = session.request("POST", resourse_path, headers=_json_headers, data=json.dumps(nlp_request))
    return(response.json())

'''
if __name__ == "__main__":
    medical_text = "Malignant neoplasm of left parotid gland"
    data={}
    data["NLPOutput"] = nlp_output
'''