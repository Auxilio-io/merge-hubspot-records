import requests
import json
import pandas as pd

token = "YOUR_TOKEN" # add your private app token here

df = pd.read_csv('records.csv') # add the link to your csv file here

def findDuplicate(object_type,sorts,properties,filter_groups):
    url = f"https://api.hubapi.com/crm/v3/objects/{object_type}/search"

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    payload = json.dumps({
        "filterGroups": filter_groups,
        "properties": properties,
        "sorts": sorts
    })
    
    try:
        response = requests.request("POST", url, headers=headers, data=payload).json()
        return response['results'][0]['id']
        print(response['results'][0]['id'])
    except requests.exceptions.RequestException as e:
      print(f"A Requests error occurred: {e}")
    except Exception as e:
      print(f"An error occurred: {e}")

def merge(object_type,object_id_to_merge,primary_object_id):
    url = f"https://api.hubapi.com/crm/v3/objects/{object_type}/merge"

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    payload = json.dumps({
        "objectIdToMerge": object_id_to_merge,
        "primaryObjectId": primary_object_id
    })
    
    try:
        response = requests.request("POST", url, headers=headers, data=payload).json()
        print(response)
    except requests.exceptions.RequestException as e:
      print(f"A Requests error occurred: {e}")
    except Exception as e:
      print(f"An error occurred: {e}")

for index, row in df.iterrows():
    email = row['Email']
    object_id_to_merge = row['Record ID']

    object_type = "contacts"
    sorts = [
        {
            "propertyName": "createdate",
            "direction": "DESCENDING"
        }
        ]
    properties = ["hs_object_id","email"]
    filter_groups = [{"filters":[{"propertyName":"email","value":email,"operator":"EQ"}]}]

    primary_object_id = findDuplicate(object_type,sorts,properties,filter_groups)
    merge(object_type,object_id_to_merge,primary_object_id)
