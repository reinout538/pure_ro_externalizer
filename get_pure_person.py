import requests
import json
import pandas as pd
import os, sys
import csv
import datetime

from config import PURE_BASE_URL, PURE_CRUD_API_KEY

file_dir = sys.path[0]

def get_pure_person(UUID):

     #get pure record log directory
     response_get_person = requests.get(PURE_BASE_URL+'/ws/api/persons/'+UUID, headers={'Accept': 'application/json', 'Content-Type': 'application/json', 'api-key':PURE_CRUD_API_KEY})
     
     if response_get_person.ok:
          json_pure_person = response_get_person.json()
     else:
          json_pure_person = None

     return json_pure_person, response_get_person.status_code
     

class getPurePerson():
          
     def __init__(self, UUID):
        json_pure_person = get_pure_person(UUID)[0]
        self.status = get_pure_person(UUID)[1]
        if self.status == 200:
             self.json = json_pure_person
             self.uuid = json_pure_person['uuid']
             self.pure_id = json_pure_person['pureId']
             self.default_lname = json_pure_person['name']['lastName']
             self.default_init = json_pure_person['name']['firstName']
             self.orcid = json_pure_person.get('orcid')
             self.visibility = json_pure_person['visibility']['key']
             self.identifiers = json_pure_person.get('identifiers')
             self.affiliations = json_pure_person['staffOrganizationAssociations']
             self.affil_first_dt = self.get_affil_dates(json_pure_person)[0]
             self.affil_last_dt = self.get_affil_dates(json_pure_person)[1]
             self.years_at_vu = self.get_affil_dates(json_pure_person)[2]
                                   
        else:
            self.json = self.uuid = self.pure_id = self.default_lname = self.default_init = self.orcid = self.visibility = self.identifiers = self.affil_first_dt = self.affil_last_dt = None
            self.affiliations = []
               
               
     def get_affil_dates(self, json_pure_person):

          affil_first_dt = datetime.datetime(9999, 12, 31)
          affil_last_dt = datetime.datetime(1900, 1, 1)
          years_at_vu = []
          
          for affil in json_pure_person['staffOrganizationAssociations']:
               affil_start_dt = datetime.datetime.strptime(affil['period']['startDate'][:10], '%Y-%m-%d')
               affil_start_yr = affil_start_dt.year
               if 'endDate' in affil['period']:
                    affil_end_dt = datetime.datetime.strptime(affil['period']['endDate'][:10], '%Y-%m-%d')
                    affil_end_yr = affil_end_dt.year
               else:
                    affil_end_dt = datetime.datetime(9999, 12, 31)
                    affil_end_yr = datetime.datetime.now().year+1

               for i in range (affil_end_yr-affil_start_yr+1):
                    if affil_start_yr + i not in years_at_vu:
                         years_at_vu.append(affil_start_yr + i)
                    else:
                         continue
                            
               if affil_start_dt < affil_first_dt:
                    affil_first_dt = affil_start_dt
               if affil_end_dt > affil_last_dt:
                    affil_last_dt = affil_end_dt

          years_at_vu.sort()
          
          return affil_first_dt, affil_last_dt, years_at_vu
                  
          
#try it out
"""
person_uuids = ["07ef5f2c-a476-49b3-b987-bae12d168867"]

for UUID in person_uuids:

     pure_person = getPurePerson(UUID)

     for key, value in pure_person.__dict__.items():
         print(f"{key}: {value}")
"""
     
