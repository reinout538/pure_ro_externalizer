import requests
import json
import pandas as pd
import os, sys
import csv

from config import PURE_BASE_URL, PURE_CRUD_API_KEY

file_dir = sys.path[0]

def get_pure(UUID):

     #get pure record log directory
     response_get_pub = requests.get(PURE_BASE_URL+'/ws/api/research-outputs/'+UUID, headers={'Accept': 'application/json', 'Content-Type': 'application/json', 'api-key':PURE_CRUD_API_KEY})
     
     if response_get_pub.ok:
          json_pure_pub = response_get_pub.json()
     else:
          json_pure_pub = None

     return json_pure_pub, response_get_pub.status_code
     

def get_journal_issn(journal_uuid):
    response_get_journal = requests.get(PURE_BASE_URL+'/ws/api/journals/'+journal_uuid, headers={'Accept': 'application/json', 'Content-Type': 'application/json', 'api-key': PURE_CRUD_API_KEY})
    json_pure_journal = response_get_journal.json()
    
    if 'issns' in json_pure_journal:
        issn_journal = json_pure_journal['issns'][0]['issn']
    elif 'additionalSearchableIssns' in json_pure_journal:
        issn_journal = json_pure_journal['additionalSearchableIssns'][0]['issn']
    else:
        issn_journal = None
    
    return issn_journal

class getPure():
          
     def __init__(self, UUID):
        json_pure_pub = get_pure(UUID)[0]
        self.status = get_pure(UUID)[1]
        if self.status == 200:
             self.json = json_pure_pub
             self.uuid = json_pure_pub['uuid']
             self.pure_id = json_pure_pub['pureId']
             self.main_title = json_pure_pub['title']['value']
             self.sub_title = json_pure_pub.get('subTitle', {}).get('value')
             self.type = json_pure_pub['type']['uri']
             self.category = json_pure_pub['category']['uri']
             self.peer_review = json_pure_pub.get('peerReview') 
             self.electr_versions = json_pure_pub.get('electronicVersions')
             self.curr_pub_status = self.get_pub_dt(json_pure_pub)[0]
             self.print_year = self.get_pub_dt(json_pure_pub)[1]
             self.print_month = self.get_pub_dt(json_pure_pub)[2]
             self.print_day = self.get_pub_dt(json_pure_pub)[3]
             self.online_year = self.get_pub_dt(json_pure_pub)[4]
             self.online_month = self.get_pub_dt(json_pure_pub)[5]
             self.online_day = self.get_pub_dt(json_pure_pub)[6]
             self.pub_yr_first = self.get_pub_dt(json_pure_pub)[7]
             #self.contributors = self.get_contrib (json_pure_pub)
             self.contributors = json_pure_pub['contributors']
             self.internal_orgs = json_pure_pub.get('organizations')
             self.external_orgs = json_pure_pub.get('externalOrganizations')
             self.managing_org = json_pure_pub['managingOrganization']['uuid']
             self.doi = self.get_doi(json_pure_pub)[0]
             self.doi_index = self.get_doi(json_pure_pub)[1]
             self.doi_access = self.get_doi(json_pure_pub)[2]
             self.doi_license = self.get_doi(json_pure_pub)[3]
             self.keyw_list = self.get_keyw(json_pure_pub)[0]
             self.class_keyw = self.get_keyw(json_pure_pub)[1]
             self.journal_uuid = json_pure_pub.get('journalAssociation', {}).get('journal', {}).get('uuid')
             if self.journal_uuid != None:
                  self.journal_issn = get_journal_issn(self.journal_uuid)
             else:
                  self.journal_issn = None
             self.workflow = json_pure_pub['workflow']['step']
             self.identifiers = self.get_identifiers (json_pure_pub)[0]
             self.scopus_eid = self.get_identifiers (json_pure_pub)[1]
               
        else:
             self.pure_id = self.type = self.category = self.peer_review = self.electr_versions = self.curr_pub_status = self.print_year = self.print_month = self.print_day = self.online_year = self.online_month = self.online_day = self.managing_org = self.doi = self.doi_index = self.doi_access = self.doi_license = self.keyw_list = self.class_keyw = self.journal_uuid = self.journal_issn = self.workflow = self.identifiers = self.scopus_eid = None     
               
               
     def get_pub_dt(self, json_pure_pub):
          
          print_year = print_month = print_day = online_year = online_month = online_day = None
          pub_year_first = 9999
          
          for pub_status in json_pure_pub["publicationStatuses"]:
               
               #get print date
               if pub_status["publicationStatus"]["uri"] == "/dk/atira/pure/researchoutput/status/published":
                    print_year = pub_status["publicationDate"]["year"]
                    if "month" in pub_status["publicationDate"]:
                         print_month = pub_status["publicationDate"]["month"]
                    else:
                         print_month = None
                    if "day" in pub_status["publicationDate"]:
                         print_day = pub_status["publicationDate"]["day"]
                    else:
                         print_day = None
               #get online date
               if pub_status["publicationStatus"]["uri"] == "/dk/atira/pure/researchoutput/status/epub":
                    online_year = pub_status["publicationDate"]["year"]
                    if "month" in pub_status["publicationDate"]:
                         online_month = pub_status["publicationDate"]["month"]
                    else:
                         online_month = None
                    if "day" in pub_status["publicationDate"]:
                         online_day = pub_status["publicationDate"]["month"]
                    else:
                         online_day = None
                    
               #get current publication status
               if pub_status["current"] == True:
                    pub_stat_curr = pub_status["publicationStatus"]["uri"]
                    pub_dt_curr = pub_status["publicationDate"]
               else:
                    pass

               #get earliest publ year with status and date
               
               if pub_status["publicationDate"]["year"] < pub_year_first:
                    pub_year_first = pub_status["publicationDate"]["year"]

          return pub_stat_curr, print_year, print_month, print_day, online_year, online_month, online_day, pub_year_first

     def get_contrib (self, json_pure_pub):

          pass
         #print (json_pure_pub['contributors'])

     def get_doi(self, json_pure_pub):
          
          doi_select = index_doi_select = doi_access_select = doi_license_select = None
          if 'electronicVersions' in json_pure_pub:
            for electr_version in json_pure_pub['electronicVersions']:
                if 'doi' in electr_version:
                    index_doi = json_pure_pub['electronicVersions'].index(electr_version)
                    doi = electr_version['doi']
                    
                    if '10.' in doi:
                        doi_select = doi[doi.find('10.'):]
                        doi_access_select = electr_version['accessType']['uri']
                        doi_license_select = electr_version.get('licenseType', {}).get('uri')
                        index_doi_select = index_doi
                    else:
                        continue
                    
          return doi_select, index_doi_select, doi_access_select, doi_license_select

              
     def get_keyw(self, json_pure_pub):
          
          classif_keyw = {}
          if 'keywordGroups' in json_pure_pub:
               keyw_list = json_pure_pub['keywordGroups']
               for keyword_group in keyw_list:   
                                  
                    if keyword_group['typeDiscriminator'] == "ClassificationsKeywordGroup":
                         classif_keyw[keyword_group['logicalName']] = {'index':keyw_list.index(keyword_group), 'values':[]}
                  
                         for classification in keyword_group['classifications']:
                             classif_keyw[keyword_group['logicalName']]['values'].append(classification['uri'])
          else: keyw_list = []                        

          return keyw_list, classif_keyw

     def get_identifiers(self, json_pure_pub):

          identifiers = eid = None
          if 'identifiers' in json_pure_pub:
               identifiers = json_pure_pub['identifiers']
               for ext_id in json_pure_pub['identifiers']:
                    if ext_id['idSource'] == 'Scopus':
                         eid = ext_id['value']
                    else:
                         continue

          return identifiers, eid
                    
          

#try it out
"""
publ_uuids = ["63c847c0-5f62-420d-b6ef-d55da7412840", "fb6105ab-5c35-4b01-979a-7ca1a053c7cb", "007f7652-c50a-4a89-bed4-20a91b191e10", "01f044ea-a1e2-432a-919a-e68114455fed", "879f49e6-327a-4925-8b2d-25fc07a035dd", "dfd76440-86a0-454c-a6c1-5bfced62958d", "6a5c96ea-6254-4613-a3e3-241bf1fc713d", "240c9aa4-3789-407c-8140-018362c9db15", "b13a2472-5558-4755-904d-4986d6c2f3dc", "4672d34a-0f08-44c7-b23b-281ffebf4faf", "a50e12e1-326e-4779-a9c0-263ab0a584ed", "987af3f3-ef55-487b-af5f-6ee27ef48b47", "4ba2d349-162f-4fd4-ab9f-94e2380603e5", "2fe848d0-37e9-4a91-acc6-ea0dee6d260f", "c8ebb025-4554-4721-a6b2-7106213f506b", "a9771c8b-0f03-4934-b15c-2ff1a1c8ac66", "06784eec-62d8-4c23-b0f7-7341bfc910d9", "2591aabf-b33b-41ad-9867-dede421e0e25", "e829d1cc-8045-459b-b7eb-3559b5aed432", "9d8a7c43-8e09-4789-8983-ebb50bd52c57", "66490884-d8b3-4bb2-8b65-5c49c5a6693f", "8ac81b3c-a2cc-4b3f-be35-ef31440f5748", "5dec8570-fc36-4fac-b34f-0e0f476c7b63", "f1947839-8f30-4e16-a59c-96ff6942aba9", "3d858e11-98df-484c-bde9-72f736fde9b0", "307e4815-6eed-491f-8c5f-dd0e6e0146a4", "13900743-68ca-4469-93c2-f33a756b79db", "49fa94bf-7f42-47b4-9d6c-663f7b526ee2", "8f31a7eb-188f-474c-b137-90fdae16e8e3", "daacb112-c5f9-45bc-bb64-bc7ad18685b3", "9758d349-625c-4d4d-8a9d-8209df5accd1", "b035258f-4067-4b31-a60f-7635c280dbd2", "93d0adc7-9c7c-4f98-9a2c-532995a5a159", "0a3074aa-96ce-4dfe-b783-4b95e58e9593", "3337c575-44d3-4f72-be5b-a0a065956172", "7007706e-8797-4b3d-b8a2-bf31bf9e842d", "7d27008a-a119-41ff-9d9c-18bd4379312a", "d2f86b5c-08f8-47be-bb9c-55eb5f1e5b2b", "979aa728-2daa-4441-b4c7-0f17772388d2", "4a39eaee-4edb-4f95-a8d1-130184062fbb", "9d49face-1169-4435-915c-a9303a550b5f", "d2b1dc27-81ed-441c-9311-05944acb6351", "63e3a5b4-a0e1-40c3-8254-af60c401924b", "009c4ffb-d409-4ce4-8eb0-844b91dc180f", "c9af03a7-b549-4dfa-a209-21abfd09cf6c", "da818672-6e93-4b72-80ca-b224b557a4dc", "81908980-3c44-4d0e-a3a1-58b782746b7b", "6956bcb7-59ac-4c84-9614-d1e5994a66e9", "bc6d6462-9c36-48e3-88d1-f4fea1460c9e", "09f2ef54-b325-4aaf-8501-dc2782ba4c25", "a18fb287-0126-40ff-b48d-aa3916eeb886", "2d2d9c4e-7ba5-4555-966c-ff65f429f641", "c1d21b2f-5793-4982-b885-bf1577dc3ee7", "dd37a2a3-a99e-41ca-a6c1-5b22a04d6deb", "02466c02-93c5-4b03-b9a9-fd42ca4d041f", "805521e5-cc86-4932-83f6-414191d925b0", "e71b183f-8af2-451b-bb91-cee126ec3d61", "dc1c57c3-8cac-4359-b4af-ea51696c75a2", "27cee517-f376-47b3-bd60-69c5cec8c93e"]
publ_uuids = ["747a281d-f48c-483b-bf64-015e014dee19"]

for UUID in publ_uuids:

     pure = getPure(UUID)
     print (pure.pub_yr_first)
"""

