import os, sys
import requests
import json
import csv
import math
import datetime
import pandas as pd

from config import PURE_BASE_URL, PURE_524_API_KEY, PURE_CRUD_API_KEY, SCOPUS_API_KEY

from get_pure_record import getPure, get_pure
from get_pure_person import getPurePerson

#get input
input_uuids = input('Enter one or more Pure RO UUIDs (comma separated, without spaces) : ')
publ_uuids = input_uuids.split(",")

uuid_author = input('enter pure uuid author:')
uuid_extorg = input('enter pure uuid ext. org. of former affiliation:')
#unknown ext org uuid: 3ca7f9ae-5220-4ef9-a4fe-6dd2ba6f1cef

#JSON bits
ext_org_json = {
    "externalOrganizations": [
        {
            "systemName": "ExternalOrganization",
            "uuid": uuid_extorg
        }
    ]
}

mou_vu_json = {
    "systemName": "Organization",
    "uuid": "971a8f57-d401-4e8b-9b1a-a1b97e46e0ea"
  }

#MAIN

#df log files
df_log = pd.DataFrame(columns=['date','publ_uuid', 'person_uuid', 'get_pure_ro','get_pure_person', 'action'])

#create session log directory
file_dir = sys.path[0]
path_session_add = 0
path_session = os.path.join(file_dir, 'log_files', str(datetime.datetime.now().strftime('%Y-%m-%d')))
while os.path.exists(path_session) == True:
    path_session_add += 1
    path_session = f"{os.path.join(file_dir, 'log_files', str(datetime.datetime.now().strftime('%Y-%m-%d')))}_{str(path_session_add)}"
os.makedirs (path_session)


#loop through list of publication UUIDs
for count, publ_uuid in enumerate(publ_uuids):

    print ('processing: ',publ_uuid, 'record ', count+1, 'of ', len(publ_uuids))
    action_log = ''
    
    #get pure objects
    pure_record = getPure(publ_uuid)
    pure_person = getPurePerson(uuid_author)
    
    #check if publication year before year of first affiliation at VU
    if pure_record.pub_yr_first in pure_person.years_at_vu:
        print ('not outside VU affil - skip record')
        action_log = "skipped - not outside VU-affil"
        df_log.loc[len(df_log.index)] = [datetime.datetime.now(), publ_uuid, uuid_author, pure_record.status, pure_person.status, action_log]   
        continue
    #check if record is validated
    if pure_record.workflow == 'approved':
        print ('already approved - skip record')
        action_log = "skipped - approved record"
        df_log.loc[len(df_log.index)] = [datetime.datetime.now(), publ_uuid, uuid_author, pure_record.status, pure_person.status, action_log]   
        continue
    
    remove_int_orgs = []
    contrib_list = pure_record.contributors
    int_org_list = pure_record.internal_orgs
    ext_org_list = pure_record.external_orgs
    
    #loop through publication contributors
    for index, contributor in enumerate(contrib_list):
        contributor.pop("pureId", None)
        #check if not external person
        if contributor['typeDiscriminator'] == 'ExternalContributorAssociation':
            continue
        #check if author is the one that should be evaluated
        if contributor['person']['uuid'] == uuid_author:
            #add affiliated internal org to removal list
            if 'organizations' in contributor:                
                for int_org in contributor['organizations']:
                    remove_int_orgs.append(int_org['uuid'])
                    
                #remove org section
                contributor.pop("organizations", None)
                #add external organization if none present
                if 'externalOrganizations' not in contributor:
                    #build new contributor record with entry for ext org
                    new_contrib = {}
                    for key, value in contributor.items():
                        new_contrib[key] = value
                        if key == "typeDiscriminator":
                            new_contrib.update(ext_org_json)
                    #replace contributor record
                    contrib_list[index] = new_contrib

                    #create or update ext org list
                    if ext_org_list != None:
                        ext_org_list.append(ext_org_json['externalOrganizations'][0])
                    else:
                        ext_org_list = ext_org_json['externalOrganizations']
                else:
                     pass
            else:
                print ('already external - skip record')
                action_log = "skipped - already external"
                df_log.loc[len(df_log.index)] = [datetime.datetime.now(), publ_uuid, uuid_author, pure_record.status, pure_person.status, action_log]   
                
    
    if action_log == "skipped - already external":
        continue
    
    #remove int orgs removed on the contributor level from top level list
    if remove_int_orgs != []:
        int_org_list = [org for org in int_org_list if org['uuid'] not in remove_int_orgs]

    #create pub dir
    path_pub = os.path.join(path_session, str(publ_uuid))
    os.makedirs (path_pub)
    
    #log json record before updates
    open(os.path.join(path_pub, f"{publ_uuid}_before.json"), 'w').write(json.dumps(pure_record.json, indent = 4))
    
    #UPDATE Pure
    if int_org_list != []:
        contrib_upd_json = json.dumps({"contributors": contrib_list, "organizations": int_org_list, "externalOrganizations": ext_org_list}, indent=4)
        response_put_contrib = requests.put(PURE_BASE_URL+'/ws/api/research-outputs/'+publ_uuid, data = contrib_upd_json, headers={'Accept': 'application/json', 'Content-Type': 'application/json', 'api-key': PURE_CRUD_API_KEY})
    else:
        contrib_upd_json = json.dumps({"contributors": contrib_list, "organizations": int_org_list, "externalOrganizations": ext_org_list, "managingOrganization": mou_vu_json}, indent=4)
        response_put_contrib = requests.put(PURE_BASE_URL+'/ws/api/research-outputs/'+publ_uuid, data = contrib_upd_json, headers={'Accept': 'application/json', 'Content-Type': 'application/json', 'api-key': PURE_CRUD_API_KEY})
        
    print ('status code update: ', response_put_contrib.status_code)
    print ('reason error update: ', response_put_contrib.reason)
    action_log = response_put_contrib.status_code
    df_log.loc[len(df_log.index)] = [datetime.datetime.now(), publ_uuid, uuid_author, pure_record.status, pure_person.status, action_log]   
    
#write operations log
df_log.to_csv(os.path.join(path_session, "operations_log.csv"), encoding='utf-8', index = False)
   
