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

uuid_extorg = '3ca7f9ae-5220-4ef9-a4fe-6dd2ba6f1cef' #unknown ext org uuid

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
df_log = pd.DataFrame(columns=['date','publ_uuid', 'action', 'update'])

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
    update_log = ''
    
    #get pure publication object
    pure_record = getPure(publ_uuid)
        
    #check if record is validated
    if pure_record.workflow == 'xapproved':
        print ('already approved - skip record')
        action_log = "skipped - approved record"
        df_log.loc[len(df_log.index)] = [datetime.datetime.now(), publ_uuid, action_log, update_log]   
        continue
    
    remove_int_orgs = []
    keep_int_orgs = []
    contrib_list = pure_record.contributors
    int_org_list = pure_record.internal_orgs
    ext_org_list = pure_record.external_orgs
    
    #loop through publication contributors
    for index, contributor in enumerate(contrib_list):

        #remove internal Id (causes error when you feed it back to Pure)
        contributor.pop("pureId", None)
        print (contributor)

        #check if not external person
        if contributor['typeDiscriminator'] in ['ExternalContributorAssociation', 'AuthorCollaborationContributorAssociation']:
            continue
        
        #get pure publication object
        pure_person = getPurePerson(contributor['person']['uuid'])

        #check if publication year within years at VU from person record
        if pure_record.pub_yr_first in pure_person.years_at_vu:
            if 'organizations' in contributor:                
                for int_org in contributor['organizations']:
                    keep_int_orgs.append(int_org['uuid'])
            continue
        else:
            print ("make external: ", pure_person.uuid)
            #make external
            #add affiliated internal org to removal list
            if 'organizations' in contributor:                
                for int_org in contributor['organizations']:
                    remove_int_orgs.append(int_org['uuid'])
   
                #remove org section
                contributor.pop("organizations", None)
                action_log = 'external affil added'
                
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
                continue

    #remove int orgs that should be kept from remove list
    remove_int_orgs = [org for org in remove_int_orgs if org not in keep_int_orgs]
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

    print (contrib_upd_json)
    
    print ('status code update: ', response_put_contrib.status_code)
    print ('status text update: ', response_put_contrib.reason)
    update_log = response_put_contrib.status_code
    df_log.loc[len(df_log.index)] = [datetime.datetime.now(), publ_uuid, action_log, update_log]   
    
#write operations log
df_log.to_csv(os.path.join(path_session, "operations_log.csv"), encoding='utf-8', index = False)
   
