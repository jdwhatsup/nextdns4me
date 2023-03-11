import os
import requests
import pandas as pd
from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor

ONE_MINUTE = 60
MAX_CALLS_PER_MINUTE = 10

@sleep_and_retry
@limits(calls=MAX_CALLS_PER_MINUTE, period=ONE_MINUTE)
def add_dns_record(session, data):
    response = session.post(api_url, json={"name": data[0], "content": data[1]}, headers=headers)
    print(response)
    return response
    
@sleep_and_retry
@limits(calls=MAX_CALLS_PER_MINUTE, period=ONE_MINUTE)
def delete_dns_record(session, data):
    response = session.delete(api_url+"/"+data, headers=headers)
    print(response)
    return response

def get_environment_variable(env_name):
    try:
        return os.environ[env_name]
    except KeyError as e:
        print(f"{env_name} environment variable not available. Error message: {str(e)}")
        raise SystemExit(1)

NEXTDNS_CONFIG=get_environment_variable("NEXTDNS_CONFIG")
NEXTDNS_APIKEY=get_environment_variable("NEXTDNS_APIKEY")
DNS4ME_APIKEY=get_environment_variable("DNS4ME_APIKEY")
CUSTOM_RECORDS=get_environment_variable("CUSTOM_RECORDS")

session = requests.Session()
api_url = f"https://api.nextdns.io/profiles/{NEXTDNS_CONFIG}/rewrites"
headers = {"X-Api-Key": f"{NEXTDNS_APIKEY}"}

# perform an api query to get the dns records in json format
nextdns_records = session.get(api_url, headers=headers).json()
nextdns_records_df = pd.DataFrame.from_dict(nextdns_records["data"])

# load the hosts file from a url into a variable
hosts_url = f"https://dns4me.net/api/v2/get_hosts/hosts/{DNS4ME_APIKEY}"
hosts_content = session.get(hosts_url).text
# append custom DNS records, if set
if len(CUSTOM_RECORDS) != 0:
    hosts_content = f"{hosts_content}\n{CUSTOM_RECORDS}"
hosts_lines = hosts_content.strip().split("\n")
dns4me_records = {"data":[{"name": domain_name, "content": ip_address} for ip_address, domain_name in [line.strip().split() for line in hosts_lines]]}
dns4me_df = pd.DataFrame.from_dict(dns4me_records["data"])

# merge the two dataframes
if not nextdns_records_df.empty and not dns4me_df.empty:
    merged = pd.merge(nextdns_records_df[["name","content"]], dns4me_df, how='outer', indicator=True)
    diff_nextdns = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
    diff_dns4me = merged[merged['_merge'] == 'right_only'].drop('_merge', axis=1)

    # perform API query for each row in diff_old dataframe
    with ThreadPoolExecutor() as executor:
        results_old = list(executor.map(lambda row: delete_dns_record(session, nextdns_records_df.loc[nextdns_records_df['name'] == row[0], 'id'].values[0]), diff_nextdns.itertuples(index=False)))

    # perform API query for each row in diff_new dataframe
    with ThreadPoolExecutor() as executor:
        results_new = list(executor.map(lambda row: add_dns_record(session, row), diff_dns4me.itertuples(index=False)))
elif not dns4me_df.empty:
    # perform API query for each row in diff_new dataframe
    with ThreadPoolExecutor() as executor:
        results_new = list(executor.map(lambda row: add_dns_record(session, row), dns4me_df.itertuples(index=False)))
elif dns4me_df.empty:
    print("Retrieved dns4me hosts file is empty, please verify the hosts file URL contents manually")
    raise SystemExit(1)