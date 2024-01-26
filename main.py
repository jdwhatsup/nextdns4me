import json
import os
import ipaddress
import pandas as pd
from ratelimit import limits, sleep_and_retry
from requests import post
from requests import Session
from requests.exceptions import RequestException 
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
def delete_dns_record(session, id_):
    response = session.delete(f"{api_url}/{id_}", headers=headers)
    print(response)
    return response

def update_nextdns_rewrites(current_rewrites_df, target_rewrites_df):
    merged_df = pd.merge(current_rewrites_df, target_rewrites_df, on=["name", "content"], how='outer', indicator=True)
    to_be_deleted = merged_df[merged_df['_merge'] == 'left_only'][['id', 'name']].to_dict('records')
    to_be_added = merged_df[merged_df['_merge'] == 'right_only'][['name', 'content']].to_dict('records')

    # Delete DNS records no longer present
    with ThreadPoolExecutor() as executor:
        delete_results = list(executor.map(lambda record: delete_dns_record(session, record['id']), to_be_deleted))

    # Add new DNS records
    with ThreadPoolExecutor() as executor:
        add_results = list(executor.map(lambda record: add_dns_record(session, (record['name'], record['content'])), to_be_added))

def get_environment_variable(env_name):
    try:
        return os.environ[env_name]
    except KeyError as e:
        print(f"{env_name} environment variable not available. Error message: {str(e)}")
        raise SystemExit(1)

def parse_records(hosts_content):
    records = []
    
    # Split content into lines and process each line
    for line in hosts_content.split("\n"):
        parts = line.strip().split()
        if len(parts) < 2:
            continue  # Skip lines that don't have at least an IP and a domain name

        ip_address, domain_names = parts[0], parts[1:]

        # Validate IP address format
        try:
            ipaddress.ip_address(ip_address)
        except ValueError:
            print(f"Ignoring invalid IP address: {ip_address}")
            continue

        # Add each domain associated with the IP address
        for domain_name in domain_names:
            # Here you could also validate the domain name format if needed
            records.append({"content": ip_address, "name": domain_name})

    return records

def send_discord_notification(webhook_url, message):
    headers = {'Content-Type': 'application/json'}
    data = json.dumps({'content': message})
    
    try:
        response = post(webhook_url, headers=headers, data=data)
        response.raise_for_status()
    except RequestException as e:
        print(f"Error sending notification to Discord: {e}")
        
# Gather environment variables
NEXTDNS4ME_RUN = get_environment_variable("NEXTDNS4ME_RUN")
NEXTDNS_CONFIG = get_environment_variable("NEXTDNS_CONFIG")
NEXTDNS_APIKEY = get_environment_variable("NEXTDNS_APIKEY")
DNS4ME_APIKEY = get_environment_variable("DNS4ME_APIKEY")
CUSTOM_RECORDS = get_environment_variable("CUSTOM_RECORDS")
NEXTDNS4ME_DISCORD_WEBHOOK_URL = get_environment_variable("NEXTDNS4ME_DISCORD_WEBHOOK_URL")

session = Session()
api_url = f"https://api.nextdns.io/profiles/{NEXTDNS_CONFIG}/rewrites"
headers = {"X-Api-Key": NEXTDNS_APIKEY}

# Retrieve the current rewrites from NextDNS
try:
    current_rewrites_response = session.get(api_url, headers=headers)
    current_rewrites_response.raise_for_status()  # This will raise an HTTPError if the HTTP request returned an unsuccessful status code

    current_rewrites = current_rewrites_response.json()["data"]
    current_rewrites_df = pd.DataFrame(current_rewrites)
except RequestException as e:
    error_message = f"Error fetching current rewrites from NextDNS: {e}"
    print(error_message) # Still print the error to standard output or log it
    send_discord_notification(NEXTDNS4ME_DISCORD_WEBHOOK_URL, error_message)
    raise SystemExit(1)

if NEXTDNS4ME_RUN == "internal":
    print("Internal run - fetching dns4me and merging with custom records")

    try:
        dns4me_hosts_url = f"https://dns4me.net/api/v2/get_hosts/hosts/{DNS4ME_APIKEY}"
        dns4me_hosts_response = session.get(dns4me_hosts_url)
        dns4me_hosts_response.raise_for_status()
        dns4me_hosts_content = dns4me_hosts_response.text
        # Append CUSTOM_RECORDS to the dns4me data
        full_hosts_content = f"{dns4me_hosts_content}\n{CUSTOM_RECORDS}".strip()
        records = parse_records(full_hosts_content)
        records_df = pd.DataFrame.from_dict(records)
        update_nextdns_rewrites(current_rewrites_df, records_df)
    except RequestException as e:
        error_message = f"Error fetching or processing dns4me hosts: {e}"
        print(error_message) # Still print the error to standard output or log it
        send_discord_notification(NEXTDNS4ME_DISCORD_WEBHOOK_URL, error_message)
        raise SystemExit(1)

elif NEXTDNS4ME_RUN == "external":
    print("External run - updating NextDNS rewrites with custom records only")
    # Use CUSTOM_RECORDS directly since there's no need to fetch dns4me data
    records = parse_records(CUSTOM_RECORDS)
    records_df = pd.DataFrame.from_dict(records)
    update_nextdns_rewrites(current_rewrites_df, records_df)

else:
    error_message = "Invalid NEXTDNS4ME_RUN value. Please use 'internal' or 'external'."
    print(error_message) # Still print the error to standard output or log it
    send_discord_notification(NEXTDNS4ME_DISCORD_WEBHOOK_URL, error_message)
    raise SystemExit(1)
