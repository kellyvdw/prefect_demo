from prefect import flow, task
import random
import csv
import io, urllib.request
import json, os
from prefect.blocks.system import Secret

gist_url = f"https://api.github.com/gists/800e7bf7c06028a0d4e74539834e05a1"
customer_data = "https://gist.githubusercontent.com/kellyvdw/800e7bf7c06028a0d4e74539834e05a1/raw/546e4d49d06d0d1e839080afaaee26247ea4cb5f/customers.csv"
product_data = "https://gist.githubusercontent.com/kellyvdw/800e7bf7c06028a0d4e74539834e05a1/raw/546e4d49d06d0d1e839080afaaee26247ea4cb5f/products.csv"
transaction_data = "https://gist.githubusercontent.com/kellyvdw/800e7bf7c06028a0d4e74539834e05a1/raw/546e4d49d06d0d1e839080afaaee26247ea4cb5f/transactions.csv"
customer_analytics_data = "dollars_by_customer.csv"
product_analytics_data = "dollars_by_product.csv"

def get_data(filename):
    data_extract = []
    with urllib.request.urlopen(filename) as resp:
        data_extract = resp.read().decode("utf-8")
        #data_extract = csv.DictReader(io.StringIO(text))
        #data_extract = list(reader)
    return data_extract 

def write_data(filename, data_out, gist_url):
    github_pat_block = Secret.load("github-pat")
    TOKEN = github_pat_block.get()

    update_payload = {
    "files": {
        filename: {"content": data_out}
        }
    }

    print(update_payload)

    req = urllib.request.Request(
        gist_url,
        data=json.dumps(update_payload).encode("utf-8"),
        headers={
            "Authorization": f"token {TOKEN}",
            "User-Agent": "python-urllib",
            "Content-Type": "application/json"
        },
        method="PATCH"
    )

    with urllib.request.urlopen(req) as resp:
        gist_info = json.load(resp)
        print("Updated raw URL:", gist_info["files"][filename]["raw_url"])
    return
   
@task
def get_customer_data(customer_data):
    data_extract = get_data(customer_data)
    return data_extract

@task
def write_customer_analytics(customer_analytics_data, customer_analytics, gist_url):
    write_data(customer_analytics_data,customer_analytics, gist_url)
    return

@flow
def main():
    customers = get_customer_data(customer_data)
    write_customer_analytics(customer_analytics_data, customers, gist_url )
    return customers



if __name__ == "__main__":
    main.serve(
        name="bakery_analytics_deployment"
        )
