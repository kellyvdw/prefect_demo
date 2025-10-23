from prefect import flow, task
from prefect.assets import materialize 
import random
import csv
import io, urllib.request

input_file = "https://gist.githubusercontent.com/kellyvdw/800e7bf7c06028a0d4e74539834e05a1/raw/22b7a28fb8ac31303a6350702f26bc75c93d5551/example_input.csv"
output_file = "https://gist.githubusercontent.com/kellyvdw/800e7bf7c06028a0d4e74539834e05a1/raw/22b7a28fb8ac31303a6350702f26bc75c93d5551/example_output.csv"

def get_data(filename):
    data_extract = []
    with urllib.request.urlopen(input_file) as resp:
        text = resp.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        data_extract = list(reader)
    return data_extract 


@materialize(input_file)
def extract_input_data() -> str:
    data_extract = get_data(input_file)
    return data_extract
    

@materialize(output_file)
def extract_output_data() -> str:
    data_extract = get_data(output_file)
    return data_extract

@task
def get_customer_ids():
    return extract_input_data()

@task
def process_customer():
    # Process a single customer
    return extract_output_data()

@task
def join_data(data_a, data_b):
    data_c = data_a + data_b
    return data_c

@flow
def main():
    customer_ids = get_customer_ids()
    results = process_customer()
    join_data (customer_ids, results)
    return results



if __name__ == "__main__":
    main.serve(
        name="test-deployment"
        )
