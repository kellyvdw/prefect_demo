from prefect import flow, task
from prefect.assets import materialize 
import random
import csv
import io, urllib.request

input_file = "https://gist.githubusercontent.com/kellyvdw/800e7bf7c06028a0d4e74539834e05a1/raw/22b7a28fb8ac31303a6350702f26bc75c93d5551/example_input.csv"
output_file = "https://gist.github.com/kellyvdw/800e7bf7c06028a0d4e74539834e05a1/example_output.csv"


@materialize(input_file)
def extract_data() -> str:
    data_extract = []
    with urllib.request.urlopen(input_file) as resp:
        text = resp.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        data_extract = list(reader)
    return data_extract

@materialize(output_file)
def load_data(data_extract):
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data_extract)
    return

@task
def get_customer_ids() -> list[str]:
    # Fetch customer IDs from a database or API
    return [f"customer{n}" for n in random.choices(range(100), k=50)]

@task
def process_customer(customer_id: str) -> str:
    # Process a single customer
    return f"Processed {customer_id}"

#@flow
#def main() -> list[str]:
#    customer_ids = get_customer_ids()
    # Map the process_customer task across all customer IDs
#    results = process_customer.map(customer_ids)
#    return results

@flow
def main():
    df = extract_data()
    #load_data(df)
    return



if __name__ == "__main__":
    main.serve(
        name="test-deployment"
        )
