from prefect import flow, task
from prefect.assets import materialize 
import random
import pandas as pd

input_file = "example_input.csv"
output_file = "example_output.csv"


@materialize(input_file)
def extract_data() -> str:
    df = pd.read_csv('input_file')
    return df

@materialize(output_file)
def load_data(df):
    df.to_csv(output_file, index = False)
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
    load_data(df)
    return



if __name__ == "__main__":
    main.serve(
        name="test-deployment"
        )
