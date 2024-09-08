import csv
import hashlib
import faker
import dask.dataframe as dd

def generate_csv(file_path, num_rows):
    """Generates a CSV file with random data."""
    fake = faker.Faker()
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['first_name', 'last_name', 'address', 'date_of_birth'])
        for _ in range(num_rows):
            writer.writerow([fake.first_name(), fake.last_name(), fake.address(), fake.date_of_birth()])

def anonymize(value):
    """Anonymizes a given value using SHA-256 hashing."""
    return hashlib.sha256(value.encode('utf-8')).hexdigest()

def anonymize_csv(input_file_path, output_file_path):
    """Anonymizes specific columns in a CSV file."""
    with open(input_file_path, 'r', encoding='utf-8') as infile, \
         open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in reader:
            row['first_name'] = anonymize(row['first_name'])
            row['last_name'] = anonymize(row['last_name'])
            row['address'] = anonymize(row['address'])
            writer.writerow(row)

def anonymize_dask_csv(input_file_path, output_file_path):
    """Anonymizes specific columns in a large CSV file using Dask for parallel processing."""
    df = dd.read_csv(input_file_path)

    df['first_name'] = df['first_name'].apply(anonymize, meta=('first_name', 'object'))
    df['last_name'] = df['last_name'].apply(anonymize, meta=('last_name', 'object'))
    df['address'] = df['address'].apply(anonymize, meta=('address', 'object'))
    
    df.to_csv(output_file_path, single_file=True, index=False)

def main():
    # Step 1: Generate a sample CSV file with random data
    generate_csv('sample_data.csv', 1000000)  # Generates a CSV with 1 million rows
    
    # Step 2: Anonymize the data in the CSV file (suitable for smaller datasets)
    anonymize_csv('sample_data.csv', 'anonymized_data.csv')
    
    # Step 3: Anonymize the data in a large CSV file using Dask (suitable for larger datasets)
    anonymize_dask_csv('sample_data.csv', 'anonymized_data_large.csv')

if __name__ == "__main__":
    main()
