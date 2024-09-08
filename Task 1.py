import csv
import json

def read_spec_file(spec_file_path):
    """Reads the specification file and returns column names, offsets, and encodings."""
    with open(spec_file_path, 'r', encoding='utf-8') as f:
        spec = json.load(f)
    
    column_names = spec["ColumnNames"]
    offsets = [int(offset) for offset in spec["Offsets"]]
    fixed_width_encoding = spec["FixedWidthEncoding"]
    include_header = spec["IncludeHeader"].lower() == 'true'
    delimited_encoding = spec["DelimitedEncoding"]
    
    return column_names, offsets, fixed_width_encoding, include_header, delimited_encoding

def generate_fixed_width_line(data_row, offsets):
    """Generates a single fixed-width formatted line from a list of data fields."""
    fixed_width_line = ""
    for value, length in zip(data_row, offsets):
        if len(value) > length:
            fixed_width_line += value[:length]  # Truncate if too long
        else:
            fixed_width_line += value.ljust(length)  # Pad with spaces if too short
    return fixed_width_line

def write_fixed_width_file(data, output_file_path, offsets, fixed_width_encoding):
    """Writes a list of data rows to a fixed-width file."""
    with open(output_file_path, 'w', encoding=fixed_width_encoding) as f:
        for data_row in data:
            fixed_width_line = generate_fixed_width_line(data_row, offsets)
            f.write(fixed_width_line + '\n')

def parse_fixed_width_line(line, offsets):
    """Parses a single line of a fixed-width file into a list of fields based on the offsets."""
    parsed_line = []
    start = 0
    for length in offsets:
        field = line[start:start + length].strip()
        parsed_line.append(field)
        start += length
    return parsed_line

def parse_fixed_width_file(fixed_width_file_path, offsets, fixed_width_encoding):
    """Parses a fixed-width file into a list of lists, where each sublist represents a line."""
    parsed_lines = []
    with open(fixed_width_file_path, 'r', encoding=fixed_width_encoding) as f:
        for line in f:
            parsed_line = parse_fixed_width_line(line, offsets)
            parsed_lines.append(parsed_line)
    return parsed_lines

def write_to_csv(parsed_lines, output_csv_file_path, column_names, include_header, delimited_encoding):
    """Writes the parsed lines to a CSV file."""
    with open(output_csv_file_path, 'w', newline='', encoding=delimited_encoding) as f:
        writer = csv.writer(f)
        if include_header:
            writer.writerow(column_names)
        writer.writerows(parsed_lines)

def main():
    spec_file_path = 'spec.json'
    output_fixed_width_file_path = 'output.txt'
    output_csv_file_path = 'output.csv'
    
    # Sample data to write to the fixed-width file based on the provided pattern
    data = [
        # Column Names:  ID   ,     Name     ,Code,State,   Description  , Status ,    ID Number ,  Invoice No ,               Address/Desc          ,  Additional Desc
        ["00001", "John Doe    ", "A12", "CA", "Order Processed", "Active", "ID12345678", "INV123456789", "1234 Elm Street, Springfield", "Order Confirmed"],
        ["00002", "Jane Smith  ", "B34", "NY", "Payment Received", "Closed", "ID87654321", "INV987654321", "4321 Oak Avenue, Metropolis ", "Payment Complete"],
        ["00003", "Alice Brown ", "C56", "TX", "Shipped        ", "Shipped", "ID13579246", "INV246813579", "5678 Pine Road, Gotham City  ", "Shipment In Transit"]
    ]
    
    # Read the specifications from the spec file
    column_names, offsets, fixed_width_encoding, include_header, delimited_encoding = read_spec_file(spec_file_path)
    
    # Write the data to a fixed-width file
    write_fixed_width_file(data, output_fixed_width_file_path, offsets, fixed_width_encoding)
    
    # Parse the fixed-width file back into a list of lists
    parsed_lines = parse_fixed_width_file(output_fixed_width_file_path, offsets, fixed_width_encoding)
    
    # Write the parsed data to a CSV file
    write_to_csv(parsed_lines, output_csv_file_path, column_names, include_header, delimited_encoding)

if __name__ == "__main__":
    main()
