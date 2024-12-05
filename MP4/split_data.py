import csv

def split_csv(input_file, output_files, line_counts):
    with open(input_file, 'r') as infile:
        reader = csv.reader(infile)
        header = next(reader)  # Read the header

        for output_file, line_count in zip(output_files, line_counts):
            with open(output_file, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(header)  # Write the header

                for _ in range(line_count):
                    try:
                        writer.writerow(next(reader))
                    except StopIteration:
                        break  # End of file reached

# Usage
input_file = 'Traffic_Signs_mp4.csv'  # Replace with your input file name
output_files = ['Traffic_Signs_1000.csv', 'Traffic_Signs_5000.csv', 'Traffic_Signs_10000.csv']
line_counts = [1000, 5000, 10000]

split_csv(input_file, output_files, line_counts)
