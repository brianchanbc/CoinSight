
import os

# Miscellaneous script to iterate folder of crypto files to get filenames used for web server dropdown select options

# Directory containing the CSV files
csv_directory = '#' # Intentionally left blank for now

# Output HTML file
output_html = '#' # Intentionally left blank for now

# Get list of CSV files in the directory
csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]

# Extract pair names from filenames (remove .csv extension)
pairs = [os.path.splitext(f)[0] for f in csv_files]

# Generate HTML options
options_html = '\n'.join([f'<option value="{pair}">{pair}</option>' for pair in sorted(pairs)])

# Write options to the output HTML file
with open(output_html, 'w') as f:
    f.write(options_html)

print(f'Options written to {output_html}')