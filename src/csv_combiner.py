import csv
from collections import OrderedDict

class CSVCombiner:
    """Combines multiple CSV files with consistent headers and deduplicates rows.

    This class ensures header consistency, validates required columns, and aggregates
    unique rows from all input files. The result is saved as a clean combined CSV file.
    """
    def __init__(self, input_files, output_file, file_manager, expected_columns=None):
        self.input_files = input_files
        self.output_file = output_file
        self.fm = file_manager
        self.combined_data = []
        self.headers = None
        self.unique_rows = set()
        self.expected_columns = expected_columns or []  # ✅ default to empty list

    def read_and_validate_files(self):
        """Read input CSVs, ensure consistent headers, and deduplicate rows."""
        for path in self.input_files:
            with open(path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)

                # ✅ Store and check consistent headers
                if self.headers is None:
                    self.headers = reader.fieldnames
                elif self.headers != reader.fieldnames:
                    raise ValueError(f"Header mismatch in file: {path}")

                # ✅ Check expected columns are present
                missing = [col for col in self.expected_columns if col not in reader.fieldnames]
                if missing:
                    raise ValueError(f"Missing expected column(s) {missing} in file: {path}")

                for row in reader:
                    row_tuple = tuple(row.get(h, "").strip() for h in self.headers)
                    if row_tuple not in self.unique_rows:
                        self.unique_rows.add(row_tuple)
                        self.combined_data.append(OrderedDict(zip(self.headers, row_tuple)))

    def save_combined_csv(self):
        """Save the combined and validated rows to a CSV file."""
        self.fm.save_csv(self.combined_data, self.output_file)

    def run(self):
        """Run the full CSV combination process: read, validate, and save."""
        print("🔄 Combining input CSV files...")
        self.read_and_validate_files()
        self.save_combined_csv()
        print(f"✅ Combined CSV saved to: {self.output_file}")
