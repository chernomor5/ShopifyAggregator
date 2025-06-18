import csv
from datetime import datetime, date, timedelta
from utils import parse_iso_datetime
from config import config  # ✅ Import shared config

    
class OrderFilter:
    """Filters order data based on cutoff date and required field completeness.

    This class reads order CSV files, verifies mandatory fields and order date,
    and produces a filtered subset of the data for merging with transactions.
    Output is saved in both CSV and JSON formats.
    """
    def __init__(self, input_file, file_manager, cutoff_date=None):
        self.fm = file_manager
        self.input_file = input_file
        self.cutoff_date = cutoff_date or config["cutoff_date"]
        self.filtered_data = []
        self.seen_names = set()

        # ✅ Combine mandatory and additional fields, ensure uniqueness
        all_fields = config["mandatory_order_fields"] + config["additional_order_columns"]
        self.required_columns = list(dict.fromkeys(all_fields))  # Preserve order

        self.output_csv = self.fm.get_path(config["filtered_orders_csv"], "csv")
        self.output_json = self.fm.get_path(config["filtered_orders_json"], "json")

    def process_file(self, file_path):
        """Read orders from CSV, apply filters, and select valid entries based on cutoff date."""
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                name = row.get("Name", "").strip()
                if not name.startswith("#") or name in self.seen_names:
                    continue

                if not all(row.get(field, '').strip() for field in ["Subtotal", "Taxes", "Shipping", "Total"]):
                    continue

                entry = {}
                for col in self.required_columns:
                    val = row.get(col, '').strip()
                    if col == "Created at":
                        try:
                            created_date = parse_iso_datetime(val).date()
                            val = created_date
                        except ValueError:
                            break
                    entry[col] = val
                else:
                    if isinstance(entry["Created at"], date) and entry["Created at"] > (self.cutoff_date - timedelta(days=1)):
                        self.filtered_data.append(entry)
                        self.seen_names.add(name)

    def filter_orders(self):
        """Process and clean the input file, retaining only valid orders based on date and data presence."""
        self.process_file(self.input_file)

        for entry in self.filtered_data:
            if isinstance(entry["Created at"], date):
                entry["Created at"] = entry["Created at"].isoformat()

    def save_outputs(self):
        """Write filtered orders to CSV and JSON outputs."""
        self.fm.save_csv(self.filtered_data, config["filtered_orders_csv"])
        self.fm.save_json(self.filtered_data, config["filtered_orders_json"])

    def run(self):
        self.filter_orders()
        self.save_outputs()
        print(f"✅ Filtered orders saved to:\n- {self.output_csv}\n- {self.output_json}")
