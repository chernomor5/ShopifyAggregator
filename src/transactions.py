import csv
import json
from utils import parse_iso_datetime
from config import config

class TransactionProcessor:
    """Simplified processor to extract and split transactions based on payout status.

    This class:
    - Loads a transaction CSV
    - Keeps only the configured columns
    - Converts date and numeric fields
    - Splits into pending and non-pending groups
    - Saves results to CSV and JSON files
    """

    def __init__(self, input_file, file_manager):
        """
        Initialize with input file path and file manager.

        Args:
            input_file (str): Path to the source CSV file.
            file_manager (FileManager): Utility for saving CSV and JSON outputs.
        """
        self.input_file = input_file
        self.fm = file_manager

        # Get columns from config: mandatory + additional
        self.columns_to_keep = list(dict.fromkeys(
            config["mandatory_transaction_fields"] + config["additional_transactions_columns"]
        ))

        # Buckets for split transactions
        self.pending = []
        self.non_pending = []

    def load_and_split(self):
        """
        Load the CSV file, clean and parse values, split by payout status.
        """
        with open(self.input_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Keep only required columns, if present
                filtered = {col: row[col].strip() for col in self.columns_to_keep if col in row}

                try:
                    # Sanitize and normalize types
                    filtered["Transaction Date"] = parse_iso_datetime(filtered["Transaction Date"]).isoformat()
                    filtered["Amount"] = float(filtered["Amount"])
                    filtered["Fee"] = float(filtered["Fee"])
                    filtered["Net"] = float(filtered["Net"])
                except (ValueError, KeyError):
                    continue  # Skip if parsing fails

                # Classify by payout status
                if filtered.get("Payout Status", "").lower() == "pending":
                    self.pending.append(filtered)
                else:
                    self.non_pending.append(filtered)

    def save_outputs(self):
        """
        Save pending and non-pending transactions to output files.
        """
        self.fm.save_csv(self.pending, config["pending_transactions_csv"])
        self.fm.save_json(self.pending, config["pending_transactions_json"])
        self.fm.save_csv(self.non_pending, config["transactions_csv"])
        self.fm.save_json(self.non_pending, config["transactions_json"])

    def run(self):
        """
        Execute the full process: load, split, and write results.
        """
        self.load_and_split()
        self.save_outputs()
        print("✅ Pending and non-pending transactions saved successfully.")
