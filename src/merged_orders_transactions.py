from datetime import datetime
import json
from utils import parse_iso_datetime
from file_manager import FileManager
from config import config


class OrderTransactionMerger:
    """
    Merges filtered orders with charge transactions from a transaction list.
    Keeps all transactions in output, merging earliest match per order.
    Also identifies and saves filtered orders that have no associated transactions.
    """

    def __init__(self, file_manager, filtered_orders_file=None, transactions_file=None):
        """
        Initialize the merger with configured file paths or provided overrides.
        """
        self.fm = file_manager

        self.filtered_orders_file = filtered_orders_file or self.fm.get_path(config["filtered_orders_json"], "json")
        self.transactions_file = transactions_file or self.fm.get_path(config["transactions_json"], "json")

        self.filtered_orders = []
        self.transactions = []
        self.processed_transactions = []
        self.unmatched_orders = []

    def load_json(self, path):
        """
        Load and parse JSON data from a file.
        """
        print(f"📥 Loading file: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def parse_date(self, record):
        """
        Convert 'Transaction Date' field to a datetime object.
        Defaults to datetime.min if not parsable.
        """
        date_str = record.get("Transaction Date")
        if date_str:
            try:
                return parse_iso_datetime(date_str)
            except Exception:
                return datetime.min
        return datetime.min

    def process(self):
        """
        Main logic: go through sorted transactions, match earliest charge per order.
        Keep all other transactions unchanged in the output.
        """
        filtered_dict = {o["Name"]: o for o in self.filtered_orders}
        met_orders = set()

        # Sort all transactions by timestamp ascending
        sorted_transactions = sorted(self.transactions, key=self.parse_date)

        for tx in sorted_transactions:
            order_id = tx.get("Order")
            tx_type = tx.get("Type")

            if (
                tx_type == "charge"
                and order_id not in met_orders
                and order_id in filtered_dict
            ):
                # First eligible charge: merge it
                combined = {**filtered_dict[order_id], **tx}
                self.processed_transactions.append(combined)
                met_orders.add(order_id)
            else:
                # Pass through all other transactions as-is
                self.processed_transactions.append(tx)

    def extract_orders_without_transactions(self):
        """
        Identifies all filtered orders that do not have any associated transaction (of any type).
        Saves them in self.unmatched_orders.
        """
        transaction_order_ids = {tx.get("Order") for tx in self.transactions}
        self.unmatched_orders = [
            order for order in self.filtered_orders if order["Name"] not in transaction_order_ids
        ]
        print(f"🛑 Found {len(self.unmatched_orders)} filtered orders with no transactions.")

    def save_outputs(self):
        """
        Save merged output and unmatched orders to JSON and CSV as defined in config.
        """
        self.fm.save_json(self.processed_transactions, config["matched_orders_json"])
        self.fm.save_csv(self.processed_transactions, config["matched_orders_csv"])

        self.fm.save_json(self.unmatched_orders, config["unmatched_orders_json"])
        self.fm.save_csv(self.unmatched_orders, config["unmatched_orders_csv"])

    def run(self):
        """
        Execute full process: load data, merge logic, extract unmatched orders, and save all outputs.
        """
        self.filtered_orders = self.load_json(self.filtered_orders_file)
        self.transactions = self.load_json(self.transactions_file)

        self.process()
        self.extract_orders_without_transactions()
        self.save_outputs()

        print(f"✅ Processed {len(self.processed_transactions)} transactions.")
        print(f"📤 Saved {len(self.unmatched_orders)} unmatched orders to output files.")
