import sys
import os

# Allow imports of sibling modules inside src/
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from file_manager import FileManager
from csv_combiner import CSVCombiner
from filter_orders import OrderFilter
from transactions import TransactionProcessor
from merged_orders_transactions import OrderTransactionMerger
from aggregate import MonthlyChargeAdjustmentAggregator
from config import config  # ✅ Import shared config

class DataPipelineManager:
    """Coordinates the execution of the entire data processing pipeline.

    This class manages the sequencing of file combination, filtering, transaction
    processing, order merging, and aggregation. It supports both full execution
    and selective step-by-step runs depending on user-specified stages.
    """
    def __init__(self):
        self.fm = FileManager(reset=True)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

        # ✅ Combine mandatory + additional fields
        expected_order_columns = config["mandatory_order_fields"] + config["additional_order_columns"]
        expected_transaction_columns = config["mandatory_transaction_fields"] + config["additional_transactions_columns"]

        # ✅ Combine order CSVs
        self.csv_combiner = CSVCombiner(
            input_files=[os.path.join(project_root, fname) for fname in config["order_input_files"]],
            output_file=config["combined_orders_csv"],
            file_manager=self.fm,
            expected_columns=expected_order_columns
        )

        # ✅ Combine transaction CSVs
        self.transaction_combiner = CSVCombiner(
            input_files=[os.path.join(project_root, fname) for fname in config["transaction_input_files"]],
            output_file=config["combined_transactions_csv"],
            file_manager=self.fm,
            expected_columns=expected_transaction_columns
        )

        # ✅ Use combined order file
        self.filter_orders = OrderFilter(
            input_file=self.fm.get_path(config["combined_orders_csv"], "csv"),
            file_manager=self.fm
        )

        # ✅ Use combined transaction file
        self.transaction_processor = TransactionProcessor(
            input_file=self.fm.get_path(config["combined_transactions_csv"], "csv"),
            file_manager=self.fm
        )

        self.merger = OrderTransactionMerger(
            file_manager=self.fm,
            filtered_orders_file=self.fm.get_path(config["filtered_orders_json"], "json"),
            transactions_file=self.fm.get_path(config["transactions_json"], "json")  # ✅ FIXED
        )

        self.aggregator = MonthlyChargeAdjustmentAggregator(
            file_manager=self.fm,
            input_file=self.fm.get_path(config["matched_orders_json"], "json")
        )

    def run_all(self):
        """Execute the full pipeline: combine, filter, process, merge, and aggregate data."""
        print("▶️ Combining all raw order CSVs...")
        self.csv_combiner.run()

        print("▶️ Combining all raw transaction CSVs...")
        self.transaction_combiner.run()

        print("▶️ Running Order Filter...")
        self.filter_orders.run()

        print("▶️ Running Transaction Filter...")
        self.filter_orders.run()

        print("▶️ Running Transaction Processor...")
        self.transaction_processor.run()

        print("▶️ Merging Orders and Transactions...")
        self.merger.run()

        print("▶️ Aggregating Monthly Summary...")
        self.aggregator.run()

        print("🎉 Pipeline complete!")

    def run_selected(self, steps):
        """Run a selected subset of pipeline steps specified by the user."""
        if "combine_orders" in steps:
            self.csv_combiner.run()
        if "combine_transactions" in steps:
            self.transaction_combiner.run()
        if "filter" in steps:
            self.filter_orders.run()
        if "transactions" in steps:
            self.transaction_processor.run()
        if "merge" in steps:
            self.merger.run()
        if "aggregate" in steps:
            self.aggregator.run()

# Entry point
if __name__ == "__main__":
    pipeline = DataPipelineManager()
    pipeline.run_all()
