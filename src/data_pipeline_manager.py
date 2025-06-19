#!/usr/bin/env python
"""
Example usage:
    python pipeline_manager.py              # Runs full pipeline in monthly mode (default)
    python pipeline_manager.py --mode daily # Runs full pipeline in daily mode

Steps included in the pipeline:
    - Combine raw order and transaction CSVs
    - Filter and preprocess data
    - Merge orders and transactions
    - Aggregate summary (monthly or daily)
"""

import sys
import os
import argparse
from config import config
from file_manager import FileManager
from csv_combiner import CSVCombiner
from filter_orders import OrderFilter
from transactions import TransactionProcessor
from merged_orders_transactions import OrderTransactionMerger
from aggregate import MonthlyChargeAdjustmentAggregator, DailyChargeAdjustmentAggregator


class DataPipelineManager:
    """
    Generic pipeline manager to execute a data aggregation pipeline using a pluggable aggregator class.
    Supports running the entire pipeline or specific steps.
    """

    def __init__(self, aggregator_cls):
        """Initialize all pipeline components and prepare file paths and processors."""
        self.fm = FileManager(reset=True)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

        # Define expected fields for CSVs
        expected_order_columns = config["mandatory_order_fields"] + config["additional_order_columns"]
        expected_transaction_columns = config["mandatory_transaction_fields"] + config["additional_transactions_columns"]

        # CSV combination steps
        self.csv_combiner = CSVCombiner(
            input_files=[os.path.join(project_root, fname) for fname in config["order_input_files"]],
            output_file=config["combined_orders_csv"],
            file_manager=self.fm,
            expected_columns=expected_order_columns
        )

        self.transaction_combiner = CSVCombiner(
            input_files=[os.path.join(project_root, fname) for fname in config["transaction_input_files"]],
            output_file=config["combined_transactions_csv"],
            file_manager=self.fm,
            expected_columns=expected_transaction_columns
        )

        # Data filtering and transformation steps
        self.filter_orders = OrderFilter(
            input_file=self.fm.get_path(config["combined_orders_csv"], "csv"),
            file_manager=self.fm
        )

        self.transaction_processor = TransactionProcessor(
            input_file=self.fm.get_path(config["combined_transactions_csv"], "csv"),
            file_manager=self.fm
        )

        self.merger = OrderTransactionMerger(
            file_manager=self.fm,
            filtered_orders_file=self.fm.get_path(config["filtered_orders_json"], "json"),
            transactions_file=self.fm.get_path(config["transactions_json"], "json")
        )

        # Pluggable aggregator (monthly or daily)
        self.aggregator = aggregator_cls(
            file_manager=self.fm,
            input_file=self.fm.get_path(config["matched_orders_json"], "json")
        )

    def run_all(self):
        """
        Execute the full pipeline:
        1. Combine CSVs
        2. Filter orders
        3. Process transactions
        4. Merge orders and transactions
        5. Aggregate results (monthly or daily)
        """
        print("▶️ Combining all raw order CSVs...")
        self.csv_combiner.run()

        print("▶️ Combining all raw transaction CSVs...")
        self.transaction_combiner.run()

        print("▶️ Running Order Filter...")
        self.filter_orders.run()

        print("▶️ Running Transaction Processor...")
        self.transaction_processor.run()

        print("▶️ Merging Orders and Transactions...")
        self.merger.run()

        print("▶️ Aggregating Summary...")
        self.aggregator.run()

        print("🎉 Pipeline complete!")

    def run_selected(self, steps):
        """
        Run a user-defined subset of pipeline steps.
        Accepts a list of step names:
            - combine_orders
            - combine_transactions
            - filter
            - transactions
            - merge
            - aggregate
        """
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


# CLI entry point
if __name__ == "__main__":
    # Set up CLI interface for selecting aggregation mode
    parser = argparse.ArgumentParser(description="Run the data pipeline.")
    parser.add_argument(
        "--mode",
        choices=["monthly", "daily"],
        default="monthly",
        help="Choose aggregation mode: monthly (default) or daily"
    )
    args = parser.parse_args()

    # Use the appropriate aggregator class
    aggregator_cls = MonthlyChargeAdjustmentAggregator if args.mode == "monthly" else DailyChargeAdjustmentAggregator

    # Execute the pipeline
    pipeline = DataPipelineManager(aggregator_cls)
    pipeline.run_all()
