from datetime import date

config = {
    # --------------------
    # Directories
    # --------------------
    "csv_output_dir": "CSV",
    "json_output_dir": "JSON",

    # --------------------
    # Input File Ranges
    # --------------------
    "order_input_files": [
        "orders_export_1.csv",
        "orders_export_2.csv",
        "orders_export_3.csv",
        "orders_export_4.csv",
        "orders_export_5.csv",
        "orders_export_6.csv",
        "orders_export_7.csv",
        "orders_export_8.csv"
    ],
    "transaction_input_files": ["payment_transactions_export_1.csv"],

    # --------------------
    # Output File Names
    # --------------------

    # ➤ Produced by: CSVCombiner (used by: OrderFilter)
    "combined_orders_csv": "all_orders_combined.csv",

    # ➤ Produced by: CSVCombiner (used by: TransactionProcessor)
    "combined_transactions_csv": "all_transactions_combined.csv",

    # ➤ Produced by: OrderFilter (used by: OrderTransactionMerger)
    "filtered_orders_csv": "filtered_orders.csv",
    "filtered_orders_json": "filtered_orders.json",

    # ➤ Produced by: TransactionProcessor (informational output)
    "pending_transactions_csv": "pending_transactions.csv",
    "pending_transactions_json": "pending_transactions.json",

    # ➤ Produced by: TransactionProcessor (used by: OrderTransactionMerger)
    "transactions_csv": "transactions.csv",
    "transactions_json": "transactions.json",

    # ➤ Produced by: OrderTransactionMerger (used by: MonthlyAggregator)
    "matched_orders_csv": "matched_orders.csv",
    "matched_orders_json": "matched_orders.json",

    # ➤ Produced by: OrderTransactionMerger (informational output)
    "unmatched_orders_csv": "unmatched_orders.csv",
    "unmatched_orders_json": "unmatched_orders.json",

    # ➤ Produced by: MonthlyAggregator (final output)
    "aggregated_csv": "aggregated.csv",
    "aggregated_json": "aggregated.json",

    # ➤ Produced by: MonthlyAggregator (intermediate output)
    "intermediate_aggregation_csv": "intermit_aggregation.csv",
    "intermediate_aggregation_json": "intermit_aggregation.json",

    # --------------------
    # Mandatory Fields Required for Processing
    # --------------------
    "mandatory_order_fields": [
        "Name", "Email", "Created at", "Subtotal", "Taxes",
        "Shipping", "Total", "Discount Amount"
    ],
    "mandatory_transaction_fields": [
        "Transaction Date", "Type", "Order", "Card Brand", "Payout Status",
        "Amount", "Fee", "Net"
    ],

    # --------------------
    # Optional Extra Columns Users May Want to Keep
    # --------------------
    "additional_order_columns": [],  # Additional columns to keep from orders
    "additional_transactions_columns": [],  # Additional columns to keep from transactions

    # --------------------
    # Processing Settings
    # --------------------
    "cutoff_date": date(2025, 1, 1),  # Format: YYYY, M, D

    # --------------------
    # Aggregation Output Settings
    # --------------------
    # Defines the column order for the final aggregated report CSV and JSON outputs
    "aggregated_report_column_order": [
        "Month",
        "AmountSum",
        "FeeSum",
        "ShippingSum",
        "TaxesSum",
        "Total",
        "Money to Bank",
        "Count"
    ]
}
