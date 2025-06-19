import json
import csv
from collections import defaultdict, Counter
from config import config
from utils import parse_iso_datetime, format_datetime


class DailyChargeAdjustmentAggregator:
    """
    Aggregates daily data from matched_orders.json with logic to zero out
    shipping and taxes for grouped charges where total amount <= 0.
    Produces a detailed daily financial summary.
    """

    def __init__(self, file_manager, input_file=None):
        self.fm = file_manager
        self.input_file = input_file or self.fm.get_path(config["matched_orders_json"], "json")

        self.processed_data = []
        self.daily_buckets = defaultdict(list)
        self.aggregated_result = []
        self.taxes_by_state_aggregated = []

        self.output_intermediate_json = self.fm.get_path(config["intermediate_aggregation_json"], "json")
        self.output_intermediate_csv = self.fm.get_path(config["intermediate_aggregation_csv"], "csv")
        self.output_aggregated_json = self.fm.get_path(config["aggregated_json"], "json")
        self.output_aggregated_csv = self.fm.get_path(config["aggregated_csv"], "csv")

    def load_data(self):
        """Load and parse input JSON with datetime conversion."""
        with open(self.input_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        for item in data:
            item["Transaction Date"] = parse_iso_datetime(item["Transaction Date"])
        return data

    def normalize_shipping_and_taxes(self, records):
        """
        If a transaction has a 'Name' field, find all matching 'Order' transactions.
        If their combined Amount <= 0, set Shipping and Taxes = 0 on all charge-type transactions.
        """
        order_map = defaultdict(list)
        for entry in records:
            if "Order" in entry:
                order_map[entry["Order"]].append(entry)

        for entry in records:
            if "Name" in entry:
                related = order_map.get(entry["Name"], [])
                if len(related) > 1:
                    total_amount = sum(float(tx.get("Amount", 0)) for tx in related)
                    if total_amount <= 0:
                        for tx in related:
                            if tx.get("Type") == "charge":
                                tx["Shipping"] = 0
                                tx["Taxes"] = 0
                                pass

    def bucket_by_day(self, data):
        """Group records by day in YYYY-MM-DD format."""
        self.daily_buckets.clear()
        for item in data:
            key = item["Transaction Date"].strftime("%Y-%m-%d")
            self.daily_buckets[key].append(item)

    def finalize_dates(self, records):
        """Convert datetime fields back to string."""
        for item in records:
            item["Transaction Date"] = format_datetime(item["Transaction Date"])

    def aggregate(self):
        """
        Aggregate data by day with:
        - Financial totals
        - Refunds, Discounts, Other transaction types
        - Net, OrderGross, Subtotal
        - All values rounded to 2 decimal places
        """
        for day, records in self.daily_buckets.items():
            summary = {
                "Day": day,
                "Amount": 0.0,
                "Fee": 0.0,
                "Shipping": 0.0,
                "Taxes": 0.0,
                "Total": 0.0,
                "Refund": 0.0,
                "Discount": 0.0,
                "Other": 0.0,
                "Net": 0.0,
                "OrderGross": 0.0,
                "Subtotal": 0.0,
                "Transaction Count": len(records),  # Count of all transactions in the day
                "OtherTypes": [],
                "OtherTypeOrders": []
            }

            other_types = Counter()
            other_orders = set()

            for r in records:
                tx_type = r.get("Type", "").lower()
                amount = float(r.get("Amount", 0))
                fee = float(r.get("Fee", 0))
                shipping = float(r.get("Shipping", 0))
                taxes = float(r.get("Taxes", 0))
                discount = float(r.get("Discount Amount", 0))
                net = float(r.get("Net", 0))
                subtotal = float(r.get("Subtotal", 0))
                order_total = float(r.get("Total", 0))

                summary["Amount"] += amount
                summary["Fee"] += fee
                summary["Shipping"] += shipping
                summary["Taxes"] += taxes
                summary["Discount"] += discount
                summary["Net"] += net
                summary["Subtotal"] += subtotal
                summary["OrderGross"] += order_total

                if tx_type == "refund":
                    summary["Refund"] += amount
                elif tx_type not in ("refund", "charge"):
                    summary["Other"] += amount
                    other_types[tx_type] += 1
                    if "Order" in r:
                        other_orders.add(r["Order"])

            taxes_by_state = self.state_tax_aggregator(records)
            sorted_taxes_by_state = {key: taxes_by_state[key] for key in sorted(taxes_by_state.keys())}
            sorted_taxes_by_state["Day"] = day
            self.taxes_by_state_aggregated.append(sorted_taxes_by_state)

            summary["Total"] = (
                summary["Amount"]
                - summary["Fee"]
                - summary["Shipping"]
                - summary["Taxes"]
            )

            for key in [
                "Amount", "Fee", "Shipping", "Taxes", "Total", "Refund",
                "Discount", "Other", "Net", "OrderGross", "Subtotal"
            ]:
                summary[key] = round(summary[key], 2)

            summary["OtherTypes"] = list(other_types.keys())
            summary["OtherTypeOrders"] = sorted(other_orders)
            self.aggregated_result.append(summary)

    def state_tax_aggregator(self, merged_transactions):
        # Initialize empty dictionary for aggregated state taxes
        aggregated_state_tax = {}
        
        # Iterate through each item in merged_transactions
        for item in merged_transactions:
            # Check if item has "Tax Breakdown" key
            if "Tax Breakdown" in item and "Taxes" in item and float(item["Taxes"]) > 0:
                # Iterate through each key-value pair in Tax Breakdown
                for state, tax_amount in item["Tax Breakdown"].items():
                    # If state not in aggregated_state_tax, add it
                    if state not in aggregated_state_tax:
                        aggregated_state_tax[state] = float(tax_amount)
                    # If state exists, sum the tax amount
                    else:
                        aggregated_state_tax[state] = aggregated_state_tax[state] + float(tax_amount)
        
        for item in aggregated_state_tax:
            aggregated_state_tax[item] = round(aggregated_state_tax[item], 2)
        # Return the aggregated dictionary
        return aggregated_state_tax

    def save_outputs(self):
        """Save normalized and aggregated outputs to configured paths."""
        self.fm.save_json(self.processed_data, config["intermediate_aggregation_json"])
        self.fm.save_csv(self.processed_data, config["intermediate_aggregation_csv"])
        self.fm.save_json(self.aggregated_result, config["aggregated_json"], copy_to_root=True)
        self.fm.save_csv(self.aggregated_result, config["aggregated_csv"], copy_to_root=True)
        self.fm.save_json(self.taxes_by_state_aggregated, config["tax_by_state_aggregation_json"], copy_to_root=True)
        self.fm.save_csv(self.taxes_by_state_aggregated, config["tax_by_state_aggregation_csv"], copy_to_root=True)        

    def run(self):
        """Full daily ETL run."""
        data = self.load_data()
        self.normalize_shipping_and_taxes(data)
        self.bucket_by_day(data)
        self.finalize_dates(data)
        self.processed_data = data
        self.aggregate()
        self.save_outputs()
        print("✅ DailyChargeAdjustmentAggregator completed.")
