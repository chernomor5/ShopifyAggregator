import json
from collections import defaultdict, Counter
from config import config
from utils import parse_iso_datetime, format_datetime


class ChargeAdjustmentAggregatorBase:
    """
    Abstract base class for charge adjustment aggregators.
    Handles common ETL steps and aggregation logic.
    Subclasses should implement specific time-based grouping and summary logic.
    """

    def __init__(self, file_manager, input_file=None):
        """Initialize file paths, buckets, and results."""
        self.fm = file_manager
        self.input_file = input_file or self.fm.get_path(config["matched_orders_json"], "json")

        self.processed_data = []
        self.buckets = defaultdict(list)
        self.aggregated_result = []
        self.taxes_by_state_aggregated = []

        self.output_intermediate_json = self.fm.get_path(config["intermediate_aggregation_json"], "json")
        self.output_intermediate_csv = self.fm.get_path(config["intermediate_aggregation_csv"], "csv")
        self.output_aggregated_json = self.fm.get_path(config["aggregated_json"], "json")
        self.output_aggregated_csv = self.fm.get_path(config["aggregated_csv"], "csv")

    def load_data(self):
        """Load and parse input JSON file with datetime conversion."""
        with open(self.input_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        for item in data:
            item["Transaction Date"] = parse_iso_datetime(item["Transaction Date"])
        return data

    def normalize_shipping_and_taxes(self, records):
        """
        Normalize transactions by setting Shipping and Taxes to 0 if grouped charges total <= 0.
        Applies only to related charge-type transactions.
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

    def finalize_dates(self, records):
        """Convert datetime objects back to formatted strings."""
        for item in records:
            item["Transaction Date"] = format_datetime(item["Transaction Date"])

    def state_tax_aggregator(self, merged_transactions):
        """Aggregate tax breakdowns by U.S. state from individual transaction records."""
        aggregated_state_tax = {}
        for item in merged_transactions:
            if "Tax Breakdown" in item and "Taxes" in item and float(item["Taxes"]) > 0:
                for state, tax_amount in item["Tax Breakdown"].items():
                    aggregated_state_tax[state] = aggregated_state_tax.get(state, 0) + float(tax_amount)
        return {k: round(v, 2) for k, v in aggregated_state_tax.items()}

    def save_outputs(self):
        """Save processed and aggregated data to configured output paths."""
        self.fm.save_json(self.processed_data, config["intermediate_aggregation_json"])
        self.fm.save_csv(self.processed_data, config["intermediate_aggregation_csv"])
        self.fm.save_json(self.aggregated_result, config["aggregated_json"], copy_to_root=True)
        self.fm.save_csv(self.aggregated_result, config["aggregated_csv"], copy_to_root=True)
        self.fm.save_json(self.taxes_by_state_aggregated, config["tax_by_state_aggregation_json"], copy_to_root=True)
        self.fm.save_csv(self.taxes_by_state_aggregated, config["tax_by_state_aggregation_csv"], copy_to_root=True)

    def bucket_key(self, item):
        """Abstract method: extract the grouping key (e.g. by month or day)."""
        raise NotImplementedError

    def summarize_record_group(self, key, records):
        """Abstract method: summarize a group of records by the key."""
        raise NotImplementedError

    def bucket_data(self, data):
        """Group data into buckets using the key returned by bucket_key()."""
        self.buckets.clear()
        for item in data:
            key = self.bucket_key(item)
            self.buckets[key].append(item)

    def aggregate(self):
        """Create summaries for each bucketed group."""
        for key, records in self.buckets.items():
            summary = self.summarize_record_group(key, records)
            self.aggregated_result.append(summary)

    def run(self):
        """Execute full ETL pipeline: load, normalize, group, aggregate, and save."""
        data = self.load_data()
        self.normalize_shipping_and_taxes(data)
        self.bucket_data(data)
        self.finalize_dates(data)
        self.processed_data = data
        self.aggregate()
        self.save_outputs()


class MonthlyChargeAdjustmentAggregator(ChargeAdjustmentAggregatorBase):
    """Aggregates monthly financial summaries from charge data."""

    def bucket_key(self, item):
        """Extract YYYY-MM from transaction date."""
        return item["Transaction Date"].strftime("%Y-%m")

    def summarize_record_group(self, key, records):
        """Summarize all transaction records for a given month."""
        summary = {
            "Month": key,
            "Amount": 0.0, "Fee": 0.0, "Shipping": 0.0, "Taxes": 0.0,
            "Total": 0.0, "Refund": 0.0, "Discount": 0.0, "Other": 0.0,
            "Transaction Count": len(records),
            "OtherTypes": [], "OtherTypeOrders": []
        }
        other_types = Counter()
        other_orders = set()

        for r in records:
            tx_type = r.get("Type", "").lower()
            summary["Amount"] += float(r.get("Amount", 0))
            summary["Fee"] += float(r.get("Fee", 0))
            summary["Shipping"] += float(r.get("Shipping", 0))
            summary["Taxes"] += float(r.get("Taxes", 0))
            summary["Discount"] += float(r.get("Discount Amount", 0))

            if tx_type == "refund":
                summary["Refund"] += float(r.get("Amount", 0))
            elif tx_type not in ("refund", "charge"):
                summary["Other"] += float(r.get("Amount", 0))
                other_types[tx_type] += 1
                if "Order" in r:
                    other_orders.add(r["Order"])

        taxes_by_state = self.state_tax_aggregator(records)
        taxes_by_state["Month"] = key
        self.taxes_by_state_aggregated.append({k: taxes_by_state[k] for k in sorted(taxes_by_state)})

        summary["Total"] = summary["Amount"] - summary["Fee"] - summary["Shipping"] - summary["Taxes"]

        for k in ["Amount", "Fee", "Shipping", "Taxes", "Total", "Refund", "Discount", "Other"]:
            summary[k] = round(summary[k], 2)

        summary["OtherTypes"] = list(other_types.keys())
        summary["OtherTypeOrders"] = sorted(other_orders)
        return summary


class DailyChargeAdjustmentAggregator(ChargeAdjustmentAggregatorBase):
    """Aggregates daily financial summaries from charge data with more fields."""

    def bucket_key(self, item):
        """Extract YYYY-MM-DD from transaction date."""
        return item["Transaction Date"].strftime("%Y-%m-%d")

    def summarize_record_group(self, key, records):
        """Summarize all transaction records for a given day."""
        summary = {
            "Day": key,
            "Amount": 0.0, "Fee": 0.0, "Shipping": 0.0, "Taxes": 0.0,
            "Total": 0.0, "Refund": 0.0, "Discount": 0.0, "Other": 0.0,
            "Net": 0.0, "OrderGross": 0.0, "Subtotal": 0.0,
            "Transaction Count": len(records),
            "OtherTypes": [], "OtherTypeOrders": []
        }
        other_types = Counter()
        other_orders = set()

        for r in records:
            tx_type = r.get("Type", "").lower()
            summary["Amount"] += float(r.get("Amount", 0))
            summary["Fee"] += float(r.get("Fee", 0))
            summary["Shipping"] += float(r.get("Shipping", 0))
            summary["Taxes"] += float(r.get("Taxes", 0))
            summary["Discount"] += float(r.get("Discount Amount", 0))
            summary["Net"] += float(r.get("Net", 0))
            summary["Subtotal"] += float(r.get("Subtotal", 0))
            summary["OrderGross"] += float(r.get("Total", 0))

            if tx_type == "refund":
                summary["Refund"] += float(r.get("Amount", 0))
            elif tx_type not in ("refund", "charge"):
                summary["Other"] += float(r.get("Amount", 0))
                other_types[tx_type] += 1
                if "Order" in r:
                    other_orders.add(r["Order"])

        taxes_by_state = self.state_tax_aggregator(records)
        taxes_by_state["Day"] = key
        self.taxes_by_state_aggregated.append({k: taxes_by_state[k] for k in sorted(taxes_by_state)})

        summary["Total"] = summary["Amount"] - summary["Fee"] - summary["Shipping"] - summary["Taxes"]

        for k in [
            "Amount", "Fee", "Shipping", "Taxes", "Total", "Refund",
            "Discount", "Other", "Net", "OrderGross", "Subtotal"
        ]:
            summary[k] = round(summary[k], 2)

        summary["OtherTypes"] = list(other_types.keys())
        summary["OtherTypeOrders"] = sorted(other_orders)
        return summary
