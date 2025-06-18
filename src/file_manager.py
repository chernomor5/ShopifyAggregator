import os
import csv
import json
import shutil
from config import config  # ✅ import config

class FileManager:
    """Handles file path resolution, creation, and saving of CSV and JSON data.

    This utility abstracts away path management and file writing for both
    intermediate and final output, and can optionally copy files to the project root.
    """
    def __init__(self, json_dir=None, csv_dir=None, reset=False):
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

        self.json_dir = os.path.join(project_root, "JSON") if json_dir is None else os.path.abspath(json_dir)
        self.csv_dir = os.path.join(project_root, "CSV") if csv_dir is None else os.path.abspath(csv_dir)

        if reset:
            if os.path.exists(self.json_dir):
                shutil.rmtree(self.json_dir)
            if os.path.exists(self.csv_dir):
                shutil.rmtree(self.csv_dir)

        os.makedirs(self.json_dir, exist_ok=True)
        os.makedirs(self.csv_dir, exist_ok=True)

    def get_path(self, filename, filetype):
        """Resolve the full file path based on file type (CSV or JSON)."""
        folder = self.json_dir if filetype.lower() == "json" else self.csv_dir
        return os.path.join(folder, filename)

    def save_json(self, data, filename, copy_to_root=False):
        """Save data as JSON to the appropriate output directory."""
        path = self.get_path(filename, "json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        if copy_to_root:
            self._copy_to_root(path, filename, self.json_dir)

    def save_csv(self, data, filename, copy_to_root=False):
        """Save data as CSV to the appropriate output directory, preserving column order for aggregated output."""
        if not data:
            return
        path = self.get_path(filename, "csv")

        # ✅ Preserve column order only for the configured aggregated.csv
        if filename == config["aggregated_csv"]:
            keys = list(data[0].keys())  # preserve order as defined in data
        else:
            keys = sorted({k for d in data for k in d})  # default to alphabetical

        with open(path, "w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)

        if copy_to_root:
            self._copy_to_root(path, filename, self.csv_dir)

    def _copy_to_root(self, src_path, filename, target_dir):
        """Copy the file from its subdirectory to the project root for easy access."""
        root_path = os.path.join(os.path.dirname(target_dir), filename)
        shutil.copy(src_path, root_path)
