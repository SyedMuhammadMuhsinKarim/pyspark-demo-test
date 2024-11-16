import re
import json
from typing import List, Dict, Any
from datetime import datetime, timedelta

class ObjectPathProcessor:
    """
    A class to parse object paths and calculate minimum and maximum months per ID,
    including handling missing months (gaps).
    """

    def __init__(self):
        self.parsed_keys = []

    @staticmethod
    def parse_object_path(object_path: str) -> Dict[str, str]:
        """
        Parse the given object path and extract components using regex.

        Args:
            object_path (str): The object path string to parse.

        Returns:
            Dict[str, str]: A dictionary containing the parsed components.

        Raises:
            ValueError: If the object path format is invalid.
        """
        pattern = r'(\w+)://([A-Za-z0-9-_]+)\/([A-Za-z0-9-_\/]+)\/id=([^/]+)/month=([\d-]+)/([^/]+)\.gz$'
        is_match = re.match(pattern, object_path.strip())  # Handle leading/trailing spaces
        if not is_match:
            raise ValueError(f"Invalid object path format: {object_path.strip()}")

        keys = ['protocol', 'bucket', 'base_path', 'id', 'month', 'timestamp']
        return dict(zip(keys, is_match.groups()))

    def process_object_paths(self, object_paths: List[str]) -> None:
        """
        Parse multiple object paths and store the results internally.

        Args:
            object_paths (List[str]): A list of object paths to process.
        """
        self.parsed_keys = [self.parse_object_path(path) for path in object_paths if path.strip()]

    @staticmethod
    def reduce_by_id(parsed_keys: List[Dict[str, str]]) -> Dict[str, List[str]]:
        """
        Group months by ID from parsed keys.

        Args:
            parsed_keys (List[Dict[str, str]]): A list of parsed object keys.

        Returns:
            Dict[str, List[str]]: A dictionary where keys are IDs and values are lists of months.
        """
        months_by_id = {}
        for key in parsed_keys:
            id_ = key['id']
            month = key['month']
            if id_ not in months_by_id:
                months_by_id[id_] = [month]
            else:
                months_by_id[id_].append(month)
        return months_by_id

    @staticmethod
    def find_min_max_months(months_by_id: Dict[str, List[str]]) -> Dict[str, Dict[str, Any]]:
        """
        Find the minimum and maximum months per ID and detect missing months (gaps).

        Args:
            months_by_id (Dict[str, List[str]]): A dictionary where keys are IDs and values are lists of months.

        Returns:
            Dict[str, Dict[str, Any]]: A dictionary where keys are IDs and values are dictionaries
                                        with 'min_month', 'max_month', and 'missing_months' keys.
        """
        min_max_by_id = {}

        for id_, months in months_by_id.items():
            sorted_months = sorted(set(months))
            min_month = sorted_months[0]
            max_month = sorted_months[-1]

            missing_months = []
            current_month = datetime.strptime(min_month, "%Y-%m-%d")
            end_month = datetime.strptime(max_month, "%Y-%m-%d")

            while current_month < end_month:
                current_month += timedelta(days=32) 
                current_month = current_month.replace(day=1)
                if current_month.strftime("%Y-%m-%d") not in sorted_months:
                    missing_months.append(current_month.strftime("%Y-%m-%d"))

            min_max_by_id[id_] = {
                'min_month': min_month,
                'max_month': max_month,
                'missing_months': missing_months,
            }

        return min_max_by_id

    @staticmethod
    def write_to_json(data: Dict[str, Any], file_name: str = 'min_max_by_id.json') -> None:
        """
        Write data to a JSON file.

        Args:
            data (Dict[str, Any]): The data to write to the file.
            file_name (str): The name of the output JSON file. Defaults to 'min_max_by_id.json'.
        """
        with open(file_name, 'w') as f:
            json.dump(data, f, indent=4)

    def process_and_write(self, object_paths: List[str], output_file: str = 'min_max_by_id.json') -> None:
        """
        Parse, process, and write the min and max months for each ID to a JSON file.

        Args:
            object_paths (List[str]): A list of object paths to process.
            output_file (str): The name of the output JSON file. Defaults to 'min_max_by_id.json'.
        """
        self.process_object_paths(object_paths)
        months_by_id = self.reduce_by_id(self.parsed_keys)
        min_max_by_id = self.find_min_max_months(months_by_id)
        self.write_to_json(min_max_by_id, output_file)