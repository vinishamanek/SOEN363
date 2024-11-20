import requests
from typing import Dict, Optional

class PublisherDataCollector:
    def __init__(self):
        """Initialize the Publisher Data Collector."""
        self.crossref_api_url = "https://api.crossref.org/members"
        self.openlibrary_api_url = "https://openlibrary.org/publishers"

    def fetch_publisher_from_crossref(self, query: str) -> Optional[Dict]:
        """Fetch publisher details from Crossref."""
        response = requests.get(f"{self.crossref_api_url}?query={query}")
        if response.status_code == 200:
            members = response.json().get("message", {}).get("items", [])
            if members:
                member = members[0]
                return {
                    "name": member.get("primary-name"),
                    "description": None,
                    "founded_year": None,
                    "website": member.get("website"),
                    "country_code": member.get("country"),
                }
        print(f"Error fetching from Crossref: {response.status_code}, {response.text}")
        return None

    def fetch_publisher_from_openlibrary(self, publisher_id: str) -> Optional[Dict]:
        """Fetch publisher details from Open Library."""
        response = requests.get(f"{self.openlibrary_api_url}/{publisher_id}.json")
        if response.status_code == 200:
            data = response.json()
            return {
                "name": data.get("name"),
                "description": data.get("description"),
                "founded_year": None,
                "website": None,
                "country_code": None,
                "openlibrary_publisher_id": publisher_id,
            }
        print(f"Error fetching from Open Library: {response.status_code}, {response.text}")
        return None

    def aggregate_publisher_data(self, name: str, openlibrary_id: str) -> Dict:
        """Combine data from multiple APIs to create a complete publisher profile."""
        crossref_data = self.fetch_publisher_from_crossref(name)
        openlibrary_data = self.fetch_publisher_from_openlibrary(openlibrary_id)

        return {
            "name": name,
            "description": crossref_data.get("description") if crossref_data else openlibrary_data.get("description"),
            "founded_year": crossref_data.get("founded_year") if crossref_data else None,
            "website": crossref_data.get("website") if crossref_data else None,
            "country_code": crossref_data.get("country_code") if crossref_data else None,
            "openlibrary_publisher_id": openlibrary_id,
        }

if __name__ == "__main__":
    collector = PublisherDataCollector()

    # Example: Fetch publisher information
    publisher_name = "Penguin Random House"
    openlibrary_id = "Random_House"
    publisher_data = collector.aggregate_publisher_data(publisher_name, openlibrary_id)

    print(f"Publisher Information: {publisher_data}")
