import json
from pprint import pprint  # for pretty printing

# Load the GeoJSON file
with open("community_areas.geojson") as f:
    data = json.load(f)

# Access the list of features
features = data["features"]

# Loop through the first 3 records (or however many you want)
for i, feature in enumerate(features[:3]):
    print(f"\n--- Feature {i+1} ---")
    pprint(feature["properties"])  # show all field-value pairs