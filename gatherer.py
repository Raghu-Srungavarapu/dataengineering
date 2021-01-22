import urllib.request
import json

with urllib.request.urlopen('http://rbi.ddns.net/getBreadCrumbData') as response:
  breadcrumb = response.read()

data = json.loads(breadcrumb)

with open('bcsample.json', 'w') as file:
    json.dump(data[:1001], file)

