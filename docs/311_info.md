# 311 API details

provider: Socrata

To access the dataset I need to sign up into NYC Open Data website.

This is the api documentation for the 311 Service Requests API which we need to know to know the number of service requests registered and number of service requests solved which tells about how is the urban infra state for this day.

Properties of this Dataset:
- Updated daily.
- API available.
- Max limit is 1000 rows per call.
- All communications with the API is done through a HTTPS call.
- Possible response outcomes file formats are `.json`, `.csv`, `.xml`. Which can be requested just by these extensions in the api call.
- Example call
```
curl --header 'X-App-Token: your-application-token' \
     --json '{
        "query": "SELECT *",
        "page": {
            "pageNumber": 1,
            "pageSize": 100
        },
        "includeSynthetic": false
     }' \
     https://soda.demo.socrata.com/api/v3/views/4tka-6guv/query.json
```
```
https://data.cityofnewyork.us/api/v3/views/erm2-nwe9/query.json?pageNumber=1&pageSize=10&app_token=$YOUR_APP_TOKEN
```

```python
#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.cityofnewyork.us", None)

# Example authenticated client (needed for non-public datasets):
# client = Socrata(data.cityofnewyork.us,
#                  MyAppToken,
#                  username="user@example.com",
#                  password="AFakePassword")

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("erm2-nwe9", limit=2000)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)
```

- There is a python library to directly access the 311 data. `https://github.com/afeld/sodapy`

- Documentation: https://deepwiki.com/afeld/sodapy/1-home
