# %%

import collections
import json

# Opening JSON file
with open("/home/src/jobs.json") as json_file:
    data = json.load(json_file)
# %%

# check for duplicates
[item for item, count in collections.Counter(data.keys()).items() if count > 1]
# %%
len(data.keys())
# %%
