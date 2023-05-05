
#from bson import json_util
from bson import (dumps, loads)
#food = json.dumps(foo, default=json_util.default)

loc = {
    "person_id":2,
    "coordinate":"cordinateXY"
}

loc_enc = dumps(loc)
loc_dec = loads(loc_enc)
print(f'loc:{loc}')
print(f'loc enc:{loc_enc}')
print(f'loc_dec:{loc_dec}')
