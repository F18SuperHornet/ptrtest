#!/bin/python
# Go to manage services, pick the PTR and set the access key here.
ACCESS_KEY=""
REPO="https://ptr.hydrus.network:45871"
CLIENTDBPATH=""
CLIENTMASTERDBPATH=""

import requests
import zlib
import sqlite3
import json
import sys
import os

# DELETE THIS IF YOU MADE A BACKUP THAT THIS SCRIPT CANNOT DESTROY
# 8<---------------- REMOVE STARING HERE -----------------8<
print("EDIT THE SOURCE CODE TO REMOVE THIS ERROR")
print("MAKE SURE YOU HAVE A DATABASE BACKUP")
print("DO NOT IGNORE THIS WARNING")
sys.exit(1)
# 8<---------------- REMOVE ENDING HERE ------------------8<

# To save PTR bandwidth, save the output to /tmp/ptr_meta.json
if os.path.isfile("/tmp/ptr_meta.json"):
    print("Using cached metadata from /tmp/ptr_meta.json")
    with open("/tmp/ptr_meta.json", "r") as f:
        remote_array = json.load(f)
else:
    print("Loading defintions from the PTR. (Please ignore the SSL warning)")
    headers = { "Hydrus-Key": ACCESS_KEY }
    response = requests.get(REPO+"/metadata?since=0", headers=headers, verify=False)
    remote_array = json.loads(zlib.decompress(response.content))
    with open("/tmp/ptr_meta.json", "w+") as f:
        json.dump(remote_array, f)

print("Attaching local client database...")
conn = sqlite3.connect(":memory:")
c = conn.cursor()
c.execute("ATTACH DATABASE ? as clientdb", (CLIENTDBPATH,))
c.execute("ATTACH DATABASE ? as masterdb", (CLIENTMASTERDBPATH,))

result = c.execute("select clientdb.repository_updates_16.update_index, masterdb.hashes.hash from clientdb.repository_updates_16 join masterdb.hashes on hashes.hash_id = repository_updates_16.hash_id").fetchall()

print("Building dicts...")
# Load remote metadata into an array
ptr_updates = dict()
for i in remote_array[2][1][0][1][2][0]:
    updateid = i[0]
    hashes = i[1]
    ts_current = i[2]
    ts_next = i[3]
    ptr_updates[updateid] = hashes

# Load local metadata into an array
local_updates = dict()
for localresult in result:
    updateid = localresult[0]
    hash_value = str(localresult[1].hex())
    if not updateid in local_updates:
        local_updates[updateid] = []

    local_updates[updateid] += (hash_value,)

# Compare the two dicts
if local_updates == ptr_updates:
    print("No inconsistencies found, terminating early.")

# Compare our local to remote (check if we have too much on our side)
for local_updateid in local_updates:
    if not local_updateid in ptr_updates:
        print("Local client has additional update id: " + str(local_updateid))
        continue
    if not local_updates[local_updateid] == ptr_updates[local_updateid]:
        print(" == FOUND MISMATCH IN UPDATE ID " + str(local_updateid))
        print("LOCAL SAYS: " + str(local_updates[local_updateid]))
        print("REMOTE SAYS: " + str(ptr_updates[local_updateid]))
# Compare remote to local - check if remote has more than us
for remote_updateid in ptr_updates:
    if not remote_updateid in local_updates:
        print("Remote update ID " + str(remote_updateid) + " is missing in local database!")
        continue
    if not ptr_updates[remote_updateid] == local_updates[remote_updateid]:
        print(" == FOUND MISMATCH IN UPDATE ID " + str(remote_updateid))
        print("LOCAL SAYS: " + str(local_updates[remote_updateid]))
        print("REMOTE SAYS: " + str(ptr_updates[remote_updateid]))
conn.close()
