#!/usr/bin/env python

"""
Dumps out a table of 'engagement' data: a table of comments correlated to 
uploads over time, for each organization, and summaries for museums, libraries
and archives.
"""

import json
import time
import redis
import logging

logging.basicConfig(filename='engagement.log', level=logging.INFO)

orgs = json.loads(open('orgs.json').read())

r = redis.StrictRedis()

# collect comments and bucket by time, organization, and org type

comments = {"A": {}, "L": {}, "M": {}}
for comment_id in r.smembers("comments"):
    image_id = r.hget(comment_id, "image")
    org_id = r.hget(image_id, "owner")
    if org_id not in orgs:
        logging.warn("%s: don't know about org %s", image_id, org_id)
        continue

    t = time.strftime('%Y-%m', time.localtime(int(r.hget(comment_id, 'created'))))

    logging.info(comment_id)

    if org_id not in comments:
        comments[org_id] = {}
    comments[org_id][t] = comments[org_id].get(t, 0) + 1

    org_type = orgs[org_id]['type']
    comments[org_type][t] = comments[org_type].get(t, 0) + 1


# collect images and bucket by time uploaded, organization, org_type

images = {"A": {}, "L": {}, "M": {}}
for image_id in r.smembers("images"):
    org_id = r.hget(image_id, "owner")
    if org_id not in orgs:
        logging.warn("%s don't know about org %s", image_id, org_id)
        continue

    logging.info(image_id)

    t = time.strftime('%Y-%m', time.localtime(int(r.hget(image_id, 'created'))))

    if org_id not in images:
        images[org_id] = {}
    images[org_id][t] = images[org_id].get(t, 0) + 1

    org_type = orgs[org_id]['type']
    images[org_type][t] = images[org_type].get(t, 0) + 1

open("stats.json", "w").write(json.dumps({"comments": comments, "images": images}, indent=2))

cols = ["M", "L", "A"].extend(orgs.keys())
print "date\tcomments\tuploads"
for year in range(2006, 2014):
    for month in range(1, 13):
        t = "%i-%02i" % (year, month)
        print t, "\t",
        for col in cols:
            print comments[col].get(t, 0), images[col].get(t, 0),
        print
