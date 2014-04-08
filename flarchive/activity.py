#!/usr/bin/env python

"""
Loads up a redis instance with notes, tags, comments authors for a given 
organization.  Use the following key patterns to look stuff up.

* images - set of all images
* image:1234 - info for image 1234
* image:1234:tags - tags for a image 1234

* tags - set of all tags
* tag:foo - info for tag "foo"
* tag:foo:images - images for a given tag "foo"

* notes - set of all notes
* note:1234 - note information
* image:1234:notes - notes for image 1234

* comments - set of all comments
* comment:1234 - comment information
* image:1234:comments - comments for image 1234

* sets - set of all sets
* set:1234 - set information
* image:1234:sets - sets that image 1234 is a part of

"""

import os
import json
import redis

r = redis.StrictRedis()

d = "/mnt/flickr-commons/flickr-commons-metadata-1.0/data/"

def main(org_id): 

    for dirpath, dirnames, filenames in os.walk(d + org_id):
        for filename in filenames:
            path = os.path.join(dirpath, filename)
            if filename.endswith("i.json"):
                load_info(path)
            elif filename.endswith("c.json"):
                load_comments(path)
            elif filename.endswith("ctx.json"):
                load_sets(path)


def load_info(path):
    image = json.loads(open(path).read())

    # general image info
    image_id = "image:%s" % image['photo']['id']
    r.hset(image_id, 'views', image['photo']['views'])
    r.hset(image_id, 'title', image['photo']['title'])
    r.hset(image_id, 'created', image['photo']['dateuploaded'])
    r.sadd('images', image_id)
   
    # tags
    for tag in image['photo']['tags']['tag']:
        tag_id = 'tag:%s' % tag['raw']
        author_id = 'author:%s' % tag['author']
        r.sadd('%s:tags' % image_id, tag_id)
        r.sadd('%s:images' % tag_id, image_id)
        r.sadd('%s:authors' % tag_id, author_id)
        r.sadd('tags', tag_id)
        if tag['machine_tag'] == 1:
            r.sadd('machinetags', tag_id)
        r.sadd('authors', author_id)

    # notes
    for note in image['photo']['notes']['note']:
        note_id = 'note:%s' % note['id']
        author_id = 'author:%s' % note['author']
        r.sadd('image-notes:%s' % image_id, note_id)
        r.hset(note_id, 'author', note['author'])
        r.hset(note_id, 'content', note['_content'])
        r.hset(note_id, 'w', note['w'])
        r.hset(note_id, 'h', note['h'])
        r.hset(note_id, 'x', note['x'])
        r.hset(note_id, 'y', note['y'])
        r.hset(note_id, 'author', author_id)
        r.sadd('notes', note_id)
        r.sadd('authors', author_id)


def load_comments(path):
    comments = json.loads(open(path).read())
    if 'comments' not in comments or 'comment' not in comments['comments']:
        return
    for comment in comments['comments']['comment']:
        comment_id = 'comment:%s' % comment['id']
        image_id = 'image:%s' % comments['comments']['photo_id']
        author_id = 'author:%s' % comment['author']
        r.hset(comment_id, 'content', comment['_content'])
        r.hset(comment_id, 'author', author_id)
        r.hset(comment_id, 'image', image_id)
        r.hset(comment_id, 'created', comment['datecreate'])
        r.sadd('%s:comments' % image_id, comment_id)
        r.sadd('comments', comment_id)
        r.sadd('authors', author_id)


def load_sets(path):
    sets = json.loads(open(path).read())
    if 'set' not in sets:
        return
    image_id = 'image:%s' % os.path.basename(path).replace('-ctx.json', '')
    for s in sets['set']:
        set_id = 'set:%s' % s['id']
        r.hset(set_id, 'title', s['title'])
        r.hset(set_id, 'views', s['view_count'])
        r.sadd('%s:images' % set_id, image_id)
        r.sadd('%s:sets' % image_id, set_id)
        r.sadd('sets', set_id)

if __name__ == "__main__":
    # TODO: right now it's hard-coded to Brooklyn Musuem 
    main('83979593@N00')


