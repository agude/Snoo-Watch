#!/usr/bin/env python3


import sqlite3
import praw

from secret import APP_ID, SECRET, USER_AGENT



CHECK_SUBMISSION_TEMPLATE = 'SELECT EXISTS(SELECT 1 FROM posts WHERE id="{id}" LIMIT 1);'
INSERT_SUBMISSION_TEMPLATE = 'INSERT INTO posts (id) VALUES ("{id}")'
CHECK_USER_TEMPLATE = 'SELECT EXISTS(SELECT 1 FROM users WHERE name="{name}" LIMIT 1);'
INSERT_NEW_USER_TEMPLATE = 'INSERT INTO users (name, seen) VALUES ("{name}", 1);'
UPDATE_USER_TEMPLATE = 'UPDATE users SET seen = seen + 1 WHERE name="{name}";'

# Get a reddit instance
reddit = praw.Reddit(
    client_id=APP_ID,
    client_secret=SECRET,
    user_agent=USER_AGENT,
)

# Get the subreddit
subreddit = reddit.subreddit('the_donald')

# Open the database
with sqlite3.connect("snoo_watch_db.sqlite3") as con:
    c = con.cursor()
    # Look at submissions
    for submission in subreddit.top('month'):
        c.execute(CHECK_SUBMISSION_TEMPLATE.format(id=submission.id))
        processed = True if c.fetchone() == (1,) else False

        # Skip if we have already processed the post
        if processed:
            continue

        # Insert the post
        c.execute(INSERT_SUBMISSION_TEMPLATE.format(id=submission.id))

        # Get all commenting users
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            redditor = comment.author
            if redditor is None:
                continue

            name = redditor.name

            # Check if the user is in the database
            c.execute(CHECK_USER_TEMPLATE.format(name=name))
            exists = True if c.fetchone() == (1,) else False

            # Add the user to the database
            if exists:
                c.execute(UPDATE_USER_TEMPLATE.format(name=name))
            else:
                c.execute(INSERT_NEW_USER_TEMPLATE.format(name=name))
