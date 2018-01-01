#!/usr/bin/env python3


import argparse
import praw
import sqlite3

from secret import APP_ID, SECRET, USER_AGENT


def setup_database(database_file):
    POSTS_TABLE = """
CREATE TABLE IF NOT EXISTS posts (
id text PRIMARY KEY
);
    """

    USER_TABLE = """
CREATE TABLE IF NOT EXISTS users (
name text PRIMARY KEY,
seen integer NOT NULL
);
    """

    with sqlite3.connect(database_file) as con:
        con.execute(POSTS_TABLE)
        con.execute(USER_TABLE)


def check_results(cursor):
    return True if cursor.fetchone() == (1,) else False


def post_already_processed(post_id, cursor):
    CHECK_SUBMISSION_TEMPLATE = 'SELECT EXISTS(SELECT 1 FROM posts WHERE id="{id}" LIMIT 1);'
    cursor.execute(CHECK_SUBMISSION_TEMPLATE.format(id=post_id))
    return check_results(cursor)


def user_exists(name, cursor):
    CHECK_USER_TEMPLATE = 'SELECT EXISTS(SELECT 1 FROM users WHERE name="{name}" LIMIT 1);'
    cursor.execute(CHECK_USER_TEMPLATE.format(name=name))
    return check_results(cursor)


def insert_post(post_id, cursor):
    INSERT_SUBMISSION_TEMPLATE = 'INSERT INTO posts (id) VALUES ("{id}")'
    cursor.execute(INSERT_SUBMISSION_TEMPLATE.format(id=post_id))


def insert_new_user(name, cursor):
    INSERT_NEW_USER_TEMPLATE = 'INSERT INTO users (name, seen) VALUES ("{name}", 1);'
    cursor.execute(INSERT_NEW_USER_TEMPLATE.format(name=name))


def update_user(name, cursor):
    UPDATE_USER_TEMPLATE = 'UPDATE users SET seen = seen + 1 WHERE name="{name}";'
    cursor.execute(UPDATE_USER_TEMPLATE.format(name=name))


def extract_commenters(subreddit, time_aggregate, cursor):
    # Look at submissions
    for submission in subreddit.top(time_aggregate, limit=None):

        # Skip if we have already processed the post
        if post_already_processed(submission.id, cursor):
            continue

        # Insert the post
        insert_post(submission.id, cursor)

        # Get all commenting users
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            redditor = comment.author
            # Sometimes blank users come through due to deleted posts or other
            # circumstances, we just ignore them
            if redditor is None:
                continue

            name = redditor.name

            # Add the user to the database
            if user_exists(redditor.name, cursor):
                update_user(name, cursor)
            else:
                insert_new_user(name, cursor)


def main():
    """Runs the conversion."""

    # We only need to parse command line flags if running as the main script
    argparser = argparse.ArgumentParser(
        description="Run Snoo Watch"
    )
    # The list of input files
    argparser.add_argument(
        "-d",
        "--database",
        type=str,
        help="the database to write to, which will be created if it does not exist",
        default="snoo_watch_db.sqlite3"
    )
    argparser.add_argument(
        "-s",
        "--subreddit",
        type=str,
        help="the name of the subreddit to watch",
    )
    argparser.add_argument(
        "-t",
        "--top",
        type=str,
        choices=['hour', 'day', 'week', 'month', 'year', 'all'],
        help="scrape the comments of the top posts aggregated by this time",
        default='day',
    )

    args = argparser.parse_args()

    # Set up the Reddit API
    reddit = praw.Reddit(
        client_id=APP_ID,
        client_secret=SECRET,
        user_agent=USER_AGENT,
    )

    # Get the subreddit
    subreddit = reddit.subreddit(args.subreddit)

    # Set up the database
    setup_database(args.database)
    with sqlite3.connect(args.database) as con:
        cursor = con.cursor()
        extract_commenters(subreddit, args.top, cursor)


# Run the main program if called directly
if __name__ == "__main__":
    main()
