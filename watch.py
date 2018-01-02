#!/usr/bin/env python3


import argparse
import logging
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
    post_count integer NOT NULL,
    last_seen integer NOT NULL
);
    """

    logging.info("Creating database: %s", database_file)
    with sqlite3.connect(database_file) as con:
        con.execute(POSTS_TABLE)
        con.execute(USER_TABLE)


def check_results(cursor):
    return True if cursor.fetchone() == (1,) else False


def post_already_processed(post_id, cursor):
    logging.debug("Checking post with ID '%s'", post_id)
    CHECK_SUBMISSION_TEMPLATE = 'SELECT EXISTS(SELECT 1 FROM posts WHERE id="{id}" LIMIT 1);'
    cursor.execute(CHECK_SUBMISSION_TEMPLATE.format(id=post_id))
    return check_results(cursor)


def user_exists(name, cursor):
    logging.debug("Checking user with name '%s'", name)
    CHECK_USER_TEMPLATE = 'SELECT EXISTS(SELECT 1 FROM users WHERE name="{name}" LIMIT 1);'
    cursor.execute(CHECK_USER_TEMPLATE.format(name=name))
    return check_results(cursor)


def insert_post(post_id, cursor):
    logging.debug("Inserting post with ID '%s'", post_id)
    INSERT_SUBMISSION_TEMPLATE = 'INSERT INTO posts (id) VALUES ("{id}")'
    cursor.execute(INSERT_SUBMISSION_TEMPLATE.format(id=post_id))


def insert_new_user(name, time, cursor):
    logging.debug("Inserting new user with name '%s'", name)
    INSERT_NEW_USER_TEMPLATE = 'INSERT INTO users (name, post_count, last_seen) VALUES ("{name}", 1, "{time}");'
    cursor.execute(INSERT_NEW_USER_TEMPLATE.format(name=name, time=time))


def update_user(name, time, cursor):
    logging.debug("Updating existing user with name '%s'", name)
    UPDATE_USER_TEMPLATE = 'UPDATE users SET post_count = post_count + 1, last_seen={time} WHERE name="{name}";'
    cursor.execute(UPDATE_USER_TEMPLATE.format(name=name, time=time))


def get_last_seen(name, cursor):
    logging.debug("Getting last seen time for user with name '%s'", name)
    UPDATE_USER_TEMPLATE = 'SELECT last_seen FROM users WHERE name="{name}";'
    cursor.execute(UPDATE_USER_TEMPLATE.format(name=name))
    return cursor.fetchone()[0]


def extract_commenters(subreddit, time_aggregate, cursor):
    # Look at submissions
    for submission in subreddit.top(time_aggregate, limit=None):
        logging.info("Checking submission: '%s' %s", submission.title, submission.id)

        # Skip if we have already processed the post
        if post_already_processed(submission.id, cursor):
            logging.debug("Submission already processed, skipping!")
            continue

        # Insert the post
        insert_post(submission.id, cursor)

        # Get all commenting users
        submission.comments.replace_more(limit=250)
        comments = submission.comments.list()
        total = len(comments)
        for i, comment in enumerate(comments):
            logging.debug("Comment %i: %s", i, comment.permalink)
            # Log progress five times during the loop
            if not i % (total // 5):
                logging.info("Processing comment %i / %i", i+1, total)

            redditor = comment.author
            # Sometimes blank users come through due to deleted posts or other
            # circumstances, we just ignore them
            if redditor is None:
                logging.debug("Commentor is empty, skipping!")
                continue

            name = redditor.name
            time = int(comment.created_utc)

            # Add the user to the database
            if user_exists(redditor.name, cursor):
                last_time = get_last_seen(name, cursor)
                update_user(name, max(time, last_time), cursor)
            else:
                insert_new_user(name, time, cursor)


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
    argparser.add_argument(
        "-l",
        "--log",
        help="set the logging level, defaults to WARNING",
        dest="log_level",
        default=logging.WARNING,
        choices=[
            'DEBUG',
            'INFO',
            'WARNING',
            'ERROR',
            'CRITICAL',
        ],
    )

    args = argparser.parse_args()

    # Set the logging level
    logging.basicConfig(level=args.log_level)

    logging.debug("Arguments: %s", args)

    # Set up the Reddit API
    logging.info("Setting up Reddit API")
    reddit = praw.Reddit(
        client_id=APP_ID,
        client_secret=SECRET,
        user_agent=USER_AGENT,
    )

    # Get the subreddit
    logging.info("Getting subreddit: %s", args.subreddit)
    subreddit = reddit.subreddit(args.subreddit)

    # Set up the database
    setup_database(args.database)
    logging.info("Connecting to database: %s", args.database)
    with sqlite3.connect(args.database) as con:
        cursor = con.cursor()

        extract_commenters(subreddit, args.top, cursor)


# Run the main program if called directly
if __name__ == "__main__":
    main()
