#!/usr/bin/env python

"""
Script for parsing git repos
"""

########################################
#
#       SETTINGS
#
########################################

# Branch to use
BRANCH = 'master'

# Repo name
# Used to identify the database
REPO_NAME = 'avail'

# Path to git repository
REPO_PATH = '/Users/sebastian/git/avail'

########################################
#
#       CODE
#
########################################

import sys
import datetime

try:
    import git
except ImportError:
    print """\
Could not import Python module git. \
Please install it from https://github.com/gitpython-developers/GitPython"""
    sys.exit(1)

try:
    import pymongo
except ImportError:
    print """\
Could not import Python module pymongo. \
Please install it using "sudo pip install pymongo"
"""
    sys.exit(1)

# Get git repo
REPO = git.Repo(REPO_PATH)

# Make sure the repo exists since before
assert REPO.bare == False


def full_parse():
    """
    Parse full repo and insert into MongoDB
    """
    # Connect to MongoDB
    connection = pymongo.Connection()
    db = connection['gitstats_%s' % REPO_NAME]

    # Initate the collections
    collection_commits = db['%s_commits' % BRANCH]

    # Parsed commits
    parsed_commits = 0
    previously_parsed = 0

    for commit in REPO.iter_commits(BRANCH):
        cursor = collection_commits.find({'_id': commit.hexsha})
        if cursor.count() == 0:
            # Get and generalize the timestamp
            authored_date = datetime.datetime.fromtimestamp(int(commit.authored_date)).strftime('%Y-%m-%d %H:%M:%S')
            authored_date = datetime.datetime.strptime(authored_date, '%Y-%m-%d %H:%M:%S')
            committed_date = datetime.datetime.fromtimestamp(int(commit.committed_date)).strftime('%Y-%m-%d %H:%M:%S')
            committed_date = datetime.datetime.strptime(committed_date, '%Y-%m-%d %H:%M:%S')

            # Add the commit to the parsed commits list
            collection_commits.insert({
                '_id': commit.hexsha,
                'author_name': commit.author.name,
                'author_email': commit.author.email,
                'author_tz_offset': commit.author_tz_offset,
                'authored_date': authored_date,
                'committer_name': commit.committer.name,
                'committer_email': commit.committer.email,
                'committer_tz_offset': commit.committer_tz_offset,
                'committed_date': committed_date,
                'encoding': commit.encoding,
                'summary': commit.summary,
                })
        else:
            previously_parsed += 1

        parsed_commits += 1
        if parsed_commits % 100 == 0:
            print '%i commits parsed..' % parsed_commits

    print """
Done parsing commits!
%i commits parsed of which %i \
has been parsed before""" % (parsed_commits, previously_parsed)

if __name__ == "__main__":
    full_parse()

sys.exit(0)
