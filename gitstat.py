#!/usr/bin/env python

"""
Script for parsing git repos
"""

import sys
import datetime
import optparse
from bson.code import Code

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


def main():
    """
    Main function
    """
    parser = optparse.OptionParser(conflict_handler="resolve",
            description="Parsing git repositories and inserting data to MongoDB")
    parser.add_option('-r', '--repository', action='store', type='string',
            dest='repository', default='', help='Path to repository')
    parser.add_option('-b', '--branch', action='store', type='string',
            dest='branch', default='master', help='Branch to parse (default: master)')
    parser.add_option('-h', '--host', action='store', type='string',
            dest='host', default='localhost', help='MongoDB host')
    parser.add_option('-p', '--port', action='store', type='int',
            dest='port', default=3002, help='MongoDB port number')
    parser.add_option('--full-parse', action='count',
            dest='full_parse', default=0, help='Parse a full repository')
    options, _ = parser.parse_args()

    # Connect to MongoDB
    try:
        connection = pymongo.Connection(host=options.host, port=options.port)
    except pymongo.errors.ConnectionFailure:
        print 'Failed to connect to MongoDB'
        sys.exit(1)
    else:
        database = connection['meteor']

    if options.full_parse > 0:
        if options.repository == '':
            print 'Error: --repository must be set'
            sys.exit(1)

        # Remove any trailing slashes
        if options.repository[len(options.repository) - 1] == '/':
            options.repository = options.repository[:len(options.repository) - 1]

        # Get the repo name
        repo_name = options.repository.split('/')
        repo_name = repo_name[len(repo_name) - 1]

        # Initialize the repository
        repo = git.Repo(options.repository)

        # Make sure the repo exists since before
        try:
            assert repo.bare == False
        except AssertionError:
            print '%s is not a valid git repo' % options.repository
            sys.exit(1)

        # Start by populating the commits collection
        print 'Populating MongoDB with repository data'
        populate_mongodb(database, repo, options.branch)

    # Reduce the data
    print "Executing MapReduce for all time high.."
    map_reduce_all_time_high(database)
    print "Executing reduction on latest commits.."
    reduce_commits(database)

    print "All done"


def populate_mongodb(database, repo, branch):
    """
    Parse full repo and insert into MongoDB
    """
    # Initate the collections
    collection_commits = database['commits']

    # Parsed commits
    parsed_commits = 0
    previously_parsed = 0

    for commit in repo.iter_commits(branch):
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
has been parsed before and was ignored""" % (parsed_commits, previously_parsed)


def map_reduce_all_time_high(database):
    """
    Make a map reduce to create the all time high top list
    """
    # Mapping function
    mapper = Code("""
        function() {
            emit(this.author_email, {commits: 1});
        };
        """)

    # Reducer function
    reducer = Code("""
        function(key, values) {
            var sum = 0;
            values.forEach(function(doc) {
                sum += doc.commits;
            });
            return {commits: sum};
        };
        """)

    collection_commits = database['commits']
    collection_commits.map_reduce(mapper, reducer, 'all_time_committers')

    # Ugly way to just save the 10 most popular
    top10 = database['all_time_committers'].find().sort('value', pymongo.DESCENDING).limit(10)
    top10_ids = []
    for doc in top10:
        top10_ids += [doc['_id']]
    
    for doc in database['all_time_committers'].find():
        if doc['_id'] not in top10_ids:
            database['all_time_committers'].remove(doc)


def reduce_commits(database):
    """
    Reduce the commits collection
    """
    new_docs = database.commits.find().sort('committed_date', pymongo.DESCENDING).limit(10)
    database.commits_small.drop()
    for doc in new_docs:
        database.commits_small.insert(doc)

if __name__ == "__main__":
    main()

sys.exit(0)
