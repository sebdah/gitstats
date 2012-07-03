var mongo = require('mongodb'),
	Server = mongo.Server,
	Db = mongo.Db;

var server = new Server('localhost', 27017, {auto_reconnect: true});
var db = new Db('gitstats_avail', server);

function start() {
	console.log("Request handler start was called");

	content = '';

	db.open(function(err, db) {
		if (!err) {
			db.collection('master_commits', function(err, collection) {
				var map = function() {
					emit(this.author_email, {commits: 1});
				};

				var reduce = function(key, values) {
					var sum = 0;
					values.forEach(function(doc) {
						sum += doc.commits;
					});
					return {commits: sum};
				};

				collection.mapReduce(
					map,
					reduce,
					{
						out: {replace : 'test'},
						verbose: true
					},
					function(err, results, stats){
						console.log("MapReduce job ran in " + stats.processtime + " ms");

						/*for (var i = results.length - 1; i >= 0; i--) {
							content = content + "\n" + results[i]['_id'] + "(" + results[i]['value']['commits'] + ")<br />";
						};*/

						db.close();
					}
				);
			});
		}
	});
	console.log('Content: ' + content);
	return content;
}

exports.start = start;
