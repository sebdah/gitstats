var mongo = require('mongodb'),
	Server = mongo.Server,
	Db = mongo.Db;

var server = new Server('localhost', 27017, {auto_reconnect: true});
var db = new Db('gitstats_saas', server);

function start(response) {
	console.log("Request handler start was called");

	content = '<h1>All time commits</h1>\n';

	function parse_result(err, results, stats) {
		if (err != null) console.log(err);
		console.log("MapReduce job ran in " + stats.processtime + " ms");
		
		results.forEach(function(result){
			content = content + result['_id'] + " (" + result['value']['commits'] + ")" + "<br />";
		});

		response.writeHead(200, {"Content-Type": "text/html"});
	  response.write(content);
	  response.end();

		db.close();
	}

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
						out: {inline : 1},
						verbose: true
					},
					parse_result
				);
			});
		}
	});
}

exports.start = start;
