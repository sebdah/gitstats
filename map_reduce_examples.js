/*
All time high commits
*/
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

db.master_commits.mapReduce(map, reduce, {out: {inline : 1}});


/*
Monthly commits
*/
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

db.master_commits.mapReduce(
	map,
	reduce,
	{
		out: { inline : 1},
		query: {
			'authored_date': {
				$lt: ISODate("2012-06-01T00:00:00Z"),
				$gt: ISODate("2012-05-01T00:00:00Z")
			}
		}
	});


