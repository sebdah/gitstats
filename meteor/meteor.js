Commits = new Meteor.Collection("commits");
AllTimeCommitters = new Meteor.Collection("all_time_committers");

if (Meteor.is_client) {
  Template.latest_commits.commits = function () {
    return Commits.find({}, {sort: {authored_date: -1}});
  };

  Template.all_time_committers.commits = function () {
    return AllTimeCommitters.find({}, {sort: {commits: 1}});
  };

  Template.latest_commits.selected = function () {
    return Session.equals("selected_commit", this._id) ? "selected" : "";
  };

  Template.latest_commits.events = {
    'click div#commit': function () {
      if (Session.equals("selected_commit", this._id)) {
        Session.set('selected_commit', '');
      } else {
        Session.set('selected_commit', this._id);
      }
      
    }
  };
}

if (Meteor.is_server) {
  Meteor.startup(function () {
    // code to run on server at startup
  });
}