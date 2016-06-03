var request = require('request');
var shuffle = require('shuffle-array');

exports.handler = function(event, context, callback) {
  var url = event.url;

  request(url + "/admin/collections?action=CLUSTERSTATUS&wt=json", function(error, response, body) {
    if (!error && response.statusCode == 200) {
      var info = JSON.parse(body);

      Object.keys(info.cluster.collections).forEach(function(collectionName) {
        var collection = info.cluster.collections[collectionName];
        var replicationFactor = collection.replicationFactor;
        
        console.log("Collection: " + collectionName);

        Object.keys(collection.shards).forEach(function(shardName) {
          var shard = collection.shards[shardName];

          var activeReplicas = Object.keys(shard.replicas).filter(function(replicaName) {
            return shard.replicas[replicaName].state == "active";
          });

          var notDownReplicas = Object.keys(shard.replicas).filter(function(replicaName) {
            return shard.replicas[replicaName].state != "down";
          });

          console.log("  Shard: " + shardName + " (active replicas: " + activeReplicas +" / " + notDownReplicas + ")");

          for (var i = notDownReplicas.length; i < replicationFactor; i++) {
            console.log("    ... adding new replica (" + i + ")");
            request(url + "/admin/collections?action=ADDREPLICA&collection=" + collectionName + "&shard=" + shardName);
          }

          if ( activeReplicas.length > replicationFactor ) {
            var replicasToDestroy = shuffle.pick(activeReplicas, { 'picks': activeReplicas.length - replicationFactor });

            replicasToDestroy.forEach(function(replicaName) {
              console.log("    ... removing replica (" + replicaName + ")");
              request(url + "/admin/collections?action=DELETEREPLICA&collection=" + collectionName + "&shard=" + shardName + "&replica=" + replicaName);
            });
          }
        });
      });

      callback();
    } else {
      callback(error);
    }
  });
};
