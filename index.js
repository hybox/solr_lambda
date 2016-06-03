var request = require('request');
var shuffle = require('shuffle-array');
var util = require('util');

exports.handler = function(event, context, callback) {
  console.info("", "=== EVENT ===\n\n%j", event);

  var url = event.url;

  console.info("GET " + url + "/admin/collections?action=CLUSTERSTATUS&wt=json");

  request(url + "/admin/collections?action=CLUSTERSTATUS&wt=json", function(error, response, body) {
    if (!error && response.statusCode == 200) {
      var info = JSON.parse(body);
      // console.info(util.format("=== RESPONSE ===\n\n%j", info));

      Object.keys(info.cluster.collections).forEach(function(collectionName) {
        var collection = info.cluster.collections[collectionName];
        var replicationFactor = collection.replicationFactor;

        Object.keys(collection.shards).forEach(function(shardName) {
          var shard = collection.shards[shardName];
          var logPrefix = collectionName + ":" + shardName;

          var activeReplicas = Object.keys(shard.replicas).filter(function(replicaName) {
            return shard.replicas[replicaName].state == "active";
          });

          var notDownReplicas = Object.keys(shard.replicas).filter(function(replicaName) {
            return shard.replicas[replicaName].state != "down";
          });

          var downReplicas = Object.keys(shard.replicas).filter(function(replicaName) {
            return shard.replicas[replicaName].state == "down";
          });

          console.info(logPrefix, " ", util.format("Replicas:%j|%j|%j", notDownReplicas, activeReplicas, downReplicas));

          for (var i = notDownReplicas.length; i < replicationFactor; i++) {
            console.warn(logPrefix + ":add", " ", util.format("ADDREPLICA (%d)", i));
            console.info("request", "GET " + url + "/admin/collections?action=ADDREPLICA&collection=" + collectionName + "&shard=" + shardName);
            request(url + "/admin/collections?action=ADDREPLICA&collection=" + collectionName + "&shard=" + shardName, function(error, response, body) {
              var logLevel = response.statusCode == 200 ? "info" : "warn";
              console[logLevel](logPrefix + ":add", " ", "GET " + url + "/admin/collections?action=ADDREPLICA&collection=" + collectionName + "&shard=" + shardName);
              console[logLevel](logPrefix + ":add", " ", util.format("STATUS: %d", response.statusCode));
            });
          }

          if ( activeReplicas.length > replicationFactor ) {
            var replicasToDestroy = shuffle.pick(activeReplicas, { 'picks': activeReplicas.length - replicationFactor });
            console.info(logPrefix, util.format("Pruning %d / %d replicas: %j", replicasToDestroy.length, activeReplicas.length, replicasToDestroy));

            replicasToDestroy.forEach(function(replicaName) {
              console.warn(logPrefix + ":" + replicaName + ":delete", " ", "DELETEREPLICA (%s)", replicaName);
              request(url + "/admin/collections?action=DELETEREPLICA&collection=" + collectionName + "&shard=" + shardName + "&replica=" + replicaName, function(error, response, body) {
                var logLevel = response.statusCode == 200 ? "info" : "warn";
                console[logLevel](logPrefix + ":" + replicaName + ":delete", " ", "GET " + url + "/admin/collections?action=DELETEREPLICA&collection=" + collectionName + "&shard=" + shardName + "&replica=" + replicaName);
                console[logLevel](logPrefix + ":" + replicaName + ":delete", " ", util.format("STATUS: %d", response.statusCode));
              });
            });
          }

          downReplicas.forEach(function(replicaName) {
            console.warn(logPrefix + ":" + replicaName + ":delete", " ", "DELETEREPLICA (%s)", replicaName);
            request(url + "/admin/collections?action=DELETEREPLICA&collection=" + collectionName + "&shard=" + shardName + "&replica=" + replicaName, function(error, response, body) {
              var logLevel = response.statusCode == 200 ? "info" : "warn";
              console[logLevel](logPrefix + ":" + replicaName + ":delete", " ", "GET " + url + "/admin/collections?action=DELETEREPLICA&collection=" + collectionName + "&shard=" + shardName + "&replica=" + replicaName);
              console[logLevel](logPrefix + ":" + replicaName + ":delete", " ", util.format("STATUS: %d", response.statusCode));
            });
          });
        });
      });

      callback();
    } else {
      console.error("GET " + url + "/admin/collections?action=CLUSTERSTATUS&wt=json");
      console.error(util.format("STATUS: %d", response.statusCode));
      console.info(util.format("=== RESPONSE ===\n\n%s", body));
      callback(error);
    }
  });
};
