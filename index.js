var request = require('request');
var shuffle = require('shuffle-array');
var log = require('npmlog');

exports.handler = function(event, context, callback) {
  log.verbose("", "=== EVENT ===\n\n%j", event);

  var url = event.url;

  log.verbose("", "GET " + url + "/admin/collections?action=CLUSTERSTATUS&wt=json");

  request(url + "/admin/collections?action=CLUSTERSTATUS&wt=json", function(error, response, body) {
    if (!error && response.statusCode == 200) {
      var info = JSON.parse(body);
      log.verbose("", "=== RESPONSE ===\n\n%j", info);

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

          log.verbose(logPrefix, "Replicas:%j|%j|%j", notDownReplicas, activeReplicas, downReplicas);

          for (var i = notDownReplicas.length; i < replicationFactor; i++) {
            log.info(logPrefix + ":add", "ADDREPLICA (%d)", i);
            log.verbose("request", "GET " + url + "/admin/collections?action=ADDREPLICA&collection=" + collectionName + "&shard=" + shardName);
            request(url + "/admin/collections?action=ADDREPLICA&collection=" + collectionName + "&shard=" + shardName, function(error, response, body) {
              var logLevel = response.statusCode == 200 ? "verbose" : "warn";
              log.log(logLevel, logPrefix + ":add", "GET " + url + "/admin/collections?action=ADDREPLICA&collection=" + collectionName + "&shard=" + shardName);
              log.log(logLevel, logPrefix + ":add", "STATUS: %d", response.statusCode);
            });
          }

          if ( activeReplicas.length > replicationFactor ) {
            var replicasToDestroy = shuffle.pick(activeReplicas, { 'picks': activeReplicas.length - replicationFactor });
            log.verbose(logPrefix, "Pruning %d / %d replicas: %j", replicasToDestroy.length, activeReplicas.length, replicasToDestroy);

            replicasToDestroy.forEach(function(replicaName) {
              log.info(logPrefix + ":" + replicaName + ":delete", "DELETEREPLICA (%s)", replicaName);
              request(url + "/admin/collections?action=DELETEREPLICA&collection=" + collectionName + "&shard=" + shardName + "&replica=" + replicaName, function(error, response, body) {
                var logLevel = response.statusCode == 200 ? "verbose" : "warn";
                log.log(logLevel, logPrefix + ":" + replicaName + ":delete", "GET " + url + "/admin/collections?action=DELETEREPLICA&collection=" + collectionName + "&shard=" + shardName + "&replica=" + replicaName);
                log.log(logLevel, logPrefix + ":" + replicaName + ":delete", "STATUS: %d", response.statusCode);
              });
            });
          }
        });
      });

      callback();
    } else {
      log.warn("", "GET " + url + "/admin/collections?action=CLUSTERSTATUS&wt=json");
      log.warn("", "STATUS: %d", response.statusCode);
      log.verbose("", "=== RESPONSE ===\n\n%s", body);
      callback(error);
    }
  });
};
