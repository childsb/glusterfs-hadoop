#!/bin/sh
rm -rf target
mvn package -DskipTests=true
scp target/glusterfs-2.0-SNAPSHOT.jar root@build:/opt/hadoop-2.0.5-alpha/share/hadoop/common/lib
