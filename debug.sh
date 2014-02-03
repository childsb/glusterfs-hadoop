rm -rf target
mvn package -DskipTests=true
scp target/gluster*.jar root@build:/opt/hadoop-2.2.0.2.0.6.0-76/share/hadoop/common/lib
