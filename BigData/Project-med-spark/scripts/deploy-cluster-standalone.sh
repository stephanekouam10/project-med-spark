#!/bin/bask

# Download spark
wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz

# De-archive spark
tar -xzf spark-3.3.1-bin-hadoop3.tgz

# create repository to move spark archive
mkdir Deploy
chmod 777 Deploy
mv spark-3.3.1-bin-hadoop3 Deploy

# set variables environments
export SPARK_HOME=$HOME/Deploy/spark-3.3.1-bin-hadoop3
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
source $HOME/.bashrc

# launch spark-master
cd Deploy/spark-3.3.1-bin-hadoop3/sbin
./start-master.sh

# launch worker
./start-worker.sh spark://gilles-virtual-machine:7077

# launch my applications
cd ../bin
./spark-submit --master spark://gilles-virtual-machine:7077 --conf spark.app.name="spark cluster" --num-executors 2 --executor-cores 1 --executor-memory 1G /home/gilles/Project-med-spark/target/Project-med-spark-1.0-SNAPSHOT.jar
