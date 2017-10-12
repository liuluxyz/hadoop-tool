# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs

PATH=$PATH:$HOME/bin

export ZOOKEEPER_HOME=/home/ocetl/app/zookeeper
#export LANG=C
export JAVA_HOME=/home/ocetl/app/java
export CLASSPATH=.:${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tool.jar
export HADOOP_HOME=/home/ocetl/app/hadoop
export HIVE_HOME=/home/ocetl/app/hive
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${JAVA_HOME}/jre/lib/amd64/server:${HADOOP_HOME}/c++/Linux-amd64-64/lib:/usr/local/lib:/usr/lib64

export AICLOUDETL_HOME=/home/ocetl/app/AI-Cloud-ETL
export SCHEDULER_SERVER_HOME=/home/ocetl/app/AI-Cloud-ETL

export PATH=$HADOOP_HOME/bin:$ZOOKEEPER_HOME/bin:$JAVA_HOME/bin:$HIVE_HOME/bin:$HOME/bin:$PATH

export PATH
