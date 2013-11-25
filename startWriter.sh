CLASSPATH=./dist/*
CLASSPATH=$CLASSPATH:./lib/*
#CLASSPATH=$CLASSPATH:/working/servers/hadoop/lib/*
export CLASSPATH
MAIN_CLASS=fresto.datastore.titan.TitanEventWriter
STREAMER_URL=tcp://localhost:7002
STORAGE_BACKEND=cassandra
STORAGE_HOSTNAME=fresto2.owlab.com
MAX_COMMIT_COUNT=3000
LOG_FILE=titan_event_writer.log
#java -Xmx256m -XX:+AggressiveOpts -XX:CompileThreshold=200 -cp $CLASSPATH $@
nohup java -server -Xmx512m -XX:+AggressiveOpts -XX:CompileThreshold=200 -Dlogback.configurationFile=./logback-writer.xml -cp $CLASSPATH $MAIN_CLASS $STREAMER_URL $STORAGE_BACKEND $STORAGE_HOSTNAME $MAX_COMMIT_COUNT < /dev/null > $LOG_FILE 2>&1 &
