CLASSPATH=./build/classes
CLASSPATH=$CLASSPATH:./lib/*
CLASSPATH=$CLASSPATH:./lib-etc/*
#CLASSPATH=$CLASSPATH:/working/servers/hadoop/lib/*
export CLASSPATH

#java -Xmx512m -Xms512m -XX:+AggressiveOpts -XX:CompileThreshold=200 -Djava.util.logging.config.file=./src/resource/logging_$1.properties -cp $CLASSPATH $@
java -Xmx512m -Xms256m -XX:+AggressiveOpts -XX:CompileThreshold=200 -Dlogback.configurationFile=./logback-writer.xml -cp $CLASSPATH $@
