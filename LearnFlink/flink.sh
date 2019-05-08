#!/usr/bin/env bash


basePatn="/Users/apple/Documents/AgentJava/flink-master/flink-dist/target/flink-1.7-SNAPSHOT-bin/flink-1.7-SNAPSHOT/bin"
jarPath="/Users/apple/Documents/AgentJava/flink-master/LearnFlink/target"
savepointPath="/Users/apple/Desktop/state/savepointData/"
#target="$0"
#iteration=0
#while [ -L "$target" ]; do
#    if [ "$iteration" -gt 100 ]; then
#        echo "Cannot resolve path: You have a cyclic symlink in $target."
#        break
#    fi
#    ls=`ls -ld -- "$target"`
#    target=`expr "$ls" : '.* -> \(.*\)$'`
#    echo "$target"
#    iteration=$((iteration + 1))
#done
# 具体命令行查看 （https://ci.apache.org/projects/flink/flink-docs-release-1.7/ops/cli.html）

#${basePatn}/flink -h

# 运行flink job
#${basePatn}/flink run ${jarPath}/${1}
# 运行flink job 指定主类
#${basePatn}/flink run -c org.apache.flink.examples.java.wordcount.WordCount ${jarPath}/${1}

# 查看正在运行的job
${basePatn}/flink list -r #查看全部正在运行的job任务

#${basePatn}/flink list -r | xargs -I a ${basePatn}/flink savepoint a ${savepointPath}

# 关闭正在运行的job
#${basePatn}/flink cancel 10356e75cd768aa31e138d2d95303cf5 #关闭指定job 怎么查看jobid #${basePatn}/flink list -r

# 触发检查点 指定jobid
#${basePatn}/flink savepoint ac30e1322dc9f39e49c88d4eba5254e8 ${savepointPath}
# 关闭并触发检查点 可指定检查点位置信息 指定jobid
#${basePatn}/flink cancel -s 10356e75cd768aa31e138d2d95303cf5
#${basePatn}/flink list -r | grep '[:.0-9]' | awk '{ print $4; }' | xargs -I a ${basePatn}/flink savepoint a ${savepointPath}

#${basePatn}/flink list -r | grep '[:.0-9]' | awk '{ print $4; }'
