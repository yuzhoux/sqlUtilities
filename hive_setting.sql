set mapred.job.queue.name=team_name;
set mapred.reduce.tasks=1811;
set mapreduce.task.timeout=60000;

set mapreduce.map.memory.mb=3072;
set mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx2611m;

set mapreduce.reduce.memory.mb=3072;
set mapreduce.reduce.java.opts=-Djava.net.preferIPv4Stack=true -Xmx2611m;
