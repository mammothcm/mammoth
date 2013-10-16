How to use: 
Mammoth is a memory-centric MapReduce based on hadoop-1.0.1 aiming to solve the problem of I/O bottleneck in data-intensive
applications. About how to use hadoop, you can refer to its homepage: http://hadoop.apache.org/. 
In the following part of this document, we assume you are familiar with hadoop.
You can just replace the "hadoop-core-1.0.1.jar" under $HADOOP_HOME with the compiled "hadoop-core-1.0.1-mammoth-0.9.0.jar".
After that you can use mammoth just in the same way with hadoop.
Mammoth is developed with 64-bit jdk7, and you are suggested to use the same.
You must specify the child jvm options before running your job, eg:
<property>
   <name>mapred.job.child.java.opts</name>
   <value>-d64 -Xmx8000M -Xms8000M</value>
</property>
This parameter is the only one required to be manually specified because Mammoth can maximize the usage of memory 
in runtime using a rule-based heuristic. You can learn more about the Mammoth on the following page: 
http://grid.hust.edu.cn/xhshi/projects/mammoth.htm.
