How to use: 
Mammoth is based on hadoop. About how to use hadoop, you can refer to http://hadoop.apache.org/. 
In the following part of this document, we assume you are familiar with hadoop.
You can just replace the "hadoop-core-1.0.1.jar" under HADOOP_HOME with the compiled "hadoop-core-1.0.1-mammoth.jar".
After that you can use mammoth just in the way like hadoop.
Mammoth is developed with 64-bit jdk7, and you are suggested to use the same.
You must specify the child jvm options before running your job, eg:
<property>
   <name>mapred.job.child.java.opts</name>
   <value>-d64 -Xmx8000M -Xms8000M</value>
</property>
You can learn more about the Mammoth on the page: http://grid.hust.edu.cn/xhshi/projects/mammoth.htm .
