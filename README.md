# flow diagram
![Flow Diagram](relative/path/to/img.jpg?raw=true "FlowDiagram")

# 1.install hadoop :(i did at C:/apps)
https://hadoop.apache.org/releases.html

cd C:\apps\hadoop\etc\hadoop

update below files config as below.
core-site.xml: security settings and file system ase location configuration as below.
<configuration>
   <property>
       <name>fs.defaultFS</name>
       <value>hdfs://localhost:9000</value>
   </property>
   <property>
      <name>hadoop.proxyuser.username.groups</name>
      <value>*</value>
   </property>
   <property>
     <name>hadoop.proxyuser.username.hosts</name>
     <value>*</value>
   </property>
</configuration>

hdfs-site.xml: minimum replication , Data node and name node locations.
<configuration>
   <property>
       <name>dfs.replication</name>
       <value>1</value>
   </property>
   <property>
       <name>dfs.namenode.name.dir</name>
       <value>C:\apps\hadoop\data\namenode</value>
   </property>
   <property>
       <name>dfs.datanode.data.dir</name>
       <value>F:\dnd</value>
   </property>
</configuration>

hive-site.xml: hive warehouse location(can be overriten by spark context),metastore db (mysql) settings.
<configuration>
  <property>
	<name>hive.server2.enable.doAs</name>
	<value>FALSE</value>
	<description>
		Setting this property to true will have HiveServer2 execute
		Hive operations as the user making the calls to it.
	</description>
   </property> 
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>hdfs://localhost:9000/hive-warehouse</value>
      </property>

    <property>
      <name>javax.jdo.option.ConnectionURL</name>
      <value>jdbc:mysql://localhost:3306/hivedb?createDatabaseIfNotExist=true</value>
    </property>
	
    <property>
      <name>javax.jdo.option.ConnectionDriverName</name>
      <value>com.mysql.cj.jdbc.Driver</value>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionUserName</name>
      <value>root</value>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionPassword</name>
      <value>hive</value>
    </property>
	
	<property>
	  <name>datanucleus.autoCreateSchema</name>
	  <value>true</value>
	</property>

	<property>
	  <name>datanucleus.fixedDatastore</name>
	  <value>true</value>
	</property>

	<property>
	 <name>datanucleus.autoCreateTables</name>
	 <value>True</value>
	</property>
</configuration>

Then format name node to reset every thing, then start hdfs file system:
/hadoop/bin/hadoop.cmd namenode -format 
/hadoop/sbin/start-dfs.cmd

#### hive setup ###
cd C:/apps/hadoop/hive
/bin/schematool -dbType mysql -initSchema  ## create hivedb, tables in mysql server.
then start metastore srevice, hiveserver2
/bin/hive --service metastore
/bin/hiveserver2 start
then to verify you can connect using beeline
/bin/beeline
## can login with any user name and password as we set * core-site.xml and hive.server2.enable.doAs value FALSE .
!connect jdbc:hive2://localhost:10000/default

### start the program ####
## to run from intellij add hive and hadoop libs, core-site.xml,hdfs-site.xml,hive-site.xml and mysql-connector.jar in classpath.
## and select add provide scope dependecies in class path.
















