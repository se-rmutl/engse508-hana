# LAB1: Installing HDFS – Hadoop

## Objective

Install and verify Hadoop in Pseudo-distributed mode on Ubuntu 20.04.

## Prerequisites

- Java 8+ installed
- Ubuntu 20.04 LTS
- User with `sudo` privileges

---

## Step 1: Install Java

```bash
sudo apt update
sudo apt install openjdk-8-jdk -y
java -version
```

---

## Step 2: Add Hadoop User

```bash
sudo adduser hadoop
sudo usermod -aG sudo hadoop
```

Switch to the `hadoop` user:

```bash
su - hadoop
```

---

## Step 3: SSH Configuration

```bash
ssh-keygen -t rsa -P ""
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh localhost
```

---

## Step 4: Download and Install Hadoop

```bash
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzvf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 hadoop
```

Set environment variables. Edit `~/.bashrc`:

```bash
nano ~/.bashrc
```

Add:

```bash
export HADOOP_HOME=/home/hadoop/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
```

Reload:

```bash
source ~/.bashrc
```

Check Hadoop version:

```bash
hadoop version
```

---

## Step 5: Configure Hadoop

### 1. core-site.xml

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

Path: `$HADOOP_HOME/etc/hadoop/core-site.xml`

---

### 2. hdfs-site.xml

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///home/hadoop/hadoopdata/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///home/hadoop/hadoopdata/hdfs/datanode</value>
    </property>
</configuration>
```

Path: `$HADOOP_HOME/etc/hadoop/hdfs-site.xml`

---

### 3. mapred-site.xml

Create from template:

```bash
cp mapred-site.xml.template mapred-site.xml
```

Edit:

```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
```

---

### 4. yarn-site.xml

```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
</configuration>
```

---

## Step 6: Format Namenode

```bash
hdfs namenode -format
```

---

## Step 7: Start Hadoop Daemons

```bash
start-dfs.sh
start-yarn.sh
```

Check Java processes:

```bash
jps
```

Expected Output:

```
NameNode
DataNode
ResourceManager
NodeManager
SecondaryNameNode
```

---

## Step 8: Verify HDFS

Create directories:

```bash
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/hadoop
```

Upload and retrieve files:

```bash
echo "Hello Hadoop" > test.txt
hdfs dfs -put test.txt /user/hadoop/
hdfs dfs -ls /user/hadoop
hdfs dfs -cat /user/hadoop/test.txt
```

---

## Step 9: Access Web Interfaces

- **Namenode UI:** [http://localhost:9870](http://localhost:9870)
- **ResourceManager UI:** [http://localhost:8088](http://localhost:8088)

---

## Notes

- Make sure all required ports are open and not in use.
- If needed, configure Hadoop for multi-node in the next lab.
