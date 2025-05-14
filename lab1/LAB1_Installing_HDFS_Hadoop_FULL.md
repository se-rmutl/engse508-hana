**LAB1  Installing HDFS – Hadoop** 

**PART 1: Installing Apache Hadoop on Ubuntu (Pseudo-distributed Mode)**

**Create a Ubuntu Server 22.04 LTS  (Virtual Machine)**

| CPU                2 Cores Memory               4-8GB Hard disk         50-100GB  |
| :---- |

**Prerequisites**   
Installing Java 8, Hadoop, and Spark requires a working Java installation. We are using version Java 1.8, as it is known to be fully compatible: 

**JDK and Scala Install**

| sudo apt-get update  sudo apt upgrade sudo apt autoremove sudo timedatectl set-timezone Asia/Bangkok timedatectl set-ntp yes date  sudo apt-get install \-y zlib1g-dev libssl-dev —- JAVA8 —-- sudo apt-get update sudo apt install openjdk-8-jdk \-y sudo apt-get install scala \-y java \-version scala \-version  |
| :---- |

**Config java for jre**

|  sudo update-alternatives \--config java  |
| :---- |

[https://towardsdatascience.com/installing-pyspark-with-java-8-on-ubuntu-18-04-6a9dea915b5b](https://towardsdatascience.com/installing-pyspark-with-java-8-on-ubuntu-18-04-6a9dea915b5b)

**Install python3 pip and other libraries** 
```bash
sudo apt install python3-pip 
sudo ln -s /usr/bin/python3 /usr/bin/python  
pip install scipy 
pip install numpy 
pip install pandas  
pip install matplotlib
```

**Step 1: Create a Hadoop User**

| sudo adduser hadoopuser sudo usermod \-aG sudo hadoopuser |
| :---- |

**Step 2: Configure SSH for Hadoop**  
Please log out and log in again with  “hadoopuser”

| ssh-keygen \-t rsa \-P "" cat \~/.ssh/id\_rsa.pub \>\> \~/.ssh/authorized\_keys chmod 0600 \~/.ssh/authorized\_keys  |
| :---- |

**Step 3: Installing Hadoop**   
You’ll visit the [Apache Hadoop Releases page](http://hadoop.apache.org/releases.html) to find the most recent stable release.  
![][image1]

**Copy link url for next step.**  
![][image2]

| wget [https://dlcdn.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz](https://dlcdn.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz) tar \-xzvf [hadoop-3.4.1.tar.gz](http://hadoop-3.4.1.tar.gz) sudo mv hadoop-3.4.1 /usr/local cd /usr/local sudo ln \-s hadoop-3.4.1 hadoop  sudo chown \-R hadoopuser:hadoopuser hadoop-3.4.1 hadoop   |
| :---- |

**Step 4: Set Environment Variables**  
**Edit \~/.bashrc file \-\>**  nano \~/.bashrc

and add these lines at the bottom of .bashrc file

| \#Java8 export JAVA\_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \#Set Hadoop-related environment variables  export HADOOP\_HOME=/usr/local/hadoop  \#Add Hadoop bin/ directory to PATH  export PATH=$PATH:$HADOOP\_HOME/bin:$HADOOP\_HOME/sbin export HADOOP\_COMMON\_LIB\_NATIVE\_DIR=$HADOOP\_HOME/lib/native export HADOOP\_OPTS="-Djava.library.path=$HADOOP\_HOME/lib/native" export LD\_LIBRARY\_PATH=$HADOOP\_HOME/lib/native:$LD\_LIBRARY\_PATH export HADOOP\_MAPRED\_HOME=$HADOOP\_HOME  |
| :---- |

**Edit the environment file \-\>**  sudo nano /etc/environment

| PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/lib/jvm/java-8-openjdk-amd64/bin" JAVA\_HOME="/usr/lib/jvm/java-8-openjdk-amd64"  |
| :---- |

**Save and then**

source \~/.bashrc  
**then**  
printenv

**PART 2: Configure Hadoop (Pseudo-distributed Mode)**

**ปรับปรุงคอนฟิกของ Hadoop:  (เปลี่ยน IP Address ตามสภาพแวดล้องของคุณ)**

**core-site.xml**  
**Edit \-\>**  nano $HADOOP\_HOME/etc/hadoop/core-site.xml

|  \<configuration\>   \<property\>     \<name\>fs.defaultFS\</name\>     \<value\>hdfs://192.168.1.33:9000\</value\>   \</property\>   \<property\>     \<name\>hadoop.tmp.dir\</name\>     \<value\>/home/hadoopuser/hadoop\_tmp\</value\>     \<description\>A base for other temporary directories.\</description\>   \</property\> \</configuration\>  |
| :---- |

สร้าง directory ที่กำหนดใน hadoop.tmp.dir:  
mkdir \-p /home/hadoopuser/hadoop\_tmp \# หรือ path ที่คุณกำหนด

**hdfs-site.xml**  
**Edit \-\>**  nano $HADOOP\_HOME/etc/hadoop/hdfs-site.xml

|  \<configuration\>    \<property\>         \<name\>dfs.replication\</name\>         \<value\>1\</value\>         \<description\>Default block replication. Use 1 for pseudo-distributed mode.\</description\>     \</property\>     \<property\>         \<name\>dfs.namenode.name.dir\</name\>         \<value\>file:///home/hadoopuser/hadoop\_store/hdfs/namenode\</value\>     \</property\>     \<property\>         \<name\>dfs.datanode.data.dir\</name\>         \<value\>file:///home/hadoopuser/hadoop\_store/hdfs/datanode\</value\>    \</property\>    \<property\>        \<name\>dfs.namenode.http-address\</name\>        \<value\>192.168.1.33:9870\</value\>    \</property\> \</configuration\>  |
| :---- |

สร้าง directories ที่กำหนด:  
mkdir \-p /home/hadoopuser/hadoop\_store/hdfs/namenode  
mkdir \-p /home/hadoopuser/hadoop\_store/hdfs/datanode

**mapred-site.xml**  
**Edit \-\>**  nano $HADOOP\_HOME/etc/hadoop/mapred-site.xml

|  \<configuration\>    \<property\>         \<name\>mapreduce.framework.name\</name\>         \<value\>yarn\</value\>         \<description\>The runtime framework for executing MapReduce jobs. Can be local, classic or yarn.\</description\>     \</property\>     \<property\>        \<name\>mapreduce.jobhistory.address\</name\>        \<value\>192.168.1.33:10020\</value\>     \</property\>     \<property\>        \<name\>mapreduce.jobhistory.webapp.address\</name\>        \<value\>192.168.1.33:19888\</value\>     \</property\> \</configuration\>  |
| :---- |

**yarn-site.xml**  
**Edit \-\>**  nano $HADOOP\_HOME/etc/hadoop/yarn-site.xml

|  \<configuration\>     \<property\>         \<name\>yarn.nodemanager.aux-services\</name\>         \<value\>mapreduce\_shuffle\</value\>         \<description\>Tell NodeManager that shuffle service is available.\</description\>     \</property\>     \<property\>         \<name\>yarn.nodemanager.aux-services.mapreduce\_shuffle.class\</name\>         \<value\>org.apache.hadoop.mapred.ShuffleHandler\</value\>     \</property\>     \<property\>         \<name\>yarn.resourcemanager.hostname\</name\>         \<value\>192.168.1.33\</value\>     \</property\>     \<property\>         \<name\>yarn.resourcemanager.address\</name\>         \<value\>192.168.1.33:8032\</value\> \</property\>     \<property\>         \<name\>yarn.resourcemanager.scheduler.address\</name\>         \<value\>192.168.1.33:8030\</value\>     \</property\>     \<property\>         \<name\>yarn.resourcemanager.resource-tracker.address\</name\>         \<value\>192.168.1.33:8031\</value\>     \</property\>     \<property\>         \<name\>yarn.resourcemanager.admin.address\</name\>         \<value\>192.168.1.33:8033\</value\>     \</property\>     \<property\>         \<name\>yarn.resourcemanager.webapp.address\</name\>         \<value\>192.168.1.33:8088\</value\>      \</property\> \</configuration\>  |
| :---- |

**PART 3: Format and Start Hadoop**

**Step 1: Format HDFS NameNode**  
Formatting the HDFS filesystem via the NameNode

| hdfs namenode \-format  |
| :---- |

**Step 2: Start Hadoop Daemons**

| start-dfs.sh  |
| :---- |

**Step 3: Start YARN Daemons (ResourceManager, NodeManager):**

| start-yarn.sh  |
| :---- |

**Step 4: Job History Server:(Optional):**

| mapred \--daemon start historyserver  |
| :---- |

**Step 5: ตรวจสอบการทำงาน**  
	ใช้คำสั่ง jps (Java Virtual Machine Process Status Tool):

| jps  |
| :---- |

คุณควรจะเห็น process ต่างๆ เช่น:

* NameNode  
* DataNode  
* SecondaryNameNode  
* ResourceManager  
* NodeManager  
* JobHistoryServer (ถ้ามี)

![][image3]

**Step 6: เข้า Web UI ผ่านเบราว์เซอร์ (จากเครื่องอื่นนอก VM):**

* HDFS NameNode: [http://192.168.1.33:9870](http://192.168.1.33:9870) (อาจเป็น 50070 ใน Hadoop เวอร์ชันเก่า)  
* YARN ResourceManager: [http://192.168.1.33:8088](http://192.168.1.33:8088)  
* JobHistoryServer: [http://192.168.1.33:19888](http://192.168.1.33:19888)

  ถ้าหน้าเว็บโหลดได้และแสดงข้อมูล แสดงว่า Hadoop ทำงานถูกต้อง

**HDFS NameNode:** [http://192.168.1.33:9870](http://192.168.1.33:9870)  
![][image4]

**YARN ResourceManager:** [http://192.168.1.33:8088](http://192.168.1.33:8088)  
![][image5]

**JobHistoryServer:** [http://192.168.1.33:19888](http://192.168.1.33:19888)  
![][image6]

ถ้าทำงานไม่ได้ให้ดู log ว่าเกิด error ตรงไหนด้วยคำสั่งนี้

tail \-n 100 $HADOOP\_HOME/logs/hadoop-hadoopuser-namenode-$(hostname).log  
\# Replace 'hadoopuser' with your Hadoop username if different  
\# Replace '$(hostname)' with your actual server hostname if the command doesn't expand it correctly  
\# Or, more generally:  
\# ls \-lt $HADOOP\_HOME/logs/ \# Find the most recent namenode log  
\# cat $HADOOP\_HOME/logs/\<your-namenode-log-file.log\> | grep ERROR

**Step 7: Stop Hadoop and other Daemons**

| stop-yarn.sh stop-dfs.sh \#ถ้า start ไว้ ก็ stop ด้วย mapred \--daemon stop historyserver  |
| :---- |

**PART 4: Start working with Hadoop MapReduce**

**Step 1: Basic HDFS command on the server**

| \#แสดงสถานะของ hdfs hdfs dfsadmin \-report \#แสดงข้อมูลใน hdfs  hdfs dfs \-ls /  |
| :---- |

**Step 2: Upload Log Files to HDFS**

|  hdfs dfs \-mkdir \-p /logs sudo cp /var/log/syslog . sudo chown $USER:$USER syslog chmod 755 syslog hdfs dfs \-put ./syslog /logs/ hdfs dfs \-ls /logs  |
| :---- |

**Step 3: Analyze Logs Using MapReduce**  
Python Code (using Hadoop Streaming)

**Example:** Count unique IPs in logs  
NASA HTTP Web Server Log from July 1995, which is often used in log analysis and Hadoop demos: Download (Approx. 20MB uncompressed)

**Download a Large Log File (Public Dataset)**

| mkdir works cd works wget https://ita.ee.lbl.gov/traces/NASA\_access\_log\_Jul95.gz gunzip NASA\_access\_log\_Jul95.gz  |
| :---- |

**Put the File into HDFS**

|  hdfs dfs \-mkdir \-p /logs hdfs dfs \-put NASA\_access\_log\_Jul95 /logs/  |
| :---- |

**Step 4: Python Code Examples for MapReduce**

**mapper.py**

| \#\!/usr/bin/env python3 import sys import io \# Required for TextIOWrapper def run\_mapper():     for line\_num, raw\_line in enumerate(sys.stdin): \# sys.stdin is now the reconfigured wrapper         try:             \# raw\_line is already decoded by the TextIOWrapper             line \= raw\_line.strip()                          if not line: \# Skip fully empty or whitespace-only lines                 \# print(f"DEBUG: Mapper skipping empty line {line\_num+1}", file=sys.stderr)                 continue                          parts \= line.split(' ')                           if not parts or not parts\[0\]: \# Check if parts is empty or first part is empty                 \# print(f"DEBUG: Mapper skipping line {line\_num+1} with no IP: '{raw\_line.strip()}'", file=sys.stderr)                 continue             ip\_address \= parts\[0\]             \# The print function will encode this string to UTF-8 by default for stdout,             \# which is what Hadoop Streaming generally expects.             print(f'{ip\_address}\\t1')         except Exception as e:             \# raw\_line here would be the string decoded using latin-1             print(f"MAPPER ERROR: Failed processing line {line\_num+1}: '{raw\_line.strip()}'", file=sys.stderr)             print(f"MAPPER EXCEPTION: {e}", file=sys.stderr) if \_\_name\_\_ \== "\_\_main\_\_":     \# Reconfigure sys.stdin to read with 'latin-1' encoding.     \# This will treat all bytes as valid characters from the Latin-1 set.     sys.stdin \= io.TextIOWrapper(sys.stdin.buffer, encoding='latin-1')          \# Optional: If you want to be sure about output encoding for Hadoop,     \# though print() usually handles this well by defaulting to UTF-8 on Linux.     \# sys.stdout \= io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')          run\_mapper() |
| :---- |

**reducer.py**

| \#\!/usr/bin/env python3 import sys import io \# Required for TextIOWrapper def run\_reducer():     current\_ip \= None     current\_count \= 0     for line\_num, raw\_line in enumerate(sys.stdin): \# sys.stdin is now the reconfigured wrapper         try:             \# raw\_line is already decoded by the TextIOWrapper             line \= raw\_line.strip()             \# Split the input line (ip\_address \<tab\> count\_str)             ip\_address, count\_str \= line.split('\\t', 1\)             count \= int(count\_str)             if current\_ip \== ip\_address:                 current\_count \+= count             else:                 if current\_ip:                     \# Output the previous IP's count                     print(f'{current\_ip}\\t{current\_count}')                                  current\_ip \= ip\_address                 current\_count \= count         except ValueError:             \# This error is for when int(count\_str) fails or line.split fails             print(f"REDUCER ERROR: ValueError on line {line\_num+1} (decoded): '{raw\_line.strip()}'", file=sys.stderr)             continue \# Skip malformed lines         except Exception as e:             print(f"REDUCER ERROR: General exception on line {line\_num+1} (decoded): '{raw\_line.strip()}'", file=sys.stderr)             print(f"REDUCER EXCEPTION: {e}", file=sys.stderr)             continue \# Skip lines that cause other errors     \# Output the last IP address count     if current\_ip:         print(f'{current\_ip}\\t{current\_count}') if \_\_name\_\_ \== "\_\_main\_\_":     \# Reconfigure sys.stdin for the reducer as well.     sys.stdin \= io.TextIOWrapper(sys.stdin.buffer, encoding='latin-1')          \# Optional: Reconfigure sys.stdout for the reducer.     \# sys.stdout \= io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')          run\_reducer() |
| :---- |

**There are 2 methods to RUN:**  
**Method 1**

| chmod \+x mapper.py reducer.py hadoop fs \-rm \-r \-f /output/ip\_counts hadoop jar $HADOOP\_HOME/share/hadoop/tools/lib/hadoop-streaming-\*.jar \\     \-files $(pwd)/mapper.py,$(pwd)/reducer.py \\     \-mapper "python mapper.py" \\     \-reducer "python reducer.py" \\     \-input /logs/NASA\_access\_log\_Jul95 \\     \-output /output/ip\_counts  |
| :---- |

**Check Hadoop output with:**

| hdfs dfs \-ls /output/ip\_counts hdfs dfs \-cat /output/ip\_counts/part-\* |
| :---- |

**Method 2**  
**Create a script and run it:**

| chmod \+x mapper.py reducer.py nano run\_mapreduce.sh chmod 755 run\_mapreduce.sh ./run\_mapreduce.sh |
| :---- |

**run\_mapreduce.sh**

| \#\!/bin/bash \# Simple shell script to run the MapReduce job \# Ensure we have HADOOP\_HOME set if \[ \-z "$HADOOP\_HOME" \]; then     echo "HADOOP\_HOME is not set. Please set it to your Hadoop installation directory."     exit 1 fi \# Input and output paths INPUT\_PATH="/logs/NASA\_access\_log\_Jul95" OUTPUT\_PATH="/output/ip\_counts" \# Remove output directory if it exists hadoop fs \-rm \-r \-f $OUTPUT\_PATH \# Run the Hadoop streaming job hadoop jar $HADOOP\_HOME/share/hadoop/tools/lib/hadoop-streaming-\*.jar \\     \-files $(pwd)/mapper.py,$(pwd)/reducer.py \\     \-mapper "python mapper.py" \\     \-reducer "python reducer.py" \\     \-input $INPUT\_PATH \\     \-output $OUTPUT\_PATH \# Show the result echo "Job completed. Results:" hadoop fs \-cat $OUTPUT\_PATH/part-\* |
| :---- |

![][image7]

**Step 5: Post-Job CSV Export Script**

**Saving raw output to other file (\*.tsv) which used for convert to \*.csv** 

| hdfs dfs \-cat /output/ip\_counts/part-\* \> ip\_counts.tsv |
| :---- |

**Then use this script to convert to CSV:**     
**Edit \-\>**   nano convert\_to\_csv.py

| \# convert\_to\_csv.py import pandas as pd df \= pd.read\_csv("ip\_counts.tsv", sep='\\t', names=\['IP', 'Count'\]) df.to\_csv("ip\_frequency.csv", index=False) |
| :---- |

**Create a script and run it:**

| chmod \+x convert\_to\_csv.py python3 convert\_to\_csv.py  |
| :---- |

**Here’s a version with improved y-axis scaling and visualization clarity::**      
**Edit \-\>**   nano plot\_ip\_frequency.py

| import pandas as pd import matplotlib.pyplot as plt \# Load CSV file df \= pd.read\_csv("ip\_frequency.csv") \# Sort by request count, descending, and select top N top\_n \= 20 df \= df.sort\_values(by='Count', ascending=False).head(top\_n) \# Plot plt.figure(figsize=(14, 8)) bars \= plt.bar(df\['IP'\], df\['Count'\], color='skyblue', edgecolor='black') \# Y-axis label and custom limit plt.ylabel('Number of Requests') plt.xlabel('IP Address') plt.title(f'Top {top\_n} IP Addresses by Request Count') \# Set dynamic y-axis limit (10% above max for padding) plt.ylim(0, df\['Count'\].max() \* 1.10) \# Add number labels above bars for bar in bars:     height \= bar.get\_height()     plt.text(bar.get\_x() \+ bar.get\_width() / 2, height \+ 1, str(height),              ha='center', va='bottom', fontsize=9) \# Improve x-label visibility plt.xticks(rotation=45, ha='right') \# Tight layout for clarity plt.tight\_layout() \# Save and show plt.savefig("top\_ip\_requests.png", dpi=300) \#plt.show() |
| :---- |

**Key Improvements**  
plt.ylim(0, df\['Count'\].max() \* 1.10) ensures space above tallest bar.  
plt.figure(figsize=(14, 8)) increases visual size.  
plt.text(..., str(height)) shows the actual number on each bar.  
plt.xticks(rotation=45) makes long IPs readable.

**Create a script and run it:**

|  chmod \+x plot\_ip\_frequency.py python3 plot\_ip\_frequency.py  |
| :---- |

Your will see a  ”top\_ip\_requests.png” file.  
![][image8]

Now, you can transfer a file from VM using **Terminus (SFTP)** to your local computer and open it.  
![][image9]

![][image10]

**END OF LAB1**
