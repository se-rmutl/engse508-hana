LAB1  Installing HDFS – Hadoop

PART 1: Installing Apache Hadoop on Ubuntu (Pseudo-distributed Mode)

Create a Ubuntu Server 22.04 LTS  (Virtual Machine)

Prerequisites
Installing Java 8, Hadoop, and Spark requires a working Java installation. We are using version Java 1.8, as it is known to be fully compatible:

JDK and Scala Install

Config java for jre



Install python3 pip and other libraries



Step 1: Create a Hadoop User

Step 2: Configure SSH for Hadoop
Please log out and log in again with  “hadoopuser”

Step 3: Installing Hadoop
You’ll visit the  to find the most recent stable release.


Copy link url for next step.





Step 4: Set Environment Variables
Edit ~/.bashrc file ->  nano ~/.bashrc
and add these lines at the bottom of .bashrc file

Edit the environment file ->  sudo nano /etc/environment

Save and then

source ~/.bashrc
then
printenv















PART 2: Configure Hadoop (Pseudo-distributed Mode)

ปรับปรุงคอนฟิกของ Hadoop:  (เปลี่ยน IP Address ตามสภาพแวดล้องของคุณ)

core-site.xml
Edit ->  nano $HADOOP_HOME/etc/hadoop/core-site.xml
สร้าง directory ที่กำหนดใน hadoop.tmp.dir:
mkdir -p /home/hadoopuser/hadoop_tmp # หรือ path ที่คุณกำหนด

hdfs-site.xml
Edit ->  nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml

สร้าง directories ที่กำหนด:
mkdir -p /home/hadoopuser/hadoop_store/hdfs/namenode
mkdir -p /home/hadoopuser/hadoop_store/hdfs/datanode





mapred-site.xml
Edit ->  nano $HADOOP_HOME/etc/hadoop/mapred-site.xml

yarn-site.xml
Edit ->  nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
PART 3: Format and Start Hadoop

Step 1: Format HDFS NameNode
Formatting the HDFS filesystem via the NameNode


Step 2: Start Hadoop Daemons

Step 3: Start YARN Daemons (ResourceManager, NodeManager):

Step 4: Job History Server:(Optional):

Step 5: ตรวจสอบการทำงาน
	ใช้คำสั่ง jps (Java Virtual Machine Process Status Tool):

คุณควรจะเห็น process ต่างๆ เช่น:

NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
JobHistoryServer (ถ้ามี)







Step 6: เข้า Web UI ผ่านเบราว์เซอร์ (จากเครื่องอื่นนอก VM):

HDFS NameNode:  (อาจเป็น 50070 ใน Hadoop เวอร์ชันเก่า)
YARN ResourceManager:
JobHistoryServer:
ถ้าหน้าเว็บโหลดได้และแสดงข้อมูล แสดงว่า Hadoop ทำงานถูกต้อง
HDFS NameNode:








YARN ResourceManager:



JobHistoryServer:


ถ้าทำงานไม่ได้ให้ดู log ว่าเกิด error ตรงไหนด้วยคำสั่งนี้

tail -n 100 $HADOOP_HOME/logs/hadoop-hadoopuser-namenode-$(hostname).log
# Replace 'hadoopuser' with your Hadoop username if different
# Replace '$(hostname)' with your actual server hostname if the command doesn't expand it correctly
# Or, more generally:
# ls -lt $HADOOP_HOME/logs/ # Find the most recent namenode log
# cat $HADOOP_HOME/logs/<your-namenode-log-file.log> | grep ERROR


Step 7: Stop Hadoop and other Daemons

PART 4: Start working with Hadoop MapReduce

Step 1: Basic HDFS command on the server


Step 2: Upload Log Files to HDFS


Step 3: Analyze Logs Using MapReduce
Python Code (using Hadoop Streaming)

Example: Count unique IPs in logs
NASA HTTP Web Server Log from July 1995, which is often used in log analysis and Hadoop demos: Download (Approx. 20MB uncompressed)

Download a Large Log File (Public Dataset)

Put the File into HDFS









Step 4: Python Code Examples for MapReduce

mapper.py









reducer.py









There are 2 methods to RUN:
Method 1

Check Hadoop output with:


Method 2
Create a script and run it:

run_mapreduce.sh




Step 5: Post-Job CSV Export Script

Saving raw output to other file (*.tsv) which used for convert to *.csv

Then use this script to convert to CSV:   
Edit ->   nano convert_to_csv.py

Create a script and run it:

Here’s a version with improved y-axis scaling and visualization clarity::    
Edit ->   nano plot_ip_frequency.py

Key Improvements
plt.ylim(0, df['Count'].max() * 1.10) ensures space above tallest bar.
plt.figure(figsize=(14, 8)) increases visual size.
plt.text(..., str(height)) shows the actual number on each bar.
plt.xticks(rotation=45) makes long IPs readable.


Create a script and run it:



Your will see a  ”top_ip_requests.png” file.


Now, you can transfer a file from VM using Terminus (SFTP) to your local computer and open it.



END OF LAB1