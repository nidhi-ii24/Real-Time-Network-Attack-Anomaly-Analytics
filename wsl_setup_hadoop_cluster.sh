#!/bin/bash
# ==============================================================================
# Automated WSL Ubuntu Single-Node Hadoop & Big Data Cluster Setup Script
# Course: Big Data Analytics (26ECSC404) - KLE Technological University
# Run inside your Ubuntu WSL terminal: bash wsl_setup_hadoop_cluster.sh
# ==============================================================================

set -e

echo "=================================================================="
echo " 1. INSTALLING PREREQUISITES (JAVA 11, SSH, PDSH, BUILD UTILS)"
echo "=================================================================="
sudo apt update
sudo apt install -y openjdk-11-jdk ssh pdsh rsync curl wget maven python3 python3-pip

# Detect Java Home
JAVA_DETECTED=$(readlink -f /usr/bin/java | sed "s:/bin/java::")
echo "[+] Detected JAVA_HOME: $JAVA_DETECTED"

echo -e "\n=================================================================="
echo " 2. CONFIGURING PASSWORDLESS SSH FOR HADOOP DAEMONS"
echo "=================================================================="
sudo service ssh start || true

if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
fi

cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
chmod 0700 ~/.ssh

# Prevent strict host key checking prompt during daemon start
cat << 'EOF' > ~/.ssh/config
Host localhost
    StrictHostKeyChecking no
Host 0.0.0.0
    StrictHostKeyChecking no
EOF
chmod 0600 ~/.ssh/config

echo "[+] Verifying SSH localhost connection..."
ssh -o BatchMode=yes localhost "echo 'SSH localhost successful!'"

echo -e "\n=================================================================="
echo " 3. DOWNLOADING & INSTALLING APACHE HADOOP 3.3.6"
echo "=================================================================="
HADOOP_DIR="/usr/local/hadoop"

if [ ! -d "$HADOOP_DIR" ]; then
    echo "[+] Downloading Apache Hadoop 3.3.6..."
    cd /tmp
    if [ ! -f "hadoop-3.3.6.tar.gz" ]; then
        wget -q --show-progress https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
    fi
    echo "[+] Extracting Hadoop to /usr/local/hadoop..."
    sudo tar -xzf hadoop-3.3.6.tar.gz -C /usr/local/
    sudo mv /usr/local/hadoop-3.3.6 $HADOOP_DIR
    sudo chown -R $USER:$USER $HADOOP_DIR
    cd -
else
    echo "[+] Hadoop is already installed at $HADOOP_DIR"
fi

echo -e "\n=================================================================="
echo " 4. CONFIGURING HADOOP XML CONFIGURATIONS"
echo "=================================================================="
HADOOP_CONF="$HADOOP_DIR/etc/hadoop"

# A. hadoop-env.sh
sed -i '/export JAVA_HOME/d' "$HADOOP_CONF/hadoop-env.sh"
echo "export JAVA_HOME=$JAVA_DETECTED" >> "$HADOOP_CONF/hadoop-env.sh"
echo "export HADOOP_HOME=$HADOOP_DIR" >> "$HADOOP_CONF/hadoop-env.sh"

# B. core-site.xml
cat << 'EOF' > "$HADOOP_CONF/core-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOF

# Create HDFS Data Directories
mkdir -p ~/hadoop_data/namenode
mkdir -p ~/hadoop_data/datanode

# C. hdfs-site.xml (Aligns with Course requirement for dfs.replication)
cat << EOF > "$HADOOP_CONF/hdfs-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file://$HOME/hadoop_data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file://$HOME/hadoop_data/datanode</value>
    </property>
</configuration>
EOF

# D. mapred-site.xml
cat << 'EOF' > "$HADOOP_CONF/mapred-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>
</configuration>
EOF

# E. yarn-site.xml
cat << 'EOF' > "$HADOOP_CONF/yarn-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
</configuration>
EOF

echo -e "\n=================================================================="
echo " 5. CONFIGURING SHELL ENVIRONMENT (~/.bashrc)"
echo "=================================================================="
if ! grep -q "HADOOP_HOME" ~/.bashrc; then
    cat << EOF >> ~/.bashrc

# Big Data Analytics Cluster Environment (Hadoop & Java)
export JAVA_HOME=$JAVA_DETECTED
export HADOOP_HOME=$HADOOP_DIR
export HADOOP_INSTALL=\$HADOOP_HOME
export HADOOP_MAPRED_HOME=\$HADOOP_HOME
export HADOOP_COMMON_HOME=\$HADOOP_HOME
export HADOOP_HDFS_HOME=\$HADOOP_HOME
export YARN_HOME=\$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native
export PATH=\$PATH:\$HADOOP_HOME/sbin:\$HADOOP_HOME/bin:\$JAVA_HOME/bin
export PDSH_RCMD_TYPE=ssh
EOF
fi

# Export for current script execution
export JAVA_HOME=$JAVA_DETECTED
export HADOOP_HOME=$HADOOP_DIR
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$JAVA_HOME/bin
export PDSH_RCMD_TYPE=ssh

echo -e "\n=================================================================="
echo " 6. FORMATTING HDFS NAMENODE & STARTING DAEMONS"
echo "=================================================================="
# Format NameNode if not already formatted
if [ ! -d "$HOME/hadoop_data/namenode/current" ]; then
    echo "[+] Formatting HDFS NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format -force
fi

echo "[+] Starting HDFS Daemons (NameNode, DataNode, SecondaryNameNode)..."
$HADOOP_HOME/sbin/start-dfs.sh

echo "[+] Starting YARN Daemons (ResourceManager, NodeManager)..."
$HADOOP_HOME/sbin/start-yarn.sh

echo -e "\n=================================================================="
echo " 7. VERIFYING CLUSTER WITH 'jps'"
echo "=================================================================="
jps

echo -e "\n[SUCCESS] Single-Node Hadoop Cluster is UP and RUNNING in WSL!"
echo "NameNode Web UI:      http://localhost:9870"
echo "YARN ResourceManager: http://localhost:8088"
echo "Next: run 'bash wsl_run_pipeline.sh' to execute the full BDA pipeline!"
