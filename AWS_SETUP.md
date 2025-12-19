# Running EastStorm on AWS EC2

## Prerequisites

1. **EC2 Instance Requirements:**
   - Ubuntu 20.04 or later (or Amazon Linux 2)
   - At least 2GB RAM
   - Java 17 or higher
   - Security groups configured (see below)

2. **Security Group Configuration:**
   Open these ports in your EC2 security group:
   - **8000** (KVS Coordinator)
   - **8001** (KVS Worker)
   - **9000** (Flame Coordinator)
   - **9001** (Flame Worker)
   - **8080** (SearchServer - optional, for web interface)
   - **22** (SSH - already open)

## Step 1: Transfer Code to EC2

From your local machine:

```bash
# Replace with your EC2 instance details
EC2_HOST="ec2-44-221-62-48.compute-1.amazonaws.com"  # or use IP address
EC2_USER="ec2-user"  # or "ec2-user" for Amazon Linux

# Create a tarball of the project (excluding generated files)
cd /Users/xichengmiao/Desktop/upenn/MSE-AI/CIS\ 555/25fa-CIS5550-G05-EastStorm
tar --exclude='bin' --exclude='lib/*.jar' --exclude='worker*' --exclude='*.class' \
    --exclude='.git' --exclude='.DS_Store' --exclude='*.pem' \
    -czf eaststorm.tar.gz src/ README .gitignore

# Transfer to EC2
scp -i 555-project.pem eaststorm.tar.gz ${EC2_USER}@${EC2_HOST}:~/

# SSH into EC2
ssh -i 555-project.pem ${EC2_USER}@${EC2_HOST}
```

## Step 2: Update Code on EC2 (After Making Changes Locally)

If you've made code changes locally and need to update the EC2 instance:

**From your local machine:**

```bash
# Set your EC2 details
EC2_HOST="ec2-44-221-62-48.compute-1.amazonaws.com"  # or use IP: 44.221.62.48
EC2_USER="ec2-user"
KEY_FILE="555-project.pem"  # path to your key file

# Create a tarball of updated source code (excluding generated files)
cd /Users/xichengmiao/Desktop/upenn/MSE-AI/CIS\ 555/25fa-CIS5550-G05-EastStorm
tar --exclude='bin' --exclude='lib/*.jar' --exclude='worker*' --exclude='*.class' \
    --exclude='.git' --exclude='.DS_Store' --exclude='*.pem' \
    -czf eaststorm-update.tar.gz src/ README .gitignore

# Transfer to EC2
scp -i ${KEY_FILE} eaststorm-update.tar.gz ${EC2_USER}@${EC2_HOST}:~/

# SSH into EC2
ssh -i ${KEY_FILE} ${EC2_USER}@${EC2_HOST}
```

**On the EC2 instance:**

```bash
# Navigate to your project directory
cd ~/eaststorm  # or wherever you extracted it before

# Backup current code (optional)
cp -r src src.backup

# Extract updated code
tar -xzf ~/eaststorm-update.tar.gz

# Recompile
javac -cp "lib/*:bin" -d bin src/cis5550/**/*.java

# Rebuild JARs
jar cf lib/webserver.jar -C bin cis5550/webserver
jar cf lib/flame.jar -C bin cis5550/flame
jar cf lib/kvs.jar -C bin cis5550/kvs -C bin cis5550/generic -C bin cis5550/tools
jar cf jobs.jar -C bin cis5550/jobs

# If services are running, you may need to restart them:
# 1. Stop all services (Ctrl+C in each terminal/screen session)
# 2. Restart services (see Step 4 below)
```

**Quick update script (optional):**

You can create a script on EC2 to make updates easier:

```bash
# On EC2, create ~/update-code.sh
cat > ~/update-code.sh << 'EOF'
#!/bin/bash
cd ~/eaststorm
tar -xzf ~/eaststorm-update.tar.gz
javac -cp "lib/*:bin" -d bin src/cis5550/**/*.java
jar cf lib/webserver.jar -C bin cis5550/webserver
jar cf lib/flame.jar -C bin cis5550/flame
jar cf lib/kvs.jar -C bin cis5550/kvs -C bin cis5550/generic -C bin cis5550/tools
jar cf jobs.jar -C bin cis5550/jobs
echo "Code updated and recompiled!"
EOF

chmod +x ~/update-code.sh

# Then after transferring new code, just run:
~/update-code.sh
```

## Step 2: Setup on EC2 (Initial Setup)

Once on the EC2 instance:

```bash
# Extract the code
tar -xzf eaststorm.tar.gz
cd eaststorm  # or whatever directory name you used

# Install Java 17 (if not already installed)
# For Amazon Linux 2:
sudo yum update -y
sudo yum install -y java-17-amazon-corretto-devel
# OR for Ubuntu:
# sudo apt update
# sudo apt install -y openjdk-17-jdk

# Verify Java version
java -version  # Should show version 17 or higher
```

## Step 3: Compile on EC2

```bash
# Create necessary directories
mkdir -p bin lib

# Step 1: Compile all Java source files
javac -cp "lib/*:bin" -d bin src/cis5550/**/*.java

# Step 2: Build webserver.jar
jar cf lib/webserver.jar -C bin cis5550/webserver

# Step 3: Build flame.jar
jar cf lib/flame.jar -C bin cis5550/flame

# Step 4: Build kvs.jar
jar cf lib/kvs.jar -C bin cis5550/kvs -C bin cis5550/generic -C bin cis5550/tools

# Step 5: Build jobs.jar
jar cf jobs.jar -C bin cis5550/jobs
```

## Step 4: Run the System

You'll need multiple terminal sessions. Use `screen` or `tmux` to manage them:

```bash
# Install screen (if not available)
sudo yum install screen -y  # Amazon Linux
# OR for Ubuntu:
# sudo apt install screen

# Start a screen session
screen -S eaststorm
```

### Terminal 1: KVS Coordinator
```bash
java -cp "lib/*:bin" cis5550.kvs.Coordinator 8000
```

### Terminal 2: KVS Worker
```bash
# In a new screen window (Ctrl+A, then C) or new SSH session
rm -rf worker1
java -cp "lib/*:bin" cis5550.kvs.Worker 8001 worker1 localhost:8000
```

### Terminal 3: Flame Coordinator
```bash
# In a new screen window or new SSH session
java -cp "lib/*:bin" cis5550.flame.Coordinator 9000 localhost:8000
```

### Terminal 4: Flame Worker
```bash
# In a new screen window or new SSH session
java -cp "lib/*:bin" cis5550.flame.Worker 9001 localhost:9000
```

## Step 5: Run the Crawler

Once all 4 services are running:

```bash
# Run the crawler with multiple seeds from different topics (RECOMMENDED)
# This provides better topic diversity for ranking and search
java -cp "lib/*:bin" cis5550.flame.FlameSubmit localhost:9000 jobs.jar cis5550.jobs.Crawler \
  https://en.wikipedia.org/wiki/Computer_science \
  https://en.wikipedia.org/wiki/Film \
  https://en.wikipedia.org/wiki/Sport \
  https://en.wikipedia.org/wiki/Technology \
  https://en.wikipedia.org/wiki/Science \
  https://en.wikipedia.org/wiki/History \
  https://en.wikipedia.org/wiki/Mathematics \
  https://en.wikipedia.org/wiki/Medicine

# Alternative: Single seed (for testing)
# java -cp "lib/*:bin" cis5550.flame.FlameSubmit localhost:9000 jobs.jar cis5550.jobs.Crawler https://en.wikipedia.org/wiki/Java

# Check progress
java -cp "lib/*:bin" cis5550.kvs.KVSClient localhost:8000 count pt-crawl
```

## Step 6: View Tables in Web Browser

You can view the crawled data in your web browser:

**For local testing:**
- List all tables: http://localhost:8001/
- View pt-crawl table: http://localhost:8001/view/pt-crawl
- View pt-index table: http://localhost:8001/view/pt-index

**For AWS EC2:**
- Replace `localhost` with your EC2 instance's public IP
- Example: http://44.221.62.48:8001/view/pt-crawl
- **IMPORTANT:** Make sure your security group allows inbound traffic on ports 8001-8003
  - Go to EC2 → Instances → Your instance → Security tab → Security group
  - Edit inbound rules → Add rules for ports 8001, 8002, 8003, Source: My IP (or 0.0.0.0/0)
  - See "Troubleshooting" section below for detailed steps

**Important: With Multiple Workers:**
- Data is distributed across workers using consistent hashing
- Each worker only shows its own subset of data
- To see all pages, access each worker separately:
  - Worker 1: http://<EC2_IP>:8001/view/pt-crawl (shows ~23,000 pages)
  - Worker 2: http://<EC2_IP>:8002/view/pt-crawl (shows its subset)
  - Worker 3: http://<EC2_IP>:8003/view/pt-crawl (shows its subset)
- **To see total count across all workers**, use command line:
  ```bash
  java -cp "lib/*:bin" cis5550.kvs.KVSClient localhost:8000 count pt-crawl
  ```
- The KVSClient automatically aggregates data from all workers

The web interface shows:
- Total row count for each table (per worker)
- Paginated view of rows (click "Next" to see more)
- All columns for each row (url, responseCode, contentType, length, page, title)

## Step 6.5: Delete Existing Data (Optional - Start Fresh)

If you want to delete existing crawled data and start over (e.g., after code changes):

```bash
# Delete pt-crawl table
java -cp "lib/*:bin" cis5550.kvs.KVSClient localhost:8000 delete pt-crawl

# Delete pt-index table
java -cp "lib/*:bin" cis5550.kvs.KVSClient localhost:8000 delete pt-index

# Delete worker data directory (optional, but recommended for clean start)
rm -rf worker1

# Restart KVS Worker (it will recreate worker1 directory)
# Stop the current Worker (Ctrl+C), then restart:
java -cp "lib/*:bin" cis5550.kvs.Worker 8001 worker1 localhost:8000
```

**Note:** Make sure all services (KVS Coordinator, KVS Worker, Flame Coordinator, Flame Worker) are running before deleting tables.

## Step 7: Run Indexer and SearchServer

```bash
# Run the indexer
java -cp "lib/*:bin" cis5550.flame.FlameSubmit localhost:9000 jobs.jar cis5550.jobs.Indexer

# Start the search server (optional)
java -cp "lib/*:bin" cis5550.jobs.SearchServer
# Then access at http://<EC2_PUBLIC_IP>:8080
```

## Resuming After Disconnection

If your SSH connection dropped and you've reconnected:

### Step 1: Check if Services Are Still Running

```bash
# Check if Java processes are still running
ps aux | grep java

# Or check specific ports
sudo lsof -i :8000  # KVS Coordinator
sudo lsof -i :8001  # KVS Worker 1
sudo lsof -i :8002  # KVS Worker 2 (if running)
sudo lsof -i :9000  # Flame Coordinator
sudo lsof -i :9001  # Flame Worker 1
```

### Step 2: Reattach to Screen Session (If Using Screen)

```bash
# List screen sessions
screen -ls

# Reattach to your session
screen -r eaststorm

# If you see "Attached", use:
screen -d -r eaststorm  # Detach and reattach
```

### Step 3: Restart Services (If They Stopped)

If services stopped when connection dropped, restart them:

```bash
# Navigate to project directory
cd ~/eaststorm

# Start screen session
screen -S eaststorm

# In separate windows (Ctrl+A, then C for new window):
# Window 1: KVS Coordinator
java -cp "lib/*:bin" cis5550.kvs.Coordinator 8000

# Window 2: KVS Worker 1
java -cp "lib/*:bin" cis5550.kvs.Worker 8001 worker1 localhost:8000

# Window 3: KVS Worker 2 (if using multiple workers)
java -cp "lib/*:bin" cis5550.kvs.Worker 8002 worker2 localhost:8000

# Window 4: KVS Worker 3 (if using multiple workers)
java -cp "lib/*:bin" cis5550.kvs.Worker 8003 worker3 localhost:8000

# Window 5: Flame Coordinator
java -cp "lib/*:bin" cis5550.flame.Coordinator 9000 localhost:8000

# Window 6: Flame Worker 1
java -cp "lib/*:bin" cis5550.flame.Worker 9001 localhost:9000

# Window 7: Flame Worker 2 (if using multiple workers)
java -cp "lib/*:bin" cis5550.flame.Worker 9002 localhost:9000

# Window 8: Flame Worker 3 (if using multiple workers)
java -cp "lib/*:bin" cis5550.flame.Worker 9003 localhost:9000
```

### Step 4: Check Crawler Status

```bash
# Check how many pages have been crawled
java -cp "lib/*:bin" cis5550.kvs.KVSClient localhost:8000 count pt-crawl
```

### Step 5: Resume Crawler (If It Stopped)

**The crawler automatically skips already-crawled pages**, so you can safely restart it:

```bash
# Resume crawling with the same seeds
java -cp "lib/*:bin" cis5550.flame.FlameSubmit localhost:9000 jobs.jar cis5550.jobs.Crawler \
  https://en.wikipedia.org/wiki/Computer_science \
  https://en.wikipedia.org/wiki/Film \
  https://en.wikipedia.org/wiki/Sport \
  https://en.wikipedia.org/wiki/Technology \
  https://en.wikipedia.org/wiki/Science \
  https://en.wikipedia.org/wiki/History \
  https://en.wikipedia.org/wiki/Mathematics \
  https://en.wikipedia.org/wiki/Medicine
```

**Note:** The crawler will:
- Skip all already-crawled pages (your existing 55,931 pages)
- Continue crawling new pages from the frontier
- Resume from where it left off

### Running in Background (Alternative)

To prevent services from stopping on disconnect, use `nohup`:

```bash
# Run services in background with nohup
nohup java -cp "lib/*:bin" cis5550.kvs.Coordinator 8000 > kvs-coord.log 2>&1 &
nohup java -cp "lib/*:bin" cis5550.kvs.Worker 8001 worker1 localhost:8000 > kvs-worker1.log 2>&1 &
nohup java -cp "lib/*:bin" cis5550.kvs.Worker 8002 worker2 localhost:8000 > kvs-worker2.log 2>&1 &
nohup java -cp "lib/*:bin" cis5550.kvs.Worker 8003 worker3 localhost:8000 > kvs-worker3.log 2>&1 &
nohup java -cp "lib/*:bin" cis5550.flame.Coordinator 9000 localhost:8000 > flame-coord.log 2>&1 &
nohup java -cp "lib/*:bin" cis5550.flame.Worker 9001 localhost:9000 > flame-worker1.log 2>&1 &
nohup java -cp "lib/*:bin" cis5550.flame.Worker 9002 localhost:9000 > flame-worker2.log 2>&1 &
nohup java -cp "lib/*:bin" cis5550.flame.Worker 9003 localhost:9000 > flame-worker3.log 2>&1 &

# Check logs
tail -f kvs-coord.log
tail -f flame-coord.log

# Check if processes are running
ps aux | grep java
```

## Using Screen/Tmux (Recommended)

To manage multiple terminals easily:

```bash
# Start screen
screen -S eaststorm

# Create new windows: Ctrl+A, then C
# Switch between windows: Ctrl+A, then N (next) or P (previous)
# Detach: Ctrl+A, then D
# Reattach: screen -r eaststorm

# Or use tmux:
tmux new -s eaststorm
# Create new panes: Ctrl+B, then %
# Switch panes: Ctrl+B, then arrow keys
```

## Accessing Web Interfaces

If you want to access the web interfaces from your local machine:

1. **Get EC2 Public IP:**
   ```bash
   # On EC2
   curl http://169.254.169.254/latest/meta-data/public-ipv4
   ```

2. **Access URLs:**
   - KVS Coordinator: `http://<EC2_PUBLIC_IP>:8000`
   - KVS Worker: `http://<EC2_PUBLIC_IP>:8001`
   - SearchServer: `http://<EC2_PUBLIC_IP>:8080`

3. **SSH Tunnel (Alternative - More Secure):**
   ```bash
   # On your local machine
   ssh -i 555-project.pem -L 8000:localhost:8000 -L 8001:localhost:8001 -L 8080:localhost:8080 ${EC2_USER}@${EC2_HOST}
   # Then access via localhost:8000, localhost:8001, etc.
   ```

## Monitoring System Resources

### Check Memory Usage
```bash
# Quick memory overview
free -h

# Detailed memory info
cat /proc/meminfo

# Real-time monitoring (updates every 2 seconds)
watch -n 2 free -h

# Using top (shows processes sorted by memory)
top
# Press 'M' to sort by memory usage, 'q' to quit
```

### Check CPU Usage
```bash
# CPU usage overview
top
# Or use htop (if installed): sudo yum install htop -y

# CPU info
lscpu
```

### Check Disk Usage
```bash
# Disk space
df -h

# Check specific directory size
du -sh worker1/
du -sh ~/eaststorm/
```

### Check Java Process Memory
```bash
# Find Java processes
ps aux | grep java

# Check Java heap usage (if using jstat)
jstat -gc <PID>  # Replace <PID> with process ID from ps aux

# Or use jps to find Java processes
jps -v
```

## Troubleshooting

### Security Group Configuration (IMPORTANT!)

If you get "Connection timed out" or "This site can't be reached" when accessing the web interface:

**You need to open ports in your EC2 Security Group:**

1. **Go to AWS Console:**
   - EC2 → Instances → Select your instance
   - Click on the "Security" tab
   - Click on the security group name (e.g., `sg-xxxxx`)

2. **Add Inbound Rules:**
   - Click "Edit inbound rules"
   - Click "Add rule" for each port:
     - **Port 8000** (KVS Coordinator): Type: Custom TCP, Port: 8000, Source: My IP (or 0.0.0.0/0 for testing)
     - **Port 8001** (KVS Worker): Type: Custom TCP, Port: 8001, Source: My IP (or 0.0.0.0/0 for testing)
     - **Port 9000** (Flame Coordinator): Type: Custom TCP, Port: 9000, Source: My IP (or 0.0.0.0/0 for testing)
     - **Port 9001** (Flame Worker): Type: Custom TCP, Port: 9001, Source: My IP (or 0.0.0.0/0 for testing)
     - **Port 8080** (SearchServer): Type: Custom TCP, Port: 8080, Source: My IP (or 0.0.0.0/0 for testing)
   - Click "Save rules"

3. **Verify services are running:**
   ```bash
   # SSH into your instance and check if services are running
   ps aux | grep java
   # Should see processes for Coordinator, Worker, etc.
   ```

4. **Test connectivity:**
   ```bash
   # From your local machine, test if port is open
   telnet 44.221.62.48 8001
   # Or use curl
   curl http://44.221.62.48:8001/
   ```

### Other Common Issues

1. **Port already in use:**
   ```bash
   # Find and kill processes
   pkill -f cis5550
   # Or find specific port
   sudo lsof -i :8000
   sudo kill -9 <PID>
   ```

2. **Connection refused (different from timeout):**
   - Verify all 4 services are running
   - Check firewall: `sudo ufw status` (Ubuntu) or `sudo firewall-cmd --list-all` (Amazon Linux)
   - Make sure services are bound to `0.0.0.0` not just `localhost`

3. **Out of memory:**
   ```bash
   # Check current memory
   free -h
   
   # Increase heap size for Java processes
   java -Xmx2g -cp "lib/*:bin" cis5550.kvs.Coordinator 8000
   java -Xmx2g -cp "lib/*:bin" cis5550.kvs.Worker 8001 worker1 localhost:8000
   java -Xmx2g -cp "lib/*:bin" cis5550.flame.Coordinator 9000 localhost:8000
   java -Xmx2g -cp "lib/*:bin" cis5550.flame.Worker 9001 localhost:9000
   ```

4. **Can't access from browser:**
   - Verify security group allows inbound traffic
   - Check EC2 instance has a public IP
   - Use SSH tunnel as alternative

## Running in Background (Optional)

To run services in background:

```bash
# Run in background with nohup
nohup java -cp "lib/*:bin" cis5550.kvs.Coordinator 8000 > kvs-coord.log 2>&1 &
nohup java -cp "lib/*:bin" cis5550.kvs.Worker 8001 worker1 localhost:8000 > kvs-worker.log 2>&1 &
nohup java -cp "lib/*:bin" cis5550.flame.Coordinator 9000 localhost:8000 > flame-coord.log 2>&1 &
nohup java -cp "lib/*:bin" cis5550.flame.Worker 9001 localhost:9000 > flame-worker.log 2>&1 &

# Check logs
tail -f kvs-coord.log
```

