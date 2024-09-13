import subprocess
import psutil
import time
import csv

# Function to monitor and write data to CSV
def monitor_and_write_to_csv(process, sql_file, csv_writer,file):
    # Convert subprocess.Popen to psutil.Process for easier resource monitoring
    java_process = psutil.Process(process.pid)

    while True:
        # Check if the Java process has terminated
        if process.poll() is not None:
            print(f"The Java process for {sql_file} has terminated.")
            break

        # The process is still running, so get the memory and CPU usage
        memory_usage_bytes = java_process.memory_info().rss  # Resident Set Size in bytes
        memory_usage_mb = memory_usage_bytes / (1024 * 1024)  # Convert bytes to MB
        memory_usage_mb = round(memory_usage_mb, 2)  # Round to two decimal places

        # Write the resource usage to CSV
        csv_writer.writerow({'sql': sql_file, 'mem': memory_usage_mb})
        file.flush()
        # Sleep for a short interval before checking again
        time.sleep(1)

# Generate the list of SQL file arguments
sql_files = [f"{i:02}.sql" for i in range(1, 100)]  # 01.sql to 99.sql

# Path to the CSV file
csv_file_path = 'monitoring_data.csv'

# Open the CSV file for writing
with open(csv_file_path, mode='w', newline='') as file:
    csv_writer = csv.DictWriter(file, fieldnames=['sql', 'mem'])
    csv_writer.writeheader()

    # Base Java command list without the SQL file argument
    base_java_command = [
        'java',
        '--add-opens=java.base/java.lang=ALL-UNNAMED',
        '--add-opens=java.base/java.lang.invoke=ALL-UNNAMED',
        '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED',
        '--add-opens=java.base/java.io=ALL-UNNAMED',
        '--add-opens=java.base/java.net=ALL-UNNAMED',
        '--add-opens=java.base/java.nio=ALL-UNNAMED',
        '--add-opens=java.base/java.util=ALL-UNNAMED',
        '--add-opens=java.base/java.util.concurrent=ALL-UNNAMED',
        '--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED',
        '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED',
        '--add-opens=java.base/sun.nio.cs=ALL-UNNAMED',
        '--add-opens=java.base/sun.security.action=ALL-UNNAMED',
        '--add-opens=java.base/sun.util.calendar=ALL-UNNAMED',
        '--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED',
        '-jar', 'target/tpcds-parquet-1.0-jar-with-dependencies.jar',
        '-c', '/opt/mstr/hazheng/tpcds-parquet/config.properties'
    ]

    # Iterate over each SQL file and start a Java process
    for sql_file in sql_files:
        # Append the SQL file argument to the base command
        java_command = base_java_command + ['-s', f"{sql_file}"]

        # Start the subprocess using Popen
        process = subprocess.Popen(java_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Monitor the Java process and write to CSV
        monitor_and_write_to_csv(process, sql_file, csv_writer,file)

        # Wait for the Java process to complete before moving to the next one
        process.wait()