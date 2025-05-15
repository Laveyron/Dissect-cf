#!/bin/bash

# Use the system's default Java installation
JAVA_BIN="java"

# Get the classpath with all dependencies
MVN_BIN="mvn"

# Use the target/classes directory and include all dependencies
CLASSPATH="target/classes:target/dependency/*:target/dissect-cf-0.9.8-SNAPSHOT.jar"

# Read the list of main classes we found previously
declare -a MAIN_CLASSES=(
  "hu.mta.sztaki.lpds.cloud.simulator.wms.SchedulerManager"
  "hu.mta.sztaki.lpds.cloud.simulator.examples.SingleVMOverloader"
  "hu.mta.sztaki.lpds.cloud.simulator.examples.MigrationModeling"
  "hu.mta.sztaki.lpds.cloud.simulator.examples.TransferDemo"
  "hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.JobDispatchingDemo"
  "hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.ConsolidationController"
  "hu.mta.sztaki.lpds.cloud.simulator.examples.backgroundworkload.CLIExample"
)

# Function to run a class and capture its output to a log file
run_class() {
  local class=$1
  local simple_name=$(echo "$class" | awk -F. '{print $NF}')
  local log_file="logs/${simple_name}.log"
  
  echo "Starting $simple_name... (Logs will be saved to $log_file)"
  mkdir -p logs
  $JAVA_BIN -cp $CLASSPATH $class > "$log_file" 2>&1 &
  echo "$simple_name started with PID $!"
}

# Print header
echo "======================================================"
echo "Running all DISSECT-CF examples in parallel using Java 17"
echo "======================================================"
echo "Log files will be saved in the logs/ directory"
echo ""

# Run each class in parallel
for class in "${MAIN_CLASSES[@]}"; do
  run_class "$class"
  # Small delay to avoid flooding the terminal with output
  sleep 0.5
done

echo ""
echo "All examples have been started. Use 'tail -f logs/*.log' to monitor the output."
echo "Press Enter to kill all running examples when you're done."

# Wait for user input
read

# Kill all java processes started by this script
echo "Terminating all running examples..."
pkill -P $$

echo "Done."
