# According to the chat, we are supposed to put our http endpoints for the log files up here
# The endpoint /checkcluster is used to get the state of the cluster from the gateway, as used here
# On each server, /summary is used to get the log file that says changes in the gossip
# While /verbose gets you the log file with every single gossip message
# The port for these log files is UDP + 1, since it was available

function run_commands {

  death_notice_time=50

  # Step 1:
  mvn test package

  # Step 2:
  for i in {0..6}; do
    java -cp target/stage5-1.0-SNAPSHOT.jar edu.yu.cs.com3800.stage5.PeerServerImpl "$i" &
    pids[i]=$!
  done
  java -cp target/stage5-1.0-SNAPSHOT.jar edu.yu.cs.com3800.stage5.GatewayServer &
  gateway_pid=$!

  # It's not being nice and doing things in order, so let's sleep a little
  # That way, we won't get passed here until the processes have at least started
  # Otherwise, the gateway won't be able to answer anything
  sleep 1

  # This is the directory where the log files are found
  # I'm doing this now, because later, there will be later logs from CatchUpServers and the like
  directory=$(ls | grep -e "logs-" | sort -n | tail -1)

  # Step 3:
  working_message="Leader Election is still in progress"
  election_done="$working_message"
  while [ "$election_done" == "$working_message" ]
  do
    election_done=$(java -cp target/stage5-1.0-SNAPSHOT.jar edu.yu.cs.com3800.Util N/A /checkcluster)
    echo "$election_done"
    sleep 0.3
  done

  # Step 4:
  for i in {1..9}; do
    rand=$RANDOM
    cat < script_files/Main.java
    echo "//$rand"
    # Note that while this isn't technically printing the code, it amounts to the same thing
    java -cp target/stage5-1.0-SNAPSHOT.jar edu.yu.cs.com3800.Util script_files/Main.java /compileandrun $rand > temp_"$i".txt &
    query_pids[i]=$!
  done
  wait "${query_pids[@]}"
  for i in {1..9}; do
    cat < "temp_${i}.txt"
  done

  # Step 5:
  echo "Killing server with ID 0"
  kill -9 "${pids[0]}"
  unset "pids[0]"
  sleep $death_notice_time

  java -cp target/stage5-1.0-SNAPSHOT.jar edu.yu.cs.com3800.Util N/A /checkcluster

  # Step 6:
  echo "Killing the leader, which should have ID 6"
  kill -9 "${pids[6]}"
  unset "pids[6]"
  sleep 1

  for i in {1..9}; do
    rand=$RANDOM
    cat < script_files/Main.java
    echo "//$rand"
    # Note that while this isn't technically printing the code, it amounts to the same thing
    java -cp target/stage5-1.0-SNAPSHOT.jar edu.yu.cs.com3800.Util script_files/Main.java /compileandrun $rand > temp_"$i".txt &
    query_pids[i]=$!
  done

  # Step 7:
  sleep $death_notice_time

  election_done="$working_message"
  while [ "$election_done" == "$working_message" ]
  do
    election_done=$(java -cp target/stage5-1.0-SNAPSHOT.jar edu.yu.cs.com3800.Util N/A /checkcluster)
    echo "$election_done"
    sleep 0.3
  done

  # Now, let's check if we can still get our answers
  wait "${query_pids[@]}"
  for i in {1..9}; do
    cat < "temp_${i}.txt"
  done

  # Step 8: Now we do the one final query
  rand=$RANDOM
  cat < script_files/Main.java
  echo "//$rand"
  # Note that while this isn't technically printing the code, it amounts to the same thing
  java -cp target/stage5-1.0-SNAPSHOT.jar edu.yu.cs.com3800.Util script_files/Main.java /compileandrun $rand

  # Step 9: Get the logs
  for i in {0..7}; do
    echo "$directory/Gossiper-$((8900 + i * 3))-Log.txt"
    echo "$directory/Gossiper-$((8900 + i * 3))-heavy-Log.txt"
  done

  # Step 10: Kill the rest of them, so we don't have danglers
  kill -9 $gateway_pid "${pids[@]}"
  # Also, get rid of temp files so they don't clog up the directory
  for i in {1..9}; do
    rm "temp_${i}.txt"
  done
}

# I use a function so I can easily print it both to the screen and to the log
run_commands | tee output.log