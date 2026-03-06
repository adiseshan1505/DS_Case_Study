#!/bin/bash
echo "Starting Central Server on port 8080..."
./server_bin -port 8080 &
SERVER_PID=$!
sleep 1

echo "Starting Client 1 on port 9001..."
./client_bin -server 127.0.0.1:8080 -port 9001 &
CLIENT1_PID=$!
sleep 1

echo "Starting Client 2 on port 9002..."
./client_bin -server 127.0.0.1:8080 -port 9002 &
CLIENT2_PID=$!
sleep 1

echo "Starting Client 3 on port 9003..."
./client_bin -server 127.0.0.1:8080 -port 9003 &
CLIENT3_PID=$!

echo "Press [ENTER] to kill all processes and exit..."
read
kill $SERVER_PID $CLIENT1_PID $CLIENT2_PID $CLIENT3_PID
wait $SERVER_PID
echo "Done."