#!/bin/bash
echo "==================================================="
echo "  GTM Context Engine - Local Launcher Node (Unix)"
echo "==================================================="

echo "[1/3] Spinning up PostgreSQL & Redis via Docker..."
docker compose up -d database queue_broker
if [ $? -ne 0 ]; then
  echo "WARNING: Docker compose could not be run. Please ensure local Postgres (5432) and Redis (6379) are active."
fi

echo "[2/3] Installing dependencies..."
cd backend && npm install
cd ../frontend && npm install
cd ..

echo "[3/3] Launching application services..."
echo "Starting Backend API Server..."
cd backend && npm run start > ../backend.log 2>&1 &
BACKEND_PID=$!

echo "Starting Background Queue Worker..."
npm run worker > ../worker.log 2>&1 &
WORKER_PID=$!

echo "Starting Frontend Dashboard..."
cd ../frontend && npm run dev -- -p 3001 > ../frontend.log 2>&1 &
FRONTEND_PID=$!

echo "==================================================="
echo "Startup sequence triggered."
echo "- Frontend: http://localhost:3001"
echo "- Backend API: http://localhost:3000"
echo "Log files are generated in the root directory."
echo "Press Ctrl+C to terminate all services."
echo "==================================================="

trap "kill $BACKEND_PID $WORKER_PID $FRONTEND_PID; exit" INT TERM
wait
