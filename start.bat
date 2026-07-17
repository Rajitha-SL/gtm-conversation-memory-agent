@echo off
title GTM Context Engine Launcher
echo ===================================================
echo   GTM Context Engine - Local Launcher Node
echo ===================================================

echo [1/3] Spinning up PostgreSQL & Redis via Docker...
docker compose up -d database queue_broker
if %ERRORLEVEL% NEQ 0 (
  echo WARNING: Docker compose could not be run. Please ensure local Postgres (5432) and Redis (6379) are active.
)

echo [2/3] Installing dependencies...
cd backend
call npm install
cd ../frontend
call npm install
cd ..

echo [3/3] Launching application services...
echo Starting Backend API Server (port 3000)...
start "Backend API Server" cmd /c "cd backend && npm run start"

echo Starting Background Queue Worker...
start "Background Queue Worker" cmd /c "cd backend && npm run worker"

echo Starting Frontend Dashboard (port 3001)...
start "Frontend Dashboard" cmd /c "cd frontend && npm run dev -- -p 3001"

echo ===================================================
echo Startup sequence triggered.
echo - Frontend: http://localhost:3001
echo - Backend API: http://localhost:3000
echo ===================================================
pause
