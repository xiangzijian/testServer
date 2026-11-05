@echo off
rem 依次在独立窗口启动 world、game、gateway（关闭窗口或 Ctrl+C 即停止对应进程）。
rem gateway 带自动重连，启动顺序不严格，但建议先后端后网关。
setlocal
set "BIN=%~dp0"
start "world" cmd /k call "%BIN%start-world.bat"
start "game" cmd /k call "%BIN%start-game.bat"
timeout /t 2 /nobreak >nul
start "gateway" cmd /k call "%BIN%start-gateway.bat"
echo started world / game / gateway in separate windows.
echo run bin\run-robot.bat to test ping/pong.
