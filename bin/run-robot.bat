@echo off
rem 用法：bin\run-robot.bat [host] [port] [rounds]   默认 127.0.0.1 9001 3
call "%~dp0_run.bat" robot slg-robot %*
