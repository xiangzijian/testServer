@echo off
rem 构建全部模块：bin\build.bat            （含单元测试与端到端测试）
rem               bin\build.bat -DskipTests （跳过测试）
setlocal
chcp 65001 >nul
cd /d "%~dp0.."
call mvn -B clean package %*
exit /b %ERRORLEVEL%
