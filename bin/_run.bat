@echo off
rem 内部脚本：_run.bat <app名> <模块目录> [程序参数...]
rem 以工程根目录为工作目录启动，日志写到 <根目录>\logs\<app>.log；
rem 若存在 <根目录>\config\<app>.properties 则优先于 jar 内默认配置。
setlocal
chcp 65001 >nul
set "BASE=%~dp0.."
for %%I in ("%BASE%") do set "BASE=%%~fI"
set "APP=%~1"
set "MODULE=%~2"
shift
shift
set "JAR=%BASE%\%MODULE%\target\%MODULE%-all.jar"

if not exist "%JAR%" (
    echo [ERROR] %JAR% not found, please run bin\build.bat first.
    exit /b 1
)

if defined JAVA_HOME (
    set "JAVA=%JAVA_HOME%\bin\java.exe"
) else (
    set "JAVA=java"
)
if not defined JAVA_OPTS set "JAVA_OPTS=-Xms256m -Xmx512m -XX:+UseG1GC"

set "ARGS="
:collect
if "%~1"=="" goto run
set ARGS=%ARGS% %1
shift
goto collect

:run
cd /d "%BASE%"
title %APP%
"%JAVA%" %JAVA_OPTS% -Dfile.encoding=UTF-8 -Dslg.app=%APP% -jar "%JAR%" %ARGS%
exit /b %ERRORLEVEL%
