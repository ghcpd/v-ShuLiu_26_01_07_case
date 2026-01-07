@echo off
python run_tests %*
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%