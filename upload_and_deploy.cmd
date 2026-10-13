@echo off
REM Upload project to the server and deploy. Uses SSH keys (no passwords in files).
REM Run from project root:
REM   set DEPLOY_SERVER=your.server.ip
REM   upload_and_deploy.cmd
REM Optional: DEPLOY_USER (default root), DEPLOY_REMOTE_DIR (default /root/67b67tmess)

if "%DEPLOY_SERVER%"=="" (
    echo ERROR: set DEPLOY_SERVER first, e.g. set DEPLOY_SERVER=203.0.113.10
    exit /b 1
)
if "%DEPLOY_USER%"=="" set DEPLOY_USER=root
if "%DEPLOY_REMOTE_DIR%"=="" set DEPLOY_REMOTE_DIR=/root/67b67tmess

where scp >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: scp not found. Install the Windows OpenSSH client.
    exit /b 1
)

echo Uploading project to %DEPLOY_USER%@%DEPLOY_SERVER%:%DEPLOY_REMOTE_DIR% ...
ssh %DEPLOY_USER%@%DEPLOY_SERVER% "mkdir -p %DEPLOY_REMOTE_DIR%"
if %ERRORLEVEL% NEQ 0 exit /b 1

REM Excludes: .git, node_modules, local data and secrets stay off the server copy.
tar --exclude=.git --exclude=node_modules --exclude=data --exclude=.env -cf - . | ssh %DEPLOY_USER%@%DEPLOY_SERVER% "tar -xf - -C %DEPLOY_REMOTE_DIR%"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Upload failed.
    exit /b 1
)

echo Upload complete. Running deploy.sh ...
ssh %DEPLOY_USER%@%DEPLOY_SERVER% "cd %DEPLOY_REMOTE_DIR% && chmod +x deploy.sh && ./deploy.sh"
