@echo off
REM Quick build script for development (Windows)

echo Building Messenger67 Mobile App...
echo.

REM Check if we're in mobile directory
if not exist "package.json" (
    echo Error: Run this script from mobile\ directory
    exit /b 1
)

REM Check if google-services.json exists
if not exist "google-services.json" (
    echo Warning: google-services.json not found!
    echo    Push notifications will not work without Firebase configuration.
    echo    See FIREBASE_SETUP.md for instructions.
    echo.
    set /p continue="Continue anyway? (y/n) "
    if /i not "%continue%"=="y" exit /b 1
)

REM Install dependencies
echo Installing dependencies...
call npm install

REM Prebuild
echo Generating native projects...
call npx expo prebuild --clean

REM Build and run
echo Building and installing on device...
call npx expo run:android

echo.
echo Build complete!
echo    App should be running on your device.
echo.
echo Next steps:
echo    1. Test login/register
echo    2. Test E2E encryption (DM chats)
echo    3. Test push notifications (close app, send message)
echo    4. Test voice/video calls
echo.
