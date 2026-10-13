#!/bin/bash
# Build Android APK on server
# This script builds the mobile app APK

set -e

echo "🔨 Building Android APK..."
echo ""

# Configuration
PROJECT_DIR="/root/67b67tmess"
MOBILE_DIR="$PROJECT_DIR/mobile"
OUTPUT_DIR="$PROJECT_DIR/builds"

# Check if project exists
if [ ! -d "$MOBILE_DIR" ]; then
    echo "❌ Mobile project not found at $MOBILE_DIR"
    exit 1
fi

cd "$MOBILE_DIR"

echo "Step 1: Installing Node.js and npm..."
# Install Node.js if not installed
if ! command -v node &> /dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
    apt-get install -y nodejs
fi

echo "✓ Node.js version: $(node --version)"
echo "✓ npm version: $(npm --version)"
echo ""

echo "Step 2: Installing dependencies..."
npm install
echo "✓ Dependencies installed"
echo ""

echo "Step 3: Installing Android SDK..."
# Install Java
if ! command -v java &> /dev/null; then
    apt-get update
    apt-get install -y openjdk-17-jdk
fi

# Install Android SDK
if [ ! -d "$HOME/android-sdk" ]; then
    echo "Downloading Android SDK..."
    cd /tmp
    wget https://dl.google.com/android/repository/commandlinetools-linux-9477386_latest.zip
    mkdir -p $HOME/android-sdk/cmdline-tools
    unzip commandlinetools-linux-9477386_latest.zip -d $HOME/android-sdk/cmdline-tools
    mv $HOME/android-sdk/cmdline-tools/cmdline-tools $HOME/android-sdk/cmdline-tools/latest

    # Set environment variables
    export ANDROID_HOME=$HOME/android-sdk
    export PATH=$PATH:$ANDROID_HOME/cmdline-tools/latest/bin:$ANDROID_HOME/platform-tools

    # Accept licenses
    yes | sdkmanager --licenses

    # Install required packages
    sdkmanager "platform-tools" "platforms;android-33" "build-tools;33.0.0"
fi

export ANDROID_HOME=$HOME/android-sdk
export PATH=$PATH:$ANDROID_HOME/cmdline-tools/latest/bin:$ANDROID_HOME/platform-tools

echo "✓ Android SDK installed"
echo ""

echo "Step 4: Generating native projects..."
cd "$MOBILE_DIR"
npx expo prebuild --clean
echo "✓ Native projects generated"
echo ""

echo "Step 5: Building APK..."
cd android
./gradlew assembleRelease

if [ $? -eq 0 ]; then
    echo "✓ APK built successfully!"
    echo ""

    # Create output directory
    mkdir -p "$OUTPUT_DIR"

    # Copy APK
    APK_PATH="app/build/outputs/apk/release/app-release.apk"
    if [ -f "$APK_PATH" ]; then
        cp "$APK_PATH" "$OUTPUT_DIR/messenger67-$(date +%Y%m%d-%H%M%S).apk"
        cp "$APK_PATH" "$OUTPUT_DIR/messenger67-latest.apk"

        echo "📦 APK saved to:"
        echo "   $OUTPUT_DIR/messenger67-latest.apk"
        echo ""

        # Get APK size
        APK_SIZE=$(du -h "$OUTPUT_DIR/messenger67-latest.apk" | cut -f1)
        echo "📊 APK size: $APK_SIZE"
        echo ""

        echo "✅ Build complete!"
        echo ""
        echo "Download APK:"
        echo "   scp root@<SERVER_IP>:$OUTPUT_DIR/messenger67-latest.apk ."
        echo ""
    else
        echo "❌ APK not found at $APK_PATH"
        exit 1
    fi
else
    echo "❌ Build failed!"
    echo "Check logs above for errors"
    exit 1
fi
