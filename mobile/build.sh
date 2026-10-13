#!/bin/bash
# Quick build script for development

set -e

echo "🚀 Building Messenger67 Mobile App..."
echo ""

# Check if we're in mobile directory
if [ ! -f "package.json" ]; then
    echo "❌ Error: Run this script from mobile/ directory"
    exit 1
fi

# Check if google-services.json exists
if [ ! -f "google-services.json" ]; then
    echo "⚠️  Warning: google-services.json not found!"
    echo "   Push notifications will not work without Firebase configuration."
    echo "   See FIREBASE_SETUP.md for instructions."
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Install dependencies
echo "📦 Installing dependencies..."
npm install

# Prebuild
echo "🔨 Generating native projects..."
npx expo prebuild --clean

# Build and run
echo "📱 Building and installing on device..."
npx expo run:android

echo ""
echo "✅ Build complete!"
echo "   App should be running on your device."
echo ""
echo "📋 Next steps:"
echo "   1. Test login/register"
echo "   2. Test E2E encryption (DM chats)"
echo "   3. Test push notifications (close app, send message)"
echo "   4. Test voice/video calls"
echo ""
