#!/usr/bin/env sh
Xvfb :99 -screen 0 1920x1280x24 -ac +extension GLX +render -noreset &
dotnet WiserTaskScheduler.dll