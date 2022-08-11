#!/bin/sh

# set the directory to the dir in which this script resides
newPath=`echo $0 | awk '{split($0, a, ";"); split(a[1], b, "/"); for(x = 2; x < length(b); x++){printf("/%s", b[x]);} print "";}'`
cd "$newPath"

lipo -create -output libpq.5.14.dylib ./libpq.5.14.arm64.dylib ./libpq.5.14.x86_64.dylib

sudo cp libpq.5.14.dylib /opt/homebrew/Cellar/libpq/14.4/lib/libpq.5.14.dylib