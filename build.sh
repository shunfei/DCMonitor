#!/bin/sh
abort()
{
    echo >&2 '
***************
Compile failed!
***************
'
    echo "An error occurred. Exiting..." >&2
    exit 1
}
trap 'abort' 0
set -e

rm -rf target
mvn package
if [[ ! -d "target" ]]; then
  echo "target folder not found!"
  exit -1
fi
cd target

BUILD_JAR=dcmonitor-*.one-jar.jar
jar -xf ${BUILD_JAR}
rm -rf META-INF OneJar.class doc src com .version ${BUILD_JAR} classes maven-archiver surefire classes generated-sources
mv main/dcmonitor-*.jar ./
rm -rf dependencies
mv -f lib dependencies
rm -rf main

trap : 0
echo >&2 '
************
Compile done!
************
'