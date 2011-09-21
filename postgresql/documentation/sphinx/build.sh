#!/bin/sh
mkdir -p ../html/doctrees
cp ../*.txt ./
cp index.rst index.txt
cp modules.rst modules.txt
sphinx-build -E -b html -d ../html/doctrees . ../html
cd ../html && pwd
