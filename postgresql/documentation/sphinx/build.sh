#!/bin/sh
mkdir -p ../html/doctrees
cp ../*.txt ./
cp index.rst index.txt
sphinx-build -E -b html -d ../html/doctrees . ../html
cd ../html && pwd
