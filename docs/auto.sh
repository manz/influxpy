#!/usr/bin/env bash
sphinx-apidoc -f -o . ../influxpy
make html
open _build/html/index.html
