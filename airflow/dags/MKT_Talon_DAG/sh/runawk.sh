#!/bin/bash
for i in 1741.csv;do
awk -F'[\{\}]' -v OFS='' 'gsub("\"\"", "\"") gsub("\", \"", "\"| \"") gsub(", \"", "| \"") gsub("\"\{","\{") gsub("\}\"","\}") { for (i=2; i<=NF; i+=2) gsub(",", ".", $i) } 1' $i > $i.out
done