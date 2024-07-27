#!/bin/bash

mvn clean install
cd target/apidocs/
git init
git remote add javadoc https://github.com/johndunlap/influx-jdbc.git
git fetch --depth=1 javadoc gh-pages
git add --all
git commit -m "Adding updated javadocs to gh-pages"
git merge --no-edit -s ours remotes/javadoc/gh-pages
git push javadoc master:gh-pages
rm -r -f .git

