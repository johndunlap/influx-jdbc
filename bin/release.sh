#!/bin/bash

NEW_VERSION=$1

# Verify that the new version is semver compliant but may optionally end with -SNAPSHOT
if [[ ! $NEW_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+(-SNAPSHOT)?$ ]]; then
    echo "Version must be semver compliant but may optionally end with -SNAPSHOT"
    exit 1
fi

# Set the new version, build and deploy the artifacts
mvn versions:set "-DnewVersion=${NEW_VERSION}"
mvn clean deploy

# Record the new version in version control
git add -A
git commit -m "Released version ${NEW_VERSION}"
git push

# Create a new tag for the release
git tag -a "${NEW_VERSION}" -m "Version ${NEW_VERSION}"
git push origin "${NEW_VERSION}"

# If the new version is not a snapshot, then update the version to the next snapshot
if [[ ! $NEW_VERSION =~ -SNAPSHOT$ ]]; then
    NEXT_VERSION=$(echo $NEW_VERSION | perl -pe 's/^((\d+\.)*)(\d+)(.*)$/$1.($3+1).$4/e')
    mvn versions:set "-DnewVersion=${NEXT_VERSION}-SNAPSHOT"
    git add -A
    git commit -m "Updated version to ${NEXT_VERSION}-SNAPSHOT"
    git push
fi
