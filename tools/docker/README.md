# Stateful Functions Docker Image

This directory contains scripts to build images using the current source version.

The scripts here assumes that distribution and core artifacts are already built from the current source, and adds them to the image build context.
Therefore, before executing the scripts, make sure to build the project first using `mvn clean package -DskipTests`.

## Building the image

```
./build-stateful-functions.sh
```

This builds the `flink-statefun` image, tagged with the current source version.

## Code examples and E2E tests

There are several [examples](../../statefun-examples) and [end-to-end tests](../../statefun-e2e-tests) that rely
on images built with the corresponding source version.

For released versions, the required image should be available in the Docker Hub registry.
For snapshots / unstable versions, you would need to build the image locally with the scripts here.
