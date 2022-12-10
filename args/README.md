# Use Case
- As a user, I want to modify the config parameters in a config file in a Docker image and use it.
- As a user, I want to be able to pick and choose the config file inside the Docker image to run with the app.

# Demo Objectives
- Copy config template from image
- Modify the original template
  - Override the file path to the config file
- Pass in parameters directly
  - Through environment variables
  - As actual arguments to the CMD

# Building Blocks
- Setup a simple Python server
- "get_info" handler returns parameterized info value
- Use configs for server settings

# Execution
## Pre-requisite: Build the Docker Image
```bash
-- build the Docker image and 
-- give it a tag name 'args:1.0b'
docker build --tag args:1.0b .
```

## Overriding CMD
```bash
-- note that '/app/template/config.json' is being read
-- instead of '/app/env/default-config.json'
-- also note that '/app/template/config.json' is the 
-- docker container path and not the host path
--
docker run args:1.0b --config /app/template/config.json
```

## Overriding Config from Host Volume
```bash

docker run args:1.0b --config /app/template/config.json
```

You can copy the config file from a running container and modify on local host.
```bash
my_container_id=$(docker create args:1.0b)
docker cp $my_container_id:/app/env/default-config.json ./config/copied.config.json
docker rm -v $my_container_id

-- edit the './config/copied.config.json' on your host

-- check it out from the my_container_id
--
docker run -it -v `pwd`/config/:/app/local/ --entrypoint /bin/sh args:1.0b

/ # ls /app/local/
config.json           copied.config.json    local.config.json     template.config.json
```

```bash
docker run -it -v `pwd`/config/:/app/local/ args:1.0b --config /app/local/copied.config.json
```
