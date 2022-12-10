# Use Case
- As a user, I want to modify the config parameters.

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
