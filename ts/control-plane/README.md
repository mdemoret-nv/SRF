## Build Instructions

Process is very manually currenly. Follow these steps:

1. Update the conda environment: `mamba env update -n ${CONDA_DEFAULT_ENV} -f ./ci/conda/environments/dev_env.yml`
2. Change directory to the node folder: `cd ts/control-plane/`
3. Install the node dependencies: `npm install`
4. Build the proto files: `npm run proto:build`
5. Build the server files: `npm run build:server`
6. Run the tests: `npm run test`