set -e
set -u
set -x
WORKSPACE_ROOT="$WORKSPACE/$BUILD_NUMBER"
echo "Setting up build in $WORKSPACE_ROOT"
TEST_OUTPUT_DIR_REL=testcase_output
TEST_OUTPUT_DIR="$WORKSPACE_ROOT/$TEST_OUTPUT_DIR_REL"
mkdir $TEST_OUTPUT_DIR
TEST_DATA_DIR="$WORKSPACE/../../../test-data"


# build and run basic unit tests
docker build --no-cache=true -f ${WORKSPACE_ROOT}/projects/xova/docker/python37.docker -t xova.1804.py37:${BUILD_NUMBER} ${WORKSPACE_ROOT}/projects/xova/
docker build --no-cache=true -f ${WORKSPACE_ROOT}/projects/xova/docker/python38.docker -t xova.2004.py38:${BUILD_NUMBER} ${WORKSPACE_ROOT}/projects/xova/

#run tests
TEST_MS_REL=1519747221.subset.ms

tar xvf $TEST_DATA_DIR/acceptance_test_data.tar.gz -C $TEST_OUTPUT_DIR

rm $TEST_OUTPUT_DIR/$TEST_MS_REL -rf
tar xvf $TEST_DATA_DIR/acceptance_test_data.tar.gz -C $TEST_OUTPUT_DIR

# test 3.8 with some real data
docker run \
    --rm \
    -v $TEST_OUTPUT_DIR:/testdata \
    --workdir /code \
    --entrypoint /bin/sh \
    xova.2004.py38:${BUILD_NUMBER} \
    -c "xova bda /testdata/$TEST_MS_REL -fov 1.0 -d 0.8 -dc DATA -mc 24 --force -o /testdata/${TEST_MS_REL}.bda"

