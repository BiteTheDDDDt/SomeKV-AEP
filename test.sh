CURRENT_DIR=$(cd $(dirname $0); pwd)
echo "CURRENT_DIR=${CURRENT_DIR}"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${CURRENT_DIR}/build
echo "LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"

rm -rf build && mkdir build
cd build
cmake .. -DMAKE_TEST=ON
make

cd test
mkdir storage
mkdir storage/aep
mkdir storage/disk
#./SomeKV-AEP