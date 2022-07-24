export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:build
rm -rf build && mkdir build
cd build
cmake .. -DMAKE_TEST=ON
make

cd test
mkdir storage
mkdir storage/aep
mkdir storage/disk
./SomeKV-AEP