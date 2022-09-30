rm -rf build && mkdir build
cd build
cmake .. -DMAKE_TEST=ON
make


cp libinterface.so ./test/
cd test
mkdir storage
mkdir storage/aep
mkdir storage/disk
./SomeKV-AEP