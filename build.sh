export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:build

# build interface and test executable
rm -rf build && mkdir build
cd build
cmake ..
make