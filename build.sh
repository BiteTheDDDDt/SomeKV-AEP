rm -rf build && mkdir build
cd build
cmake .. -DMAKE_TEST=OFF
make