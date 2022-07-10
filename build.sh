# build interface
rm -rf build && mkdir build
cd build
cmake .. -DCMAKE_EXPORT_COMPILE_COMMANDS=1
make
cd ..

# build test executable
cd test
rm -rf build && mkdir build
cd build
cmake ..
make
cd ../..