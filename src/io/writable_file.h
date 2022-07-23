#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <string>

#include "io/common.h"
#include "schema.h"

class WritableFile {
public:
    WritableFile(std::string path) { _fd = open(path.data(), O_TRUNC | O_WRONLY | O_CREAT); }
    ~WritableFile() {
        flush();
        close(_fd);
    }

    void append(const void* data_ptr) {
        std::memcpy(_buffer + _data_count++ * Schema::ROW_LENGTH, data_ptr, Schema::ROW_LENGTH);

        if (_data_count == WRITE_FILE_BUFFER_SIZE) {
            flush();
        }
    }
    void flush() {
        write(_fd, _buffer, _data_count * Schema::ROW_LENGTH);
        _data_count = 0;
    }

private:
    int _fd;
    int _data_count = 0;
    char _buffer[WRITE_FILE_BUFFER_SIZE * Schema::ROW_LENGTH];
};