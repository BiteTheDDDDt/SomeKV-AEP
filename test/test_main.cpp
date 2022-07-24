#include <assert.h>
#include <string.h>

#include <iostream>
#include <string>
#include <vector>

#include "interface.h"
#include "schema.h"

const char* AEP_DIR = "storage/aep/";
const char* DISK_DIR = "storage/disk/";

void test_revover(int row_number_stage1, int row_number_stage2) {
    std::vector<Schema::Row> rows;
    rows.resize(row_number_stage1 + row_number_stage2);

    for (int i = 0; i < row_number_stage1 + row_number_stage2; i++) {
        rows[i].id = i;

        auto user_id = std::to_string(i);
        memcpy(rows[i].user_id, user_id.data(), user_id.length());

        memcpy(&rows[i].name, "hello", 5);

        rows[i].salary = i * i;
    }

    void* ctx = engine_init(nullptr, nullptr, 0, AEP_DIR, DISK_DIR);

    for (int i = 0; i < row_number_stage1; i++) {
        engine_write(ctx, &rows[i], Schema::ROW_LENGTH);
    }

    // stage1 write/read test

    char res[2000 * 128];
    size_t read_num = engine_read(ctx, Schema::Column::Id, Schema::Column::Name, "hello", 5, res);

    if (read_num != row_number_stage1) {
        std::cout << "stage1 read_num!=row_number_stage1" << std::endl;
        assert(false);
    }

    for (int i = 0; i < row_number_stage1; i++) {
        auto real = Schema::create_from_address(res + Schema::ROW_LENGTH * i);
        if (equal(real, rows[i])) {
            std::cout << "stage1 write/read test check fail." << std::endl;
            std::cout << "expect: " + rows[i].to_string() << std::endl;
            std::cout << "expect: " + rows[i].to_string() << std::endl;
            assert(false);
        }
    }

    // stage2 recovery test

    engine_deinit(ctx);
    ctx = engine_init(nullptr, nullptr, 0, AEP_DIR, DISK_DIR);

    read_num = engine_read(ctx, Schema::Column::Id, Schema::Column::Name, "hello", 5, res);

    if (read_num != row_number_stage1) {
        std::cout << "stage2 read_num!=row_number_stage1" << std::endl;
        assert(false);
    }

    for (int i = 0; i < row_number_stage1; i++) {
        auto real = Schema::create_from_address(res + Schema::ROW_LENGTH * i);
        if (equal(real, rows[i])) {
            std::cout << "stage2 write/read test check fail." << std::endl;
            std::cout << "expect: " + rows[i].to_string() << std::endl;
            std::cout << "expect: " + rows[i].to_string() << std::endl;
            assert(false);
        }
    }

    for (int i = 0; i < row_number_stage2; i++) {
        engine_write(ctx, &rows[row_number_stage1 + i], Schema::ROW_LENGTH);
    }

    engine_deinit(ctx);
}
int main() {
    test_revover(10, 10);

    std::cout << "pass all check" << std::endl;
    return 0;
}
