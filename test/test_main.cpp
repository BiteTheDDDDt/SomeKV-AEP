#include <glog/logging.h>
#include <string.h>

#include <iostream>
#include <string>
#include <vector>

#include "interface.h"
#include "utils/common.h"
#include "utils/schema.h"

const char* AEP_DIR = "storage/aep/";
const char* DISK_DIR = "storage/disk/";

void test_recover(size_t row_number_stage1, size_t row_number_stage2) {
    std::vector<Schema::Row> rows;
    rows.resize(row_number_stage1 + row_number_stage2);
    int64_t salary = 6;

    for (int i = 0; i < row_number_stage1 + row_number_stage2; i++) {
        rows[i].id = i;

        auto name = std::to_string(i);
        memcpy(rows[i].name, name.data(), name.length());

        memcpy(&rows[i].user_id, "hello", 5);

        rows[i].salary = salary;
    }
    char** a = new char*[10];
    a[0] = new char[150];
    memcpy(a[0], "127.0.0.1:15001", sizeof("127.0.0.1:15001"));

    void* ctx = engine_init("127.0.0.1:15000", a, 1, AEP_DIR, DISK_DIR);

    LOG(INFO) << " succc init" << std::endl;

    for (int i = 0; i < row_number_stage1; i++) {
        engine_write(ctx, &rows[i], Schema::ROW_LENGTH);
    }

    // stage1 write/read test

    char res[2000 * 128];
    size_t read_num = engine_read(ctx, Schema::Column::Id, Schema::Column::Salary, &salary, 8, res);

    if (read_num != row_number_stage1) {
        LOG(FATAL) << " read_num=" << read_num << ", row_number_stage1=" << row_number_stage1;
    }
    for (int i = 0; i < row_number_stage1; i++) {
        auto real = create_from_int64(res + Schema::ID_LENGTH * i);
        if (real != rows[i].id) {
            LOG(FATAL) << "real: " << real << " "
                       << "expect: " << rows[i].id;
        }
    }

    // stage2 recovery test

    engine_deinit(ctx);
    ctx = engine_init(nullptr, nullptr, 0, AEP_DIR, DISK_DIR);

    read_num = engine_read(ctx, Schema::Column::Id, Schema::Column::Salary, &salary, 8, res);

    if (read_num != row_number_stage1) {
        LOG(FATAL) << " read_num=" << read_num << ", row_number_stage1=" << row_number_stage1;
    }
    for (int i = 0; i < row_number_stage1; i++) {
        auto real = create_from_int64(res + Schema::ID_LENGTH * i);
        if (real != rows[i].id) {
            LOG(FATAL) << "real: " << real << " "
                       << "expect: " << rows[i].id;
        }
    }

    read_num = engine_read(ctx, Schema::Column::Name, Schema::Column::Salary, &salary, 8, res);

    if (read_num != row_number_stage1) {
        LOG(FATAL) << " read_num=" << read_num << ", row_number_stage1=" << row_number_stage1;
    }
    for (int i = 0; i < row_number_stage1; i++) {
        auto real = create_from_string128(res + Schema::NAME_LENGTH * i);
        if (real != create_from_string128(rows[i].name)) {
            LOG(FATAL) << "real: " << real << " "
                       << "expect: " << rows[i].name;
        }
    }

    read_num = engine_read(ctx, Schema::Column::Salary, Schema::Column::Salary, &salary, 8, res);

    if (read_num != row_number_stage1) {
        LOG(FATAL) << " read_num=" << read_num << ", row_number_stage1=" << row_number_stage1;
    }
    for (int i = 0; i < row_number_stage1; i++) {
        auto real = create_from_int64(res + Schema::SALARY_LENGTH * i);
        if (real != rows[i].salary) {
            LOG(FATAL) << "real: " << real << " "
                       << "expect: " << rows[i].salary;
        }
    }

    read_num = engine_read(ctx, Schema::Column::Userid, Schema::Column::Salary, &salary, 8, res);

    if (read_num != row_number_stage1) {
        LOG(FATAL) << " read_num=" << read_num << ", row_number_stage1=" << row_number_stage1;
    }
    for (int i = 0; i < row_number_stage1; i++) {
        auto real = create_from_string128(res + Schema::USERID_LENGTH * i);
        if (real != create_from_string128(rows[i].user_id)) {
            LOG(FATAL) << "real: " << real << " "
                       << "expect: " << rows[i].user_id;
        }
    }

    for (int i = 0; i < row_number_stage2; i++) {
        engine_write(ctx, &rows[row_number_stage1 + i], Schema::ROW_LENGTH);
    }

    engine_deinit(ctx);
    ctx = engine_init(nullptr, nullptr, 0, AEP_DIR, DISK_DIR);

    read_num = engine_read(ctx, Schema::Column::Userid, Schema::Column::Salary, &salary, 8, res);

    if (read_num != row_number_stage1 + row_number_stage2) {
        LOG(FATAL) << " read_num=" << read_num << ", row_number_stage1=" << row_number_stage1;
    }
    for (int i = 0; i < row_number_stage1 + row_number_stage2; i++) {
        auto real = create_from_string128(res + Schema::USERID_LENGTH * i);
        if (real != create_from_string128(rows[i].user_id)) {
            LOG(FATAL) << "real: " << real << " "
                       << "expect: " << rows[i].user_id;
        }
    }

    engine_deinit(ctx);
}
int main() {
    LOG(INFO) << "start";
    test_recover(10, 10);

    LOG(INFO) << "pass all check";
    return 0;
}
