#pragma once

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "schema.h"

class MemoryStorage {
public:
    void write(const Schema::Row* row_ptr) { datas.emplace_back(*row_ptr); }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, void* res) {
        bool is_equal = true;
        size_t res_num = 0;
        res = malloc(128 * 2000);

        for (size_t i = 0; i < datas.size(); ++i) {
            switch (where_column) {
            case 0:
                is_equal = memcmp(column_key, &datas[i].id, column_key_len) == 0;
                break;
            case 1:
                is_equal = memcmp(column_key, datas[i].user_id, column_key_len) == 0;
                break;
            case 2:
                is_equal = memcmp(column_key, datas[i].name, column_key_len) == 0;
                break;
            case 3:
                is_equal = memcmp(column_key, &datas[i].salary, column_key_len) == 0;
                break;
            default:
                is_equal = false;
                break; // wrong
            }
            if (is_equal) {
                ++res_num;
                switch (select_column) {
                case 0:
                    memcpy(res, &datas[i].id, 8);
                    res = (char*)res + 8;
                    break;
                case 1:
                    memcpy(res, datas[i].user_id, 128);
                    res = (char*)res + 128;
                    break;
                case 2:
                    memcpy(res, datas[i].name, 128);
                    res = (char*)res + 128;
                    break;
                case 3:
                    memcpy(res, &datas[i].salary, 8);
                    res = (char*)res + 8;
                    break;
                default:
                    break; // wrong
                }
            }
        }
        return res_num;
    }

private:
    std::vector<Schema::Row> datas;
};
