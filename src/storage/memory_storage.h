#pragma once

#include <glog/logging.h>

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "utils/schema.h"

class MemoryStorage {
public:
    void write(const Schema::Row* row_ptr) { _datas.emplace_back(*row_ptr); }

    std::vector<uint8_t> update_selector(int32_t where_column, const void* column_key,
                                         size_t column_key_len) {
        std::vector<uint8_t> selector;
        selector.resize(_datas.size());

        if (where_column == Schema::Column::Id) {
            const int64_t key_value = *static_cast<const int64_t*>(column_key);
            for (size_t i = 0; i < _datas.size(); ++i) {
                selector[i] = _datas[i].id == key_value;
            }
        }

        if (where_column == Schema::Column::Salary) {
            const int64_t key_value = *static_cast<const int64_t*>(column_key);
            for (size_t i = 0; i < _datas.size(); ++i) {
                selector[i] = _datas[i].salary == key_value;
            }
        }

        if (where_column == Schema::Column::Userid) {
            for (size_t i = 0; i < _datas.size(); ++i) {
                selector[i] = memcmp(column_key, &_datas[i].user_id, column_key_len) == 0;
            }
        }

        if (where_column == Schema::Column::Name) {
            for (size_t i = 0; i < _datas.size(); ++i) {
                selector[i] = memcmp(column_key, &_datas[i].name, column_key_len) == 0;
            }
        }

        return selector;
    }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, void* res) {
        auto selector = update_selector(where_column, column_key, column_key_len);

        size_t select_number = 0;
        for (size_t i = 0; i < selector.size(); i++) {
            select_number += selector[i];
        }

        if (select_column == Schema::Column::Id) {
            for (size_t i = 0; i < _datas.size(); ++i) {
                if (selector[i]) {
                    memcpy(res, &_datas[i].id, Schema::ID_LENGTH);
                    res = (char*)res + Schema::ID_LENGTH;
                }
            }
        }

        if (select_column == Schema::Column::Salary) {
            for (size_t i = 0; i < _datas.size(); ++i) {
                if (selector[i]) {
                    memcpy(res, &_datas[i].salary, Schema::SALARY_LENGTH);
                    res = (char*)res + Schema::SALARY_LENGTH;
                }
            }
        }

        if (select_column == Schema::Column::Userid) {
            for (size_t i = 0; i < _datas.size(); ++i) {
                if (selector[i]) {
                    memcpy(res, &_datas[i].user_id, Schema::USERID_LENGTH);
                    res = (char*)res + Schema::USERID_LENGTH;
                }
            }
        }

        if (select_column == Schema::Column::Name) {
            for (size_t i = 0; i < _datas.size(); ++i) {
                if (selector[i]) {
                    memcpy(res, &_datas[i].name, Schema::NAME_LENGTH);
                    res = (char*)res + Schema::NAME_LENGTH;
                }
            }
        }

        // LOG(INFO) << "Read: res_num=" << select_number;

        return select_number;
    }

private:
    std::vector<Schema::Row> _datas;
};
