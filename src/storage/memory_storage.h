#pragma once

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <unordered_map>
#include <vector>

#include "utils/common.h"
#include "utils/schema.h"

class MemoryStorage {
public:
    void write(const Schema::Row* row_ptr) {
        id_index[row_ptr->id] = _datas.size();
        user_id_index[std::string(row_ptr->user_id)] = _datas.size();

        _datas.emplace_back(*row_ptr);
    }

    std::vector<size_t> get_selector(int32_t where_column, const void* column_key,
                                     size_t column_key_len) {
        std::vector<size_t> selector;

        if (where_column == Schema::Column::Id) {
            //LOG(INFO) << "Read: Predicate(column_key=" << create_from_int64(column_key)
            //          << ", column_key_len=" << column_key_len << ")";
            const int64_t key_value = *static_cast<const int64_t*>(column_key);
            selector.push_back(id_index[key_value]);
        }

        if (where_column == Schema::Column::Salary) {
            //LOG(INFO) << "Read: Predicate(column_key=" << create_from_int64(column_key)
            //          << ", column_key_len=" << column_key_len << ")";
            const int64_t key_value = *static_cast<const int64_t*>(column_key);
            for (size_t i = 0; i < _datas.size(); ++i) {
                if (_datas[i].salary == key_value) {
                    selector.push_back(i);
                }
            }
        }

        if (where_column == Schema::Column::Userid) {
            //LOG(INFO) << "Read: Predicate(column_key=" << create_from_string(column_key)
            //          << ", column_key_len=" << column_key_len << ")";
            auto key_value = std::string(static_cast<const char*>(column_key), column_key_len);
            selector.push_back(user_id_index[key_value]);
        }

        if (where_column == Schema::Column::Name) {
            //LOG(INFO) << "Read: Predicate(column_key=" << create_from_string(column_key)
            //          << ", column_key_len=" << column_key_len << ")";
            for (size_t i = 0; i < _datas.size(); ++i) {
                if (memcmp(column_key, &_datas[i].name, column_key_len) == 0) {
                    selector.push_back(i);
                }
            }
        }

        return selector;
    }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, void* res) {
        auto selector = get_selector(where_column, column_key, column_key_len);

        size_t select_number = selector.size();

        if (select_column == Schema::Column::Id) {
            std::vector<int64_t> data;
            for (auto i : selector) {
                data.emplace_back(_datas[i].id);
            }
            std::sort(data.begin(), data.end());
            for (auto i : data) {
                memcpy(res, &i, Schema::ID_LENGTH);
                res = (char*)res + Schema::ID_LENGTH;
            }
        }

        if (select_column == Schema::Column::Salary) {
            std::vector<int64_t> data;
            for (auto i : selector) {
                data.emplace_back(_datas[i].salary);
            }
            std::sort(data.begin(), data.end());
            for (auto i : data) {
                memcpy(res, &i, Schema::SALARY_LENGTH);
                res = (char*)res + Schema::SALARY_LENGTH;
            }
        }

        if (select_column == Schema::Column::Userid) {
            std::vector<std::string> data;
            for (auto i : selector) {
                data.emplace_back(_datas[i].user_id);
            }
            std::sort(data.begin(), data.end());
            for (auto i : data) {
                memset(res, 0, Schema::USERID_LENGTH);
                memcpy(res, i.data(), i.length());
                res = (char*)res + Schema::USERID_LENGTH;
            }
        }

        if (select_column == Schema::Column::Name) {
            std::vector<std::string> data;
            for (auto i : selector) {
                data.emplace_back(_datas[i].name);
            }
            std::sort(data.begin(), data.end());
            for (auto i : data) {
                memset(res, 0, Schema::NAME_LENGTH);
                memcpy(res, i.data(), i.length());
                res = (char*)res + Schema::NAME_LENGTH;
            }
        }

        /*
        if (select_column == Schema::Column::Id) {
            for (auto i : selector) {
                memcpy(res, &_datas[i].id, Schema::ID_LENGTH);
                res = (char*)res + Schema::ID_LENGTH;
            }
        }

        if (select_column == Schema::Column::Salary) {
            for (auto i : selector) {
                memcpy(res, &_datas[i].salary, Schema::SALARY_LENGTH);
                res = (char*)res + Schema::SALARY_LENGTH;
            }
        }

        if (select_column == Schema::Column::Userid) {
            for (auto i : selector) {
                memcpy(res, &_datas[i].user_id, Schema::USERID_LENGTH);
                res = (char*)res + Schema::USERID_LENGTH;
            }
        }

        if (select_column == Schema::Column::Name) {
            for (auto i : selector) {
                memcpy(res, &_datas[i].name, Schema::NAME_LENGTH);
                res = (char*)res + Schema::NAME_LENGTH;
            }
        }
*/
        LOG(INFO) << "Read: res_num=" << select_number << " " << vector_to_string(selector);

        return select_number;
    }

private:
    std::unordered_map<int64_t, size_t> id_index;
    std::unordered_map<std::string, size_t> user_id_index;
    std::vector<Schema::Row> _datas;
};
