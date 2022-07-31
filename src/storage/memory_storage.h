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

constexpr int WRITE_LOG_TIMES = (1 << 20) - 1;

class MemoryStorage {
public:
    void write(const void* row_ptr) {
        Schema::Row row = create_from_address(row_ptr);
        id_index[row.id] = _datas.size();
        user_id_index[create_from_string128(row.user_id)] = _datas.size();
        salary_index[row.salary].emplace_back(_datas.size());

        _datas.emplace_back(row);
        if ((_datas.size() & WRITE_LOG_TIMES) == WRITE_LOG_TIMES) {
            LOG(INFO) << "Write: " << _datas.size();
        }
    }

    std::vector<size_t> get_selector(int32_t where_column, const void* column_key,
                                     size_t column_key_len) {
        if (where_column == Schema::Column::Id) {
            //LOG(INFO) << "Read: Predicate(column_key=" << create_from_int64(column_key)
            //          << ", column_key_len=" << column_key_len << ")";
            const int64_t key_value = *static_cast<const int64_t*>(column_key);
            if (id_index.count(key_value)) {
                return {id_index[key_value]};
            }
        }

        if (where_column == Schema::Column::Salary) {
            //LOG(INFO) << "Read: Predicate(column_key=" << create_from_int64(column_key)
            //          << ", column_key_len=" << column_key_len << ")";
            const int64_t key_value = *static_cast<const int64_t*>(column_key);
            if (salary_index.count(key_value)) {
                return salary_index[key_value];
            }
        }

        if (where_column == Schema::Column::Userid) {
            if (column_key_len != Schema::USERID_LENGTH) {
                LOG(FATAL) << "Read: Invalid Predicate(column_key="
                           << create_from_string128(column_key)
                           << ", column_key_len=" << column_key_len << ")";
            }
            auto key_value = create_from_string128(column_key);
            //LOG(INFO) << "Read: USERID Predicate(column_key=" << key_value
            //          << ", column_key_len=" << column_key_len << ")";
            if (user_id_index.count(key_value)) {
                return {user_id_index[key_value]};
            }
        }

        if (where_column == Schema::Column::Name) {
            LOG(FATAL) << "Read: Predicate(column_key=" << create_from_string128(column_key)
                       << ", column_key_len=" << column_key_len << ")";
        }

        return {};
    }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, char* res) {
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
                res += Schema::ID_LENGTH;
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
                res += Schema::SALARY_LENGTH;
            }
        }

        if (select_column == Schema::Column::Userid) {
            std::vector<std::string> data;
            for (auto i : selector) {
                data.emplace_back(create_from_string128(_datas[i].user_id));
            }
            std::sort(data.begin(), data.end());
            for (auto i : data) {
                memcpy(res, i.data(), Schema::USERID_LENGTH);
                res += Schema::USERID_LENGTH;
            }
        }

        if (select_column == Schema::Column::Name) {
            std::vector<std::string> data;
            for (auto i : selector) {
                data.emplace_back(create_from_string128(_datas[i].name));
            }
            std::sort(data.begin(), data.end());
            for (auto i : data) {
                memcpy(res, i.data(), Schema::NAME_LENGTH);
                res += Schema::NAME_LENGTH;
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
        if (select_number > 1) {
            LOG(INFO) << "Read: res_num=" << select_number << " " << vector_to_string(selector);
        }

        return select_number;
    }

private:
    std::unordered_map<int64_t, size_t> id_index;
    std::unordered_map<std::string, size_t> user_id_index;
    std::unordered_map<int64_t, std::vector<size_t>> salary_index;
    std::vector<Schema::Row> _datas;
};
