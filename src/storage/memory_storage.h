#pragma once

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "parallel_hashmap/phmap.h"
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

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, char* res) {
        auto selector = get_selector(where_column, column_key, column_key_len);

        if (selector.size() == 1) {
            read_single(select_column, res, selector[0]);
        } else {
            read_multiple(select_column, res, selector);
        }
        return selector.size();
    }

private:
    void read_single(int32_t select_column, char* res, size_t selector) {
        if (select_column == Schema::Column::Id) {
            memcpy(res, &_datas[selector].id, Schema::ID_LENGTH);
            res += Schema::ID_LENGTH;
        }

        if (select_column == Schema::Column::Salary) {
            memcpy(res, &_datas[selector].salary, Schema::SALARY_LENGTH);
            res += Schema::SALARY_LENGTH;
        }

        if (select_column == Schema::Column::Userid) {
            memcpy(res, _datas[selector].user_id, Schema::USERID_LENGTH);
            res += Schema::USERID_LENGTH;
        }

        if (select_column == Schema::Column::Name) {
            memcpy(res, _datas[selector].name, Schema::NAME_LENGTH);
            res += Schema::NAME_LENGTH;
        }
    }
    void read_multiple(int32_t select_column, char* res, const std::vector<size_t>& selector) {
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
    }

    std::vector<size_t> get_selector(int32_t where_column, const void* column_key,
                                     size_t column_key_len) {
        if (where_column == Schema::Column::Id) {
            const int64_t key_value = *static_cast<const int64_t*>(column_key);
            if (id_index.count(key_value)) {
                return {id_index[key_value]};
            }
        }

        if (where_column == Schema::Column::Salary) {
            const int64_t key_value = *static_cast<const int64_t*>(column_key);
            if (salary_index.count(key_value)) {
                return salary_index[key_value];
            }
        }

        if (where_column == Schema::Column::Userid) {
            auto key_value = create_from_string128(column_key);
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

    phmap::parallel_flat_hash_map<int64_t, size_t> id_index;
    phmap::parallel_flat_hash_map<std::string, size_t> user_id_index;
    phmap::parallel_flat_hash_map<int64_t, std::vector<size_t>> salary_index;
    std::vector<Schema::Row> _datas;
};
