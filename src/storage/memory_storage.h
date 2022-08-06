#pragma once

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <list>
#include <string_view>

#include "parallel_hashmap/phmap.h"
#include "utils/common.h"
#include "utils/schema.h"

constexpr int WRITE_LOG_TIMES = (1 << 20) - 1;
constexpr int MAX_DATA_SIZE = 50000000;

class MemoryStorage {
    using Container = std::vector<Schema::Row>;
    using Offset = size_t;
    using Selector = std::vector<Offset>;
    using Mutex = std::mutex;
    template <typename K, typename V>
    using ParallelMap =
            phmap::parallel_flat_hash_map<K, V, phmap::Hash<K>, phmap::EqualTo<K>,
                                          std::allocator<std::pair<const K, V>>, 4UL, std::mutex>;

public:
    MemoryStorage() { _datas.reserve(MAX_DATA_SIZE); }

    void write(const Schema::Row* row) {
        Offset offset;
        {
            std::unique_lock lock(_mtx);
            offset = _datas.size();
            _datas.emplace_back(*row);
        }

        id_index.try_emplace_l(
                row->id, [](const auto&) {}, offset);

        user_id_index.try_emplace_l(
                create_from_string128(row->user_id), [](const auto&) {}, offset);

        salary_index.try_emplace_l(
                row->salary, [offset](auto& v) { v.second.emplace_back(offset); },
                Selector {offset});

        if ((offset & WRITE_LOG_TIMES) == WRITE_LOG_TIMES) {
            LOG(INFO) << "Write: " << offset;
            sync();
            print_meminfo();
        }
    }

    size_t read(int32_t select_column, int32_t where_column, const void* column_key,
                size_t column_key_len, char* res) {
        auto selector = get_selector(where_column, column_key, column_key_len);
        size_t size = selector.size();

        if (size > 1) {
            read_multiple(select_column, res, selector);
        } else if (size == 1) {
            read_single(select_column, res, *selector.begin());
        }
        return size;
    }

private:
    void read_single(int32_t select_column, char* res, Offset offset) {
        if (select_column == Schema::Column::Id) {
            memcpy(res, &_datas[offset].id, Schema::ID_LENGTH);
            res += Schema::ID_LENGTH;
        }

        if (select_column == Schema::Column::Salary) {
            memcpy(res, &_datas[offset].salary, Schema::SALARY_LENGTH);
            res += Schema::SALARY_LENGTH;
        }

        if (select_column == Schema::Column::Userid) {
            memcpy(res, _datas[offset].user_id, Schema::USERID_LENGTH);
            res += Schema::USERID_LENGTH;
        }

        if (select_column == Schema::Column::Name) {
            memcpy(res, _datas[offset].name, Schema::NAME_LENGTH);
            res += Schema::NAME_LENGTH;
        }
    }

    void read_multiple(int32_t select_column, char* res, const Selector& selector) {
        if (select_column == Schema::Column::Id) {
            std::vector<int64_t> data;
            for (auto offset : selector) {
                data.emplace_back(_datas[offset].id);
            }
            std::sort(data.begin(), data.end());
            for (auto i : data) {
                memcpy(res, &i, Schema::ID_LENGTH);
                res += Schema::ID_LENGTH;
            }
        }

        if (select_column == Schema::Column::Salary) {
            std::vector<int64_t> data;
            for (auto offset : selector) {
                data.emplace_back(_datas[offset].salary);
            }
            std::sort(data.begin(), data.end());
            for (auto i : data) {
                memcpy(res, &i, Schema::SALARY_LENGTH);
                res += Schema::SALARY_LENGTH;
            }
        }

        if (select_column == Schema::Column::Userid) {
            std::vector<std::string_view> data;
            for (auto offset : selector) {
                data.emplace_back(create_from_string128_ref(_datas[offset].user_id));
            }
            std::sort(data.begin(), data.end());
            for (auto i : data) {
                memcpy(res, i.data(), Schema::USERID_LENGTH);
                res += Schema::USERID_LENGTH;
            }
        }

        if (select_column == Schema::Column::Name) {
            std::vector<std::string_view> data;
            for (auto offset : selector) {
                data.emplace_back(create_from_string128_ref(_datas[offset].name));
            }
            std::sort(data.begin(), data.end());
            for (auto i : data) {
                memcpy(res, i.data(), Schema::NAME_LENGTH);
                res += Schema::NAME_LENGTH;
            }
        }
    }

    Selector get_selector(int32_t where_column, const void* column_key, size_t column_key_len) {
        Selector selector;
        if (where_column == Schema::Column::Id) {
            const int64_t key_value = *static_cast<const int64_t*>(column_key);
            id_index.if_contains(key_value,
                                 [&selector](const auto& v) { selector.emplace_back(v.second); });
        }

        if (where_column == Schema::Column::Salary) {
            const int64_t key_value = *static_cast<const int64_t*>(column_key);
            salary_index.if_contains(key_value,
                                     [&selector](const auto& v) { selector = v.second; });
        }

        if (where_column == Schema::Column::Userid) {
            std::string key_value = create_from_string128(column_key);
            user_id_index.if_contains(
                    key_value, [&selector](const auto& v) { selector.emplace_back(v.second); });
        }

        return selector;
    }

    ParallelMap<int64_t, Offset> id_index;
    ParallelMap<std::string, Offset> user_id_index;
    ParallelMap<int64_t, Selector> salary_index;
    Container _datas;
    Mutex _mtx;
};
