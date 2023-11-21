/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "matchdoc/VectorStorage.h"

namespace table {

template <typename T>
class SimpleColumnData final {
public:
    SimpleColumnData() = default;
    explicit SimpleColumnData(const std::shared_ptr<autil::mem_pool::Pool> &pool)
        : SimpleColumnData(std::make_shared<matchdoc::VectorStorage>(pool, sizeof(T))) {}
    SimpleColumnData(std::shared_ptr<matchdoc::VectorStorage> data) : SimpleColumnData(std::move(data), 0) {}
    SimpleColumnData(std::shared_ptr<matchdoc::VectorStorage> data, int64_t offset)
        : _data(std::move(data)), _offset(offset) {}
    ~SimpleColumnData() = default;

public:
    uint32_t capacity() const { return _data->getSize(); }
    uint32_t usedBytes(uint32_t rowCount) const { return sizeof(T) * rowCount; }
    uint32_t allocatedBytes() const { return sizeof(T) * capacity(); }

    void resize(uint32_t capacity) { _data->growToSize(capacity); }

    bool merge(SimpleColumnData<T> &other) {
        if (_offset != other._offset) {
            return false;
        }
        _data->append(*other._data);
        return true;
    }

    autil::mem_pool::Pool *getPool() { return _data->getPool(); }

    const std::shared_ptr<matchdoc::VectorStorage> &getData() const { return _data; }

    int64_t getOffset() const { return _offset; }

    T get(size_t index) const { return *reinterpret_cast<T *>(_data->get(index) + _offset); }

    void set(size_t index, const T &value) { *reinterpret_cast<T *>(_data->get(index) + _offset) = value; }

private:
    std::shared_ptr<matchdoc::VectorStorage> _data;
    int64_t _offset = 0;
};

} // namespace table
