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

#include <string>
#include <type_traits>
#include <vector>

#include "autil/MultiValueFormatter.h"
#include "autil/MultiValueType.h"
#include "autil/StringView.h"
#include "matchdoc/VectorStorage.h"
#include "table/ListDataHolder.h"

namespace table {

template <typename T>
class ListColumnData final {
public:
    using ElemType = typename T::type;
    static constexpr bool IsMultiString = std::is_same<T, autil::MultiString>::value;
    using DataHolderType =
        typename std::conditional<IsMultiString, ListDataHolder<char>, ListDataHolder<ElemType>>::type;

public:
    ListColumnData() = default;
    explicit ListColumnData(const std::shared_ptr<autil::mem_pool::Pool> &pool)
        : ListColumnData(std::make_shared<matchdoc::VectorStorage>(pool, sizeof(T))) {}
    ListColumnData(std::shared_ptr<matchdoc::VectorStorage> data) : ListColumnData(std::move(data), 0) {}
    ListColumnData(std::shared_ptr<matchdoc::VectorStorage> data, int64_t offset)
        : _data(std::move(data)), _offset(offset) {
        if (_data) {
            _dataHolder = std::make_shared<DataHolderType>(_data->getPoolPtr());
        }
    }
    ~ListColumnData() = default;

public:
    uint32_t capacity() const { return _data->getSize(); }
    uint32_t usedBytes(uint32_t rowCount) const { return sizeof(T) * rowCount + _dataHolder->totalBytes(); }
    uint32_t allocatedBytes() const { return sizeof(T) * capacity() + _dataHolder->totalBytes(); }

    void resize(uint32_t capacity) {
        uint32_t sizeBefore = _data->getSize();
        _data->growToSize(capacity);
        uint32_t sizeAfter = _data->getSize();
        for (uint32_t i = sizeBefore; i < sizeAfter; ++i) {
            char *address = _data->get(i) + _offset;
            new ((void *)address) T();
        }
    }

    bool merge(ListColumnData<T> &other) {
        if (_offset != other._offset) {
            return false;
        }
        _data->append(*other._data);
        _dataHolder->merge(*other._dataHolder);
        return true;
    }

    autil::mem_pool::Pool *getPool() { return _data->getPool(); }

    const std::shared_ptr<matchdoc::VectorStorage> &getData() const { return _data; }

    int64_t getOffset() const { return _offset; }

    T get(size_t index) const { return *reinterpret_cast<T *>(_data->get(index) + _offset); }

    void setNoCopy(size_t index, const T &value) {
        T *addr = reinterpret_cast<T *>(_data->get(index) + _offset);
        *addr = value;
    }

    void set(size_t index, const T &value) {
        T *addr = reinterpret_cast<T *>(_data->get(index) + _offset);
        const char *data = value.getData();
        uint32_t dataSize = value.getDataSize();
        if (dataSize == 0) {
            *addr = value;
        } else {
            char *buf = (char *)_dataHolder->reserve(dataSize);
            memcpy(buf, data, dataSize);
            *addr = T(buf, value.getEncodedCountValue());
        }
    }

    template <typename U>
    typename std::enable_if<!IsMultiString && std::is_same<U, ElemType>::value, void>::type
    set(size_t index, const U *data, size_t count) {
        T *addr = reinterpret_cast<T *>(_data->get(index) + _offset);
        *addr = T(_dataHolder->append(data, count), count);
    }

    template <typename U>
    typename std::enable_if<IsMultiString && (std::is_same_v<U, std::string> || std::is_same_v<U, autil::StringView>),
                            void>::type
    set(size_t index, const U *data, size_t count) {
        size_t offsetLen = 0;
        size_t bufLen = autil::MultiValueFormatter::calculateMultiStringBufferLen(data, count, offsetLen);
        char *buf = _dataHolder->reserve(bufLen);
        autil::MultiValueFormatter::formatMultiStringToBuffer(buf, bufLen, offsetLen, data, count);
        *reinterpret_cast<T *>(_data->get(index) + _offset) = T(buf);
    }

private:
    std::shared_ptr<matchdoc::VectorStorage> _data;
    int64_t _offset = 0;
    std::shared_ptr<DataHolderType> _dataHolder;
};

} // namespace table
