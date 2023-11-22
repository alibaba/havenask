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

#include "autil/StringUtil.h"
#include "matchdoc/ToString.h"
#include "table/BaseColumnData.h"
#include "table/ColumnDataTraits.h"

namespace table {

template <typename T>
class ColumnData final : public BaseColumnData {
private:
    static constexpr bool IsMultiType = autil::IsMultiType<T>::value;
    static constexpr bool SUPPORT_TO_STRING = matchdoc::__detail::ToStringImpl<T>::value;
    using Self = ColumnData<T>;
    using Impl = typename ColumnDataTraits<T>::ColumnDataType;

public:
    ColumnData(const std::shared_ptr<autil::mem_pool::Pool> &pool) : _impl(pool) {}
    ColumnData(std::shared_ptr<matchdoc::VectorStorage> data, int64_t offset) : _impl(std::move(data), offset) {}
    ~ColumnData() = default;

public:
    static std::unique_ptr<ColumnData<T>> fromReference(matchdoc::Reference<T> *ref) {
        return std::make_unique<ColumnData<T>>(ref->mutableStorage()->shared_from_this(), ref->getOffset());
    }

public:
    inline T get(size_t rowIndex) const { return get(getRow(rowIndex)); }
    inline T get(const Row &row) const { return _impl.get(row.getIndex()); }

    inline void set(size_t rowIndex, const T &value) { set(getRow(rowIndex), value); }
    inline void set(const Row &row, const T &value) { _impl.set(row.getIndex(), value); }

    template <typename U>
    void set(size_t rowIndex, const U *data, size_t count) {
        set(getRow(rowIndex), data, count);
    }
    template <typename U>
    void set(const Row &row, const U *data, size_t count) {
        if constexpr (IsMultiType) {
            _impl.set(row.getIndex(), data, count);
        }
    }

    void setNoCopy(size_t rowIndex, const T &value) { setNoCopy(getRow(rowIndex), value); }
    void setNoCopy(const Row &row, const T &value) {
        if constexpr (IsMultiType) {
            _impl.setNoCopy(row.getIndex(), value);
        } else {
            _impl.set(row.getIndex(), value);
        }
    }

    autil::mem_pool::Pool *getPool() { return _impl.getPool(); }

public:
    void resize(uint32_t size) override { _impl.resize(size); }

    bool merge(BaseColumnData &other) override {
        Self &otherSelf = static_cast<Self &>(other);
        return _impl.merge(otherSelf._impl);
    }

    bool copy(size_t startIndex, const BaseColumnData &other, size_t srcStartIndex, size_t count) override {
        const Self &src = static_cast<const Self &>(other);
        for (size_t i = 0; i < count; ++i) {
            set(startIndex + i, src.get(srcStartIndex + i));
        }
        return true;
    }

    std::unique_ptr<BaseColumnData> share(const std::vector<Row> *rows) override {
        auto col = std::make_unique<ColumnData>(*this);
        col->attachRows(rows);
        return col;
    }

    std::string toOriginString(size_t rowIndex) const override {
        if constexpr (SUPPORT_TO_STRING) {
            auto value = get(rowIndex);
            return matchdoc::__detail::ToStringImpl<T>::toString(value);
        } else {
            return "";
        }
    }
    std::string toString(size_t rowIndex) const override {
        if constexpr (SUPPORT_TO_STRING) {
            auto value = get(rowIndex);
            auto str = matchdoc::__detail::ToStringImpl<T>::toString(value);
            if constexpr (IsMultiType) {
                autil::StringUtil::replaceAll(str, std::string(1, autil::MULTI_VALUE_DELIMITER), "^]");
            }
            return str;
        } else {
            return "";
        }
    }

    std::unique_ptr<matchdoc::ReferenceBase> toReference() const override {
        const auto &storage = _impl.getData();
        return std::make_unique<matchdoc::Reference<T>>(
            storage.get(), _impl.getOffset(), INVALID_OFFSET, sizeof(T), "", false);
    }

    void serialize(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows) const override {
        for (size_t i = 0; i < rows.size(); ++i) {
            dataBuffer.write(get(rows[i]));
        }
    }

    void deserialize(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows) override {
        for (size_t i = 0; i < rows.size(); ++i) {
            T val;
            dataBuffer.read(val, _impl.getPool());
            this->setNoCopy(rows[i], val);
        }
    }

    uint32_t capacity() const override { return _impl.capacity(); }
    uint32_t usedBytes(uint32_t rowCount) const override { return _impl.usedBytes(rowCount); }
    uint32_t allocatedBytes() const override { return _impl.allocatedBytes(); }

private:
    Impl _impl;
};

} // namespace table
