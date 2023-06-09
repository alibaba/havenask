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

#include <memory>

#include "future_lite/CoroInterface.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/kkv/common/KKVMetricsCollector.h"
#include "indexlib/index/kkv/pkey_table/PKeyTableType.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableIteratorBase.h"

namespace indexlibv2::index {

enum class PKeyTableOpenType {
    READ,
    RW,
    WRITE,
    UNKNOWN,
};

template <typename ValueType>
class PrefixKeyTableBase
{
public:
    using Iterator = PrefixKeyTableIteratorBase<ValueType>;

public:
    PrefixKeyTableBase(PKeyTableType type) : _openType(PKeyTableOpenType::UNKNOWN), _tableType(type) {}

    virtual ~PrefixKeyTableBase() {}

public:
    virtual bool Open(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, PKeyTableOpenType openType) = 0;
    virtual void Close() = 0;

    virtual size_t Size() const = 0;
    virtual ValueType* Find(const uint64_t& key) const = 0;
    virtual bool Insert(const uint64_t& key, const ValueType& value) = 0;
    virtual size_t GetTotalMemoryUse() const = 0;
    virtual std::shared_ptr<Iterator> CreateIterator() const = 0;
    virtual bool IsFull() const { return false; }
    virtual ValueType* FindForRW(const uint64_t& key) const = 0;
    virtual ValueType* FindForRead(const uint64_t& key) const = 0;
    virtual FL_LAZY(bool) FindForRead(const uint64_t& key, ValueType& value, KKVMetricsCollector* collector) const = 0;
    PKeyTableOpenType GetOpenType() const { return _openType; }
    PKeyTableType GetTableType() const { return _tableType; }

protected:
    PKeyTableOpenType _openType;
    PKeyTableType _tableType;
};

} // namespace indexlibv2::index
