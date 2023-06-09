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
#ifndef __INDEXLIB_PREFIX_KEY_TABLE_BASE_H
#define __INDEXLIB_PREFIX_KEY_TABLE_BASE_H

#include <memory>

#include "future_lite/CoroInterface.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/prefix_key_table_iterator_base.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {
template <typename ValueType>
class PrefixKeyTableBase
{
public:
    typedef PrefixKeyTableIteratorBase<ValueType> Iterator;
    DEFINE_SHARED_PTR(Iterator);

public:
    PrefixKeyTableBase(PKeyTableType type) : mOpenType(PKOT_UNKNOWN), mTableType(type) {}

    virtual ~PrefixKeyTableBase() {}

public:
    virtual bool Open(const file_system::DirectoryPtr& dir, PKeyTableOpenType openType) = 0;
    virtual void Close() = 0;

    virtual size_t Size() const = 0;
    virtual ValueType* Find(const uint64_t& key) const = 0;
    virtual bool Insert(const uint64_t& key, const ValueType& value) = 0;
    virtual size_t GetTotalMemoryUse() const = 0;
    virtual IteratorPtr CreateIterator() = 0;
    virtual bool IsFull() const { return false; }
    virtual ValueType* FindForRW(const uint64_t& key) const = 0;
    virtual ValueType* FindForRead(const uint64_t& key) const = 0;
    virtual FL_LAZY(bool) FindForRead(const uint64_t& key, ValueType& value, KVMetricsCollector* collector) const = 0;
    PKeyTableOpenType GetOpenType() const { return mOpenType; }
    PKeyTableType GetTableType() const { return mTableType; }

protected:
    PKeyTableOpenType mOpenType;
    PKeyTableType mTableType;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

#endif //__INDEXLIB_PREFIX_KEY_TABLE_BASE_H
