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

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/common/OnDiskPKeyOffset.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/pkey_table/ClosedHashPrefixKeyTableTraits.h"

namespace indexlibv2::index {

template <PKeyTableType Type>
class OnDiskClosedHashIterator
{
public:
    OnDiskClosedHashIterator() = default;
    ~OnDiskClosedHashIterator() = default;

public:
    void Open(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& config,
              const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory);
    bool IsValid() const;
    void MoveToNext();
    void SortByKey();
    size_t Size() const;
    uint64_t Key() const;
    OnDiskPKeyOffset Value() const;
    void Reset() { _iterator.Reset(); }

private:
    using Traits = typename ClosedHashPrefixKeyTableTraits<uint64_t, Type>::Traits;
    using ClosedTableIterator = typename Traits::template FileIterator<false>;

private:
    ClosedTableIterator _iterator;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
template <PKeyTableType Type>
alog::Logger*
    OnDiskClosedHashIterator<Type>::_logger = alog::Logger::getLogger("indexlib.index.OnDiskClosedHashIterator");

template <PKeyTableType Type>
void OnDiskClosedHashIterator<Type>::Open(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& config,
                                          const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto readOption = indexlib::file_system::ReaderOption::CacheFirst(indexlib::file_system::FSOT_BUFFERED);
    auto fileReader = indexDirectory->CreateFileReader(PREFIX_KEY_FILE_NAME, readOption).GetOrThrow();
    if (!_iterator.Init(fileReader)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Init closed hash file iterator for file [%s] failed!",
                             fileReader->DebugString().c_str());
    }
    fileReader->Close().GetOrThrow();
}

template <PKeyTableType Type>
bool OnDiskClosedHashIterator<Type>::IsValid() const
{
    return _iterator.IsValid();
}

template <PKeyTableType Type>
void OnDiskClosedHashIterator<Type>::MoveToNext()
{
    _iterator.MoveToNext();
}

template <PKeyTableType Type>
void OnDiskClosedHashIterator<Type>::SortByKey()
{
    AUTIL_LOG(INFO, "Begin SortByKey");
    _iterator.SortByKey();
    AUTIL_LOG(INFO, "End SortByKey");
}

template <PKeyTableType Type>
size_t OnDiskClosedHashIterator<Type>::Size() const
{
    return _iterator.Size();
}

template <PKeyTableType Type>
uint64_t OnDiskClosedHashIterator<Type>::Key() const
{
    return _iterator.Key();
}

template <PKeyTableType Type>
OnDiskPKeyOffset OnDiskClosedHashIterator<Type>::Value() const
{
    return OnDiskPKeyOffset(_iterator.Value());
}

} // namespace indexlibv2::index
