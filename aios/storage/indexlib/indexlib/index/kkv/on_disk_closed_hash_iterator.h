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

#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <PKeyTableType Type>
class OnDiskClosedHashIterator
{
public:
    OnDiskClosedHashIterator() {}
    ~OnDiskClosedHashIterator() {}

public:
    void Open(const config::KKVIndexConfigPtr& config, const index_base::SegmentData& segmentData);
    bool IsValid() const;
    void MoveToNext();
    void SortByKey();
    size_t Size() const;
    uint64_t Key() const;
    OnDiskPKeyOffset Value() const;
    void Reset() { mIterator.Reset(); }

private:
    typedef typename ClosedHashPrefixKeyTableTraits<uint64_t, Type>::Traits Traits;
    typedef typename Traits::template FileIterator<false> ClosedTableIterator;

private:
    ClosedTableIterator mIterator;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE_CUSTOM(PKeyTableType, index, OnDiskClosedHashIterator);

template <PKeyTableType Type>
void OnDiskClosedHashIterator<Type>::Open(const config::KKVIndexConfigPtr& config,
                                          const index_base::SegmentData& segmentData)
{
    file_system::DirectoryPtr dataDir = segmentData.GetIndexDirectory(config->GetIndexName(), true);
    file_system::FileReaderPtr fileReader = dataDir->CreateFileReader(
        PREFIX_KEY_FILE_NAME, file_system::ReaderOption::CacheFirst(file_system::FSOT_BUFFERED));
    if (!mIterator.Init(fileReader)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Init closed hash file iterator for file [%s] failed!",
                             fileReader->DebugString().c_str());
    }
    fileReader->Close().GetOrThrow();
}

template <PKeyTableType Type>
bool OnDiskClosedHashIterator<Type>::IsValid() const
{
    return mIterator.IsValid();
}

template <PKeyTableType Type>
void OnDiskClosedHashIterator<Type>::MoveToNext()
{
    mIterator.MoveToNext();
}

template <PKeyTableType Type>
void OnDiskClosedHashIterator<Type>::SortByKey()
{
    IE_LOG(INFO, "Begin SortByKey");
    mIterator.SortByKey();
    IE_LOG(INFO, "End SortByKey");
}

template <PKeyTableType Type>
size_t OnDiskClosedHashIterator<Type>::Size() const
{
    return mIterator.Size();
}

template <PKeyTableType Type>
uint64_t OnDiskClosedHashIterator<Type>::Key() const
{
    return mIterator.Key();
}

template <PKeyTableType Type>
OnDiskPKeyOffset OnDiskClosedHashIterator<Type>::Value() const
{
    return OnDiskPKeyOffset(mIterator.Value());
}
}} // namespace indexlib::index
