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
#ifndef __INDEXLIB_ON_DISK_KKV_SEGMENT_ITERATOR_H
#define __INDEXLIB_ON_DISK_KKV_SEGMENT_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_segment_iterator.h"
#include "indexlib/index/kkv/on_disk_pkey_hash_iterator_creator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class OnDiskKKVSegmentIterator
{
public:
    typedef KKVSegmentIterator SinglePKeyIterator;

public:
    OnDiskKKVSegmentIterator(size_t segIdx);
    ~OnDiskKKVSegmentIterator();

public:
    void Init(const config::KKVIndexConfigPtr& kkvIndexConfig, const index_base::SegmentData& segmentData);

    void ResetBufferParam(size_t readerBufferSize, bool asyncRead);
    void Reset() { mPKeyTable->Reset(); }
    bool IsValid() const;
    void MoveToNext() const;
    uint64_t GetPrefixKey(regionid_t& regionId) const;
    size_t GetPkeyCount() { return mPKeyTable->Size(); }
    SinglePKeyIterator* GetIterator(autil::mem_pool::Pool* pool, const config::KKVIndexConfigPtr& regionKkvConfig);

    size_t GetSegmentIdx() const { return mSegmentIdx; }
    bool KeepSortSequence() const { return mKeepSortSequence; }

private:
    void ResetBufferParam(const file_system::FileReaderPtr& reader, size_t readerBufferSize, bool asyncRead);

private:
    typedef PrefixKeyTableIteratorBase<OnDiskPKeyOffset> PKeyTableIterator;
    DEFINE_SHARED_PTR(PKeyTableIterator);

private:
    PKeyTableIteratorPtr mPKeyTable;
    file_system::FileReaderPtr mSkeyReader;
    file_system::FileReaderPtr mValueReader;
    size_t mSegmentIdx;
    FieldType mSkeyDictType;
    config::KKVIndexConfigPtr mKKVIndexConfig;
    bool mStoreTs;
    bool mKeepSortSequence;
    uint32_t mDefaultTs;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskKKVSegmentIterator);
}} // namespace indexlib::index

#endif //__INDEXLIB_ON_DISK_KKV_SEGMENT_ITERATOR_H
