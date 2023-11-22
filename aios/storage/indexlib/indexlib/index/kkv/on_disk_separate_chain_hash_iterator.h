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

#include "indexlib/common/hash_table/hash_table_reader.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class OnDiskSeparateChainHashIterator
{
public:
    OnDiskSeparateChainHashIterator();
    ~OnDiskSeparateChainHashIterator();

public:
    void Open(const config::KKVIndexConfigPtr& config, const index_base::SegmentData& segmentData);
    bool IsValid() const;
    void MoveToNext();
    void SortByKey();
    size_t Size() const;
    uint64_t Key() const;
    OnDiskPKeyOffset Value() const;
    void Reset()
    {
        mCursor = 0;
        ReadCurrentKeyValue();
    }

private:
    void ReadCurrentKeyValue();

private:
    typedef common::HashTableReader<uint64_t, OnDiskPKeyOffset> HashTable;
    typedef common::HashTableNode<uint64_t, OnDiskPKeyOffset> HashNode;

private:
    file_system::FileReaderPtr mFileReader;
    uint32_t mSize;
    uint32_t mCursor;
    uint64_t mKey;
    OnDiskPKeyOffset mValue;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskSeparateChainHashIterator);
}} // namespace indexlib::index
