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

#include <map>

#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/on_disk_kkv_iterator.h"
#include "indexlib/index/kv/doc_reader_base.h"

namespace indexlib { namespace index {

class KKVDocReader : public DocReaderBase
{
public:
    KKVDocReader();
    virtual ~KKVDocReader();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema, index_base::PartitionDataPtr partData,
              uint32_t targetShardId, int64_t currentTs, const std::string& ttlFieldName) override;
    bool Read(document::RawDocument* doc, uint32_t& timestamp) override;
    bool Seek(int64_t segmentIdx, int64_t offset) override;
    std::pair<int64_t, int64_t> GetCurrentOffset() const override;
    bool IsEof() const override { return !mKKVIterator->IsValid(); }

private:
    void GenerateSegmentIds();
    void CreateKKVIterator();

    bool ReadSinglePrefixKey(document::RawDocument* doc, uint32_t& timestamp);
    void MoveToFirstValidSKeyPosition();
    void MoveToNextValidSKeyPosition();
    void MoveToNextPkeyIter()
    {
        auto idxs = mCurrentSinglePKeyIter->GetSegementIdxs();
        for (auto idx : idxs) {
            mCurrentDocIds[idx]++;
        }

        mKKVIterator->MoveToNext();
        mPkeyOffset++;
        mSkeyOffset = 0;
    }
    void MoveToNextSkeyIter()
    {
        mCurrentSinglePKeyIter->MoveToNext();
        mSkeyOffset++;
    }
    bool ReadValue(const keytype_t pkeyHash, const uint64_t skeyHash, autil::StringView value,
                   document::RawDocument* doc);

private:
    config::KKVIndexConfigPtr mKKVConfig;
    OnDiskKKVIteratorPtr mKKVIterator;
    std::map<size_t, docid_t> mCurrentDocIds;
    OnDiskSinglePKeyIterator* mCurrentSinglePKeyIter = nullptr;

    bool mTTLFromDoc = false;
    int64_t mPkeyOffset = 0;
    int64_t mSkeyOffset = 0;
    int64_t mLastValidPkeyOffset = 0;
    int64_t mLastValidSkeyOffset = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KKVDocReader);
}} // namespace indexlib::index
