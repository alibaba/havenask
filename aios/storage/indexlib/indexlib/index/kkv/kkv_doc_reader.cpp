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
#include "indexlib/index/kkv/kkv_doc_reader.h"

#include "autil/StringUtil.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/plain_format_encoder.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/value_config.h"
#include "indexlib/index/kv/kv_ttl_decider.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KKVDocReader);

KKVDocReader::KKVDocReader() {}

KKVDocReader::~KKVDocReader() { mKKVIterator.reset(); }

bool KKVDocReader::Init(const IndexPartitionSchemaPtr& schema, PartitionDataPtr partData, uint32_t targetShardId,
                        int64_t currentTs, const string& ttlFieldName)
{
    if (!DocReaderBase::Init(schema, partData, currentTs, ttlFieldName)) {
        IE_LOG(ERROR, "doc reader base init failed");
        return false;
    }

    mKKVConfig = CreateDataKKVIndexConfig(mSchema);
    mTTLFromDoc = mSchema->TTLFromDoc();

    CreateSegmentDataVec(partData, targetShardId);

    GenerateSegmentIds();

    CreateKKVIterator();

    if (mNeedStorePKeyValue) {
        if (!CreateSegmentAttributeIteratorVec(partData)) {
            IE_LOG(ERROR, "create segment attribute iterator vec failed");
            return false;
        }
    }

    IE_LOG(INFO, "kkv doc reader init success");

    return true;
}

void KKVDocReader::GenerateSegmentIds()
{
    for (size_t i = 0; i < mSegmentDataVec.size(); ++i) {
        mSegmentIds.push_back(mSegmentDataVec[i].GetSegmentId());
    }
}

void KKVDocReader::CreateKKVIterator()
{
    mKKVIterator.reset(new OnDiskKKVIterator(mSchema, mKKVConfig));
    mKKVIterator->Init(mSegmentDataVec);
    for (size_t i = 0; i < mSegmentDataVec.size(); ++i) {
        mCurrentDocIds[i] = 0;
        mSegmentDocCountVec.push_back(mKKVIterator->GetPkeyCount(i));
    }
}

bool KKVDocReader::Read(RawDocument* doc, uint32_t& timestamp)
{
    if (mSessionPool.getUsedBytes() >= MAX_RELEASE_POOL_MEMORY_THRESHOLD) {
        mSessionPool.release();
    } else if (mSessionPool.getUsedBytes() >= MAX_RESET_POOL_MEMORY_THRESHOLD) {
        mSessionPool.reset();
    }

    while (mKKVIterator->IsValid()) {
        mCurrentSinglePKeyIter = mKKVIterator->GetCurrentIterator();
        if (ReadSinglePrefixKey(doc, timestamp)) {
            return true;
        }
        MoveToNextPkeyIter();
    }
    return false;
}

bool KKVDocReader::Seek(int64_t pkeyOffset, int64_t skeyOffset)
{
    mPkeyOffset = 0;
    mSkeyOffset = 0;
    mCurrentDocIds.clear();

    if (mKKVIterator->IsEmpty()) {
        return true;
    }

    while (mPkeyOffset < pkeyOffset && mKKVIterator->IsValid()) {
        mCurrentSinglePKeyIter = mKKVIterator->GetCurrentIterator();
        MoveToNextPkeyIter();
    }
    if (!mKKVIterator->IsValid()) {
        IE_LOG(ERROR, "seek to pkey position [%ld] failed!", pkeyOffset);
        return false;
    }
    mCurrentSinglePKeyIter = mKKVIterator->GetCurrentIterator();
    while (mSkeyOffset < skeyOffset) {
        MoveToNextSkeyIter();
    }
    return mCurrentSinglePKeyIter->IsValid();
}

pair<int64_t, int64_t> KKVDocReader::GetCurrentOffset() const
{
    return make_pair(mLastValidPkeyOffset, mLastValidSkeyOffset);
}

inline void KKVDocReader::MoveToFirstValidSKeyPosition()
{
    while (mCurrentSinglePKeyIter->IsValid()) {
        if (!mTTLFromDoc && mTTLDecider->IsExpiredItem(mCurrentSinglePKeyIter->GetRegionId(),
                                                       mCurrentSinglePKeyIter->GetCurrentTs(), mCurrentTsInSecond)) {
            MoveToNextSkeyIter();
            continue;
        }

        uint64_t skeyHash = 0;
        bool isDeleted = false;
        mCurrentSinglePKeyIter->GetCurrentSkey(skeyHash, isDeleted);
        if (isDeleted || mCurrentSinglePKeyIter->CurrentSKeyExpired(mCurrentTsInSecond)) {
            MoveToNextSkeyIter();
            continue;
        }
        break;
    }
}

inline void KKVDocReader::MoveToNextValidSKeyPosition()
{
    MoveToNextSkeyIter();
    MoveToFirstValidSKeyPosition();
}

bool KKVDocReader::ReadSinglePrefixKey(RawDocument* doc, uint32_t& timestamp)
{
    MoveToFirstValidSKeyPosition();
    while (mCurrentSinglePKeyIter->IsValid()) {
        bool hasPkeyDeleted = mCurrentSinglePKeyIter->HasPKeyDeleted();
        uint32_t pkeyDeletedTs = mCurrentSinglePKeyIter->GetPKeyDeletedTs();
        uint64_t skeyHash = 0;
        bool isDeleted = false;
        mCurrentSinglePKeyIter->GetCurrentSkey(skeyHash, isDeleted);
        timestamp = mCurrentSinglePKeyIter->GetCurrentTs();
        uint32_t expireTime = mCurrentSinglePKeyIter->GetCurrentExpireTime();
        auto pkeyHash = mCurrentSinglePKeyIter->GetPKeyHash();
        if (!isDeleted && hasPkeyDeleted) {
            if (timestamp < pkeyDeletedTs) {
                isDeleted = true;
            }
        }

        if (!isDeleted && !(mTTLFromDoc && mCurrentTsInSecond > expireTime)) {
            StringView value;
            mCurrentSinglePKeyIter->GetCurrentValue(value);
            if (!mCurrentSinglePKeyIter->CurrentSKeyExpired(mCurrentTsInSecond)) {
                mLastValidPkeyOffset = mPkeyOffset;
                mLastValidSkeyOffset = mSkeyOffset;
                mCurrentSegmentIdx = mCurrentSinglePKeyIter->GetCurrentSegmentIdx();
                mCurrentDocId = mCurrentDocIds[mCurrentSegmentIdx];
                if (!ReadValue(pkeyHash, skeyHash, value, doc)) {
                    return false;
                }
                if (expireTime != UNINITIALIZED_EXPIRE_TIME && !mTTLFieldName.empty()) {
                    doc->setField(mTTLFieldName, StringUtil::toString(expireTime - timestamp));
                }
                MoveToNextValidSKeyPosition();
                return true;
            }
        }
        MoveToNextValidSKeyPosition();
    }
    return false;
}

bool KKVDocReader::ReadValue(const keytype_t pkeyHash, const uint64_t skeyHash, StringView value, RawDocument* doc)
{
    if (mNeedStorePKeyValue) {
        if (!ReadPkeyValue(mCurrentSegmentIdx, mCurrentDocId, pkeyHash, doc)) {
            IE_LOG(WARN, "read pkey value failed, pkeyHash[%lu], current segment idx[%ld]", pkeyHash,
                   mCurrentSegmentIdx);
            return false;
        }
    }

    if (mPlainFormatEncoder) {
        if (!mPlainFormatEncoder->Decode(value, &mSessionPool, value)) {
            IE_LOG(ERROR, "plain format decode failed, pkeyHash[%lu], skeyHash[%lu]", pkeyHash, skeyHash);
            return false;
        }
    }

    auto& attrConfigs = mFormatter->GetPackAttributeConfig()->GetAttributeConfigVec();
    for (size_t i = 0; i < attrConfigs.size(); ++i) {
        AttributeReference* attrRef = mFormatter->GetAttributeReference(attrConfigs[i]->GetAttrId());
        string attrValue;
        attrRef->GetStrValue(value.data(), attrValue, &mSessionPool);
        doc->setField(attrRef->GetAttrName(), attrValue);
    }
    if (mKKVConfig->OptimizedStoreSKey()) {
        doc->setField(mKKVConfig->GetSuffixFieldName(), StringUtil::toString(skeyHash));
    }
    return true;
}

}} // namespace indexlib::index
