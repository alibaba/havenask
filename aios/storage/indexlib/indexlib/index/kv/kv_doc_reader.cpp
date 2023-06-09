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
#include "indexlib/index/kv/kv_doc_reader.h"

#include "indexlib/common/field_format/pack_attribute/attribute_reference.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index/kv/kv_merge_writer.h"
#include "indexlib/index/kv/kv_segment_iterator.h"
#include "indexlib/index/kv/kv_ttl_decider.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::document;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KVDocReader);

KVDocReader::KVDocReader() {}

KVDocReader::~KVDocReader() { mSegmentIteratorVec.clear(); }

bool KVDocReader::Init(const IndexPartitionSchemaPtr& schema, PartitionDataPtr partData, uint32_t targetShardId,
                       int64_t currentTs, const string& ttlFieldName)
{
    if (!DocReaderBase::Init(schema, partData, currentTs, ttlFieldName)) {
        IE_LOG(ERROR, "doc reader base init failed");
        return false;
    }

    CreateSegmentDataVec(partData, targetShardId);

    GenerateSegmentIds();

    if (!CreateKVSegmentIteratorVec()) {
        IE_LOG(ERROR, "create kv segment iterator vec failed");
        return false;
    }

    if (mNeedStorePKeyValue) {
        if (!CreateSegmentAttributeIteratorVec(partData)) {
            IE_LOG(ERROR, "create segment attribute iterator vec failed");
            return false;
        }
    }

    IE_LOG(INFO, "create kv doc reader success");

    return true;
}

void KVDocReader::GenerateSegmentIds()
{
    reverse(mSegmentDataVec.begin(), mSegmentDataVec.end());
    for (size_t i = 0; i < mSegmentDataVec.size(); ++i) {
        mSegmentIds.push_back(mSegmentDataVec[i].GetSegmentId());
    }
}

bool KVDocReader::CreateKVSegmentIteratorVec()
{
    for (size_t i = 0; i < mSegmentDataVec.size(); ++i) {
        auto directory = mSegmentDataVec[i].GetDirectory();
        auto indexDir = directory->GetDirectory(INDEX_DIR_NAME, true);
        auto kvDir = indexDir->GetDirectory(mKVIndexConfig->GetIndexName(), true);
        KVFormatOptionsPtr kvOptions(new KVFormatOptions());
        if (kvDir->IsExist(KV_FORMAT_OPTION_FILE_NAME)) {
            std::string content;
            kvDir->Load(KV_FORMAT_OPTION_FILE_NAME, content);
            kvOptions->FromString(content);
        }
        bool needRandomAccess = (i != mSegmentDataVec.size() - 1);
        KVSegmentIteratorPtr segmentIterator(new KVSegmentIterator());
        if (!segmentIterator->Open(mSchema, kvOptions, mSegmentDataVec[i], needRandomAccess)) {
            IE_LOG(WARN, "open segment iterator failed, segment [%s]", mSegmentDataVec[i].GetSegmentDirName().c_str());
            return false;
        }
        mSegmentIteratorVec.push_back(segmentIterator);
        mSegmentDocCountVec.push_back(segmentIterator->Size());
    }
    return true;
}

util::Status KVDocReader::ProcessOneDoc(KVSegmentIterator& segmentIterator, RawDocument* doc, uint32_t& timestamp)
{
    keytype_t pkeyHash;
    bool isDeleted = false;
    segmentIterator.GetKey(pkeyHash, isDeleted);
    if (isDeleted) {
        return util::NOT_FOUND;
    }

    bool canEmit = true;
    for (int fresherSegIdx = 0; fresherSegIdx < mCurrentSegmentIdx; ++fresherSegIdx) {
        StringView findValue;
        util::Status status = mSegmentIteratorVec[fresherSegIdx]->Find(pkeyHash, findValue);
        if (util::NOT_FOUND != status) {
            canEmit = false;
            break;
        }
    }
    if (canEmit) {
        StringView value;
        regionid_t regionId;
        segmentIterator.GetValue(value, timestamp, regionId, false);
        if (!mTTLDecider->IsExpiredItem(regionId, timestamp, mCurrentTsInSecond)) {
            mLastValidSegmentIdx = mCurrentSegmentIdx;
            mLastValidIteratorOffset = mSegmentIteratorVec[mCurrentSegmentIdx]->GetOffset();
            if (!ReadValue(pkeyHash, value, doc)) {
                return util::FAIL;
            }
            mCurrentDocId++;
            segmentIterator.MoveToNext();
            return util::OK;
        }
    }
    return util::NOT_FOUND;
}

bool KVDocReader::Read(RawDocument* doc, uint32_t& timestamp)
{
    if (mSessionPool.getUsedBytes() >= MAX_RELEASE_POOL_MEMORY_THRESHOLD) {
        mSessionPool.release();
    } else if (mSessionPool.getUsedBytes() >= MAX_RESET_POOL_MEMORY_THRESHOLD) {
        mSessionPool.reset();
    }

    while (mCurrentSegmentIdx < mSegmentIteratorVec.size()) {
        auto& segmentIterator = *mSegmentIteratorVec[mCurrentSegmentIdx];
        while (segmentIterator.IsValid()) {
            auto status = ProcessOneDoc(segmentIterator, doc, timestamp);
            if (status == util::OK) {
                return true;
            } else if (status == util::FAIL) {
                return false;
            }
            mCurrentDocId++;
            segmentIterator.MoveToNext();
        }
        mCurrentSegmentIdx++;
        if (mCurrentSegmentIdx < mSegmentIteratorVec.size()) {
            mSegmentIteratorVec[mCurrentSegmentIdx]->Seek(0);
            mCurrentDocId = 0;
        }
    }
    return false;
}

bool KVDocReader::Seek(int64_t segmentIdx, int64_t offset)
{
    if (segmentIdx < 0 || segmentIdx >= mSegmentIteratorVec.size()) {
        IE_LOG(WARN, "target segment idx [%ld] invalid, vector size [%lu]", segmentIdx, mSegmentIteratorVec.size());
        return false;
    }
    mCurrentSegmentIdx = segmentIdx;
    return mSegmentIteratorVec[mCurrentSegmentIdx]->Seek(offset);
}

pair<int64_t, int64_t> KVDocReader::GetCurrentOffset() const
{
    return make_pair(mLastValidSegmentIdx, mLastValidIteratorOffset);
}

bool KVDocReader::IsEof() const
{
    return (mCurrentSegmentIdx + 1 == mSegmentIteratorVec.size() && !(*mSegmentIteratorVec.rbegin())->IsValid()) ||
           mCurrentSegmentIdx + 1 > mSegmentIteratorVec.size();
}

bool KVDocReader::ReadSimpleValue(const autil::StringView& value, RawDocument* doc)
{
    FieldType fieldType = mValueConfig->GetAttributeConfig(0)->GetFieldType();
    auto& fieldName = mValueConfig->GetAttributeConfig(0)->GetAttrName();
#define EMIT_SIMPLE_SINGLE_VALUE(fieldType)                                                                            \
    case fieldType: {                                                                                                  \
        typedef IndexlibFieldType2CppType<fieldType, false>::CppType cppType;                                          \
        doc->setField(fieldName, StringUtil::toString((*(cppType*)value.data())));                                     \
        break;                                                                                                         \
    }

    switch (fieldType) {
        BASIC_NUMBER_FIELD_MACRO_HELPER(EMIT_SIMPLE_SINGLE_VALUE);

    default:
        IE_LOG(WARN, "fieldType[%d] is not support in simple value", fieldType);
        return false;
    }
    return true;
}

bool KVDocReader::ReadPackValue(const autil::StringView& value, RawDocument* doc)
{
    StringView packData;
    if (mValueConfig->GetFixedLength() != -1) {
        packData = value;
    } else {
        size_t encodeCountLen = 0;
        MultiValueFormatter::decodeCount(value.data(), encodeCountLen);
        packData = StringView(value.data() + encodeCountLen, value.size() - encodeCountLen);
        if (mPlainFormatEncoder && !mPlainFormatEncoder->Decode(packData, &mSessionPool, packData)) {
            IE_LOG(WARN, "plain format decode failed");
            return false;
        }
    }

    auto& attrConfigs = mFormatter->GetPackAttributeConfig()->GetAttributeConfigVec();
    for (size_t i = 0; i < attrConfigs.size(); ++i) {
        AttributeReference* attrRef = mFormatter->GetAttributeReference(attrConfigs[i]->GetAttrId());
        string attrValue;
        if (!attrRef->GetStrValue(packData.data(), attrValue, &mSessionPool)) {
            IE_LOG(WARN, "attr ref [%s] read str value failed", attrRef->GetAttrName().c_str());
            return false;
        }
        doc->setField(attrRef->GetAttrName(), attrValue);
    }

    return true;
}

bool KVDocReader::ReadValue(const keytype_t pkeyHash, const autil::StringView& value, RawDocument* doc)
{
    if (mNeedStorePKeyValue) {
        if (!ReadPkeyValue(mCurrentSegmentIdx, mCurrentDocId, pkeyHash, doc)) {
            IE_LOG(WARN, "read pkey value failed, pkeyHash[%lu], current segment idx[%ld]", pkeyHash,
                   mCurrentSegmentIdx);
            return false;
        }
    }

    if (mValueConfig->IsSimpleValue()) {
        return ReadSimpleValue(value, doc);
    } else {
        return ReadPackValue(value, doc);
    }
}

}} // namespace indexlib::index
