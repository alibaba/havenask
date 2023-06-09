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
#include "indexlib/index/kv/doc_reader_base.h"

#include "autil/memory.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/plain_format_encoder.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/index/kv/kv_ttl_decider.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/on_disk_partition_data.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DocReaderBase);

DocReaderBase::DocReaderBase() {}

DocReaderBase::~DocReaderBase() {}

bool DocReaderBase::Init(const IndexPartitionSchemaPtr& schema, const PartitionDataPtr partData, int64_t currentTs,
                         const std::string& ttlFieldName)
{
    mSchema = schema;
    mPartitionData = partData;
    mTTLFieldName = ttlFieldName;
    mTTLDecider.reset(new KVTTLDecider);
    mTTLDecider->Init(mSchema);
    mCurrentTsInSecond = MicrosecondToSecond(currentTs);
    mKVIndexConfig = CreateDataKVIndexConfig(mSchema);
    mPkeyAttrConfig = mSchema->GetRegionSchema(0)->CreateAttributeConfig(mKVIndexConfig->GetKeyFieldName());
    mKeyHasherType = mKVIndexConfig->GetKeyHashFunctionType();
    mValueConfig = mKVIndexConfig->GetValueConfig();

    mPlainFormatEncoder.reset(
        PackAttributeFormatter::CreatePlainFormatEncoder(mValueConfig->CreatePackAttributeConfig()));
    mFormatter.reset(new PackAttributeFormatter());
    if (!mFormatter->Init(mValueConfig->CreatePackAttributeConfig())) {
        IE_LOG(ERROR, "formatter init failed");
        return false;
    }

    bool storePKey = false;
    char* needStorePKeyValue = getenv(NEED_STORE_PKEY_VALUE);
    if (needStorePKeyValue && autil::StringUtil::fromString(std::string(needStorePKeyValue), storePKey)) {
        mNeedStorePKeyValue = storePKey;
    }

    return true;
}

void DocReaderBase::CreateSegmentDataVec(const PartitionDataPtr& partData, uint32_t targetShardId)
{
    for (auto segmentData = partData->Begin(); segmentData < partData->End(); ++segmentData) {
        if (targetShardId != std::numeric_limits<uint32_t>::max() &&
            segmentData->GetShardId() != std::numeric_limits<uint32_t>::max() &&
            targetShardId != segmentData->GetShardId()) {
            continue;
        }
        mSegmentDataVec.push_back(*segmentData);
    }
}

bool DocReaderBase::CreateSegmentAttributeIteratorVec(const PartitionDataPtr& partData)
{
    for (size_t i = 0; i < mSegmentDataVec.size(); ++i) {
        SegmentDirectoryPtr segmentDir = partData->GetSegmentDirectory();
        SegmentDirectoryPtr attrSegDir(segmentDir->Clone());
        auto& segmentDatas = attrSegDir->GetSegmentDatas();
        segmentDatas.clear();
        segmentDatas.push_back(mSegmentDataVec[i]);

        const plugin::PluginManagerPtr pluginManager;
        partition::OnDiskPartitionDataPtr attrPartData(new partition::OnDiskPartitionData(pluginManager));
        attrPartData->Open(attrSegDir);

        // reset the doc count in segment info, because of the doc count in segment info is not equal to the key count.
        auto segmentInfoPtr = attrPartData->Begin()->GetSegmentInfo();
        SegmentInfo segmentInfo = *segmentInfoPtr;
        segmentInfo.docCount = mSegmentDocCountVec[i];
        attrPartData->GetSegmentDatas()[0].SetSegmentInfo(segmentInfo);

        config::ReadPreference mReadPreference = RP_RANDOM_PREFER;
        auto attrReader = AttributeReaderFactory::CreateAttributeReader(mPkeyAttrConfig, attrPartData, mReadPreference,
                                                                        nullptr, nullptr);
        if (!attrReader) {
            IE_LOG(ERROR, "create attribute reader failed");
            return false;
        }
        auto attrIter = autil::shared_ptr_pool(&mPool, attrReader->CreateIterator(&mPool));
        if (!attrIter) {
            IE_LOG(ERROR, "create attribute iterator failed");
            return false;
        }
        mAttributeSegmentReaderVec.push_back(attrReader);
        mAttributeSegmentIteratorVec.push_back(attrIter);
    }
    IE_LOG(INFO, "create attribute iterator success");
    return true;
}

bool DocReaderBase::ReadNumberTypePKeyValue(const size_t currentSegmentIdx, docid_t currentDocId,
                                            const keytype_t pkeyHash, std::string& pkeyValue)
{
    auto pkeyType = mPkeyAttrConfig->GetFieldConfig()->GetFieldType();
    auto isMulti = mPkeyAttrConfig->GetFieldConfig()->IsMultiValue();
    switch (pkeyType) {
#define SEEK_PKEY_MACRO(type)                                                                                          \
    case type: {                                                                                                       \
        if (isMulti) {                                                                                                 \
            IE_LOG(WARN, "not support multi type pkey, pkeyType is [%d]", pkeyType);                                   \
            return false;                                                                                              \
        } else {                                                                                                       \
            typedef IndexlibFieldType2CppType<type, false>::CppType cppType;                                           \
            typedef indexlib::index::AttributeIteratorTyped<cppType> IteratorType;                                     \
            IteratorType* iter = static_cast<IteratorType*>(mAttributeSegmentIteratorVec[currentSegmentIdx].get());    \
            cppType pkeyTypeValue;                                                                                     \
            if (likely(iter->Seek(currentDocId, pkeyTypeValue))) {                                                     \
                pkeyValue = StringUtil::toString(pkeyTypeValue);                                                       \
                return true;                                                                                           \
            } else {                                                                                                   \
                IE_LOG(WARN, "seek failed, currentDocId = [%d], pkeyHash = %ld", currentDocId, pkeyHash);              \
                return false;                                                                                          \
            }                                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }

        BASIC_NUMBER_FIELD_MACRO_HELPER(SEEK_PKEY_MACRO)
#undef SEEK_PKEY_MACRO

    default:
        IE_LOG(ERROR, "unsupport field type [%d] for pkey", pkeyType);
        return false;
    }
    return false;
}

bool DocReaderBase::ReadStringTypePKeyValue(const size_t currentSegmentIdx, docid_t currentDocId,
                                            const keytype_t pkeyHash, std::string& pkeyValue)
{
    typedef indexlib::index::AttributeIteratorTyped<MultiChar> IteratorType;
    IteratorType* iter = static_cast<IteratorType*>(mAttributeSegmentIteratorVec[currentSegmentIdx].get());
    MultiChar pkeyValueData;
    try {
        if (likely(iter->Seek(currentDocId, pkeyValueData))) {
            pkeyValue = string(pkeyValueData.data(), pkeyValueData.size());
            return true;
        } else {
            IE_LOG(WARN, "seek failed, currentSegmentIdx = [%ld], currentDocId = [%d], pkeyHash = %lu",
                   currentSegmentIdx, currentDocId, pkeyHash);
            return false;
        }
    } catch (std::exception& e) {
        IE_LOG(WARN, "seek failed, currentSegmentIdx = [%ld], currentDocId = [%d], pkeyHash = %lu", currentSegmentIdx,
               currentDocId, pkeyHash);
        return false;
    }
}

bool DocReaderBase::ReadPkeyValue(const size_t currentSegmentIdx, docid_t currentDocId, const keytype_t pkeyHash,
                                  document::RawDocument* doc)
{
    string pkeyValue;
    if (ReadPkeyValue(currentSegmentIdx, currentDocId, pkeyHash, pkeyValue)) {
        auto& name = mKVIndexConfig->GetKeyFieldName();
        StringView pkeyName(name.data(), name.length());
        StringView pkeyStrValue(pkeyValue.data(), pkeyValue.length());
        doc->setField(pkeyName, pkeyStrValue);
        return true;
    }
    return false;
}

bool DocReaderBase::ReadPkeyValue(const size_t currentSegmentIdx, docid_t currentDocId, const keytype_t pkeyHash,
                                  std::string& pkeyValue)
{
    if (mKeyHasherType == hft_int64 || mKeyHasherType == hft_uint64) {
        pkeyValue = StringUtil::toString(pkeyHash);
        return true;
    }

    auto pkeyType = mPkeyAttrConfig->GetFieldConfig()->GetFieldType();
    switch (pkeyType) {
#define SEEK_NUMBER_PKEY_MACRO(type)                                                                                   \
    case type: {                                                                                                       \
        return ReadNumberTypePKeyValue(currentSegmentIdx, currentDocId, pkeyHash, pkeyValue);                          \
    }

        BASIC_NUMBER_FIELD_MACRO_HELPER(SEEK_NUMBER_PKEY_MACRO)
#undef SEEK_NUMBER_PKEY_MACRO

    case ft_string: {
        return ReadStringTypePKeyValue(currentSegmentIdx, currentDocId, pkeyHash, pkeyValue);
    }
    default: {
        IE_LOG(ERROR, "unsupport field type [%d] for pkey", pkeyType);
        return false;
    }
    }
    return false;
}

}} // namespace indexlib::index
