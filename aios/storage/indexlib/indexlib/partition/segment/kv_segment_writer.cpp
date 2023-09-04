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
#include "indexlib/partition/segment/kv_segment_writer.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/plain_format_encoder.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/document/document_parser/kv_parser/kv_key_extractor.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/kv_factory.h"
#include "indexlib/index/kv/kv_resource_assigner.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_factory.h"
#include "indexlib/index/util/dump_item_typed.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, KVSegmentWriter);

void KVSegmentWriter::Init(BuildingSegmentData& segmentData,
                           const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics,
                           const QuotaControlPtr& buildMemoryQuotaControler,
                           const BuildResourceMetricsPtr& buildResourceMetrics)
{
    mPool = std::make_shared<autil::mem_pool::Pool>();

    InitConfig();

    if (!buildResourceMetrics) {
        mBuildResourceMetrics.reset(new BuildResourceMetrics());
        mBuildResourceMetrics->Init();
    } else {
        mBuildResourceMetrics = buildResourceMetrics;
    }
    mBuildResourceMetricsNode = mBuildResourceMetrics->AllocateNode();
    segmentData.PrepareDirectory();
    mSegmentData = segmentData;

    mSegmentMetrics = metrics;
    mCurrentSegmentInfo.reset(new SegmentInfo(*segmentData.GetSegmentInfo()));

    KvResourceAssigner assigner(mKVConfig, mColumnIdx, mTotalColumnCount);
    assigner.Init(buildMemoryQuotaControler, mOptions);
    int64_t memQuota = assigner.Assign(mSegmentMetrics);
    double segInitMemUseRatio = 1.0;
    if (mSegmentMetrics->Get(SEGMENT_MEM_CONTROL_GROUP, SEGMENT_INIT_MEM_USE_RATIO, segInitMemUseRatio)) {
        IE_LOG(INFO, "kv segment init memory use ratio : %lf", segInitMemUseRatio);
    }
    memQuota = memQuota * segInitMemUseRatio;
    if (memQuota <= 0) {
        memQuota = 2 * 1024 * 1024;
    }

    const string& groupName = SegmentWriter::GetMetricsGroupName(GetColumnIdx());
    const std::shared_ptr<framework::SegmentGroupMetrics>& groupMetrics =
        mSegmentMetrics->GetSegmentGroupMetrics(groupName);
    mWriter = KVFactory::CreateWriter(mKVConfig);
    mWriter->Init(mKVConfig, mSegmentData.GetDirectory(), mOptions.IsOnline(), memQuota, groupMetrics);

    // get compress ratio for estimate dump file size
    if (mKVConfig->GetIndexPreference().GetValueParam().EnableFileCompress()) {
        double compressRatio = 1.0;
        mSegmentMetrics->Get(index::KV_COMPRESS_RATIO_GROUP_NAME, KV_VALUE_FILE_NAME, compressRatio);
        mWriter->SetValueCompressRatio(compressRatio);
        IE_LOG(INFO, "kv value compress ratio : %lf", compressRatio);
    }

    InitStorePkey();

    UpdateBuildResourceMetrics();
}

KVSegmentWriter* KVSegmentWriter::CloneWithNewSegmentData(BuildingSegmentData& segmentData, bool isShared) const
{
    assert(isShared);
    KVSegmentWriter* newSegmentWriter = new KVSegmentWriter(*this);
    segmentData.PrepareDirectory();
    newSegmentWriter->mSegmentData = segmentData;
    return newSegmentWriter;
}

void KVSegmentWriter::InitConfig()
{
    assert(mSchema->GetRegionCount() == 1);
    const config::IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema(DEFAULT_REGIONID);
    mKVConfig = DYNAMIC_POINTER_CAST(config::KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    assert(mKVConfig);

    // decide read value from pack attribute or not
    const ValueConfigPtr& valueConfig = mKVConfig->GetValueConfig();
    if (valueConfig->IsSimpleValue()) {
        const config::AttributeConfigPtr& attrConfig = valueConfig->GetAttributeConfig(0);
        assert(attrConfig);
        mValueFieldId = attrConfig->GetFieldId();
        mAttrConvertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    } else {
        mValueFieldId = INVALID_FIELDID;
        config::PackAttributeConfigPtr packAttrConfig = valueConfig->CreatePackAttributeConfig();
        assert(packAttrConfig);
        mAttrConvertor.reset(AttributeConvertorFactory::GetInstance()->CreatePackAttrConvertor(packAttrConfig));
        mEnableCompactPackAttribute = (valueConfig->GetFixedLength() != -1);
        mPlainFormatEncoder.reset(common::PackAttributeFormatter::CreatePlainFormatEncoder(packAttrConfig));
    }
    // decide key hash type
    assert(!mKVConfig->GetFieldConfig()->IsMultiValue());
    mKVKeyExtractor.reset(new KVKeyExtractor(mKVConfig));
}

void KVSegmentWriter::InitStorePkey()
{
    bool storePKey = false;
    if (autil::EnvUtil::getEnvWithoutDefault(NEED_STORE_PKEY_VALUE, storePKey)) {
        mNeedStorePKeyValue = storePKey;
    }
    if (mNeedStorePKeyValue) {
        auto pkeyFieldName = mKVConfig->GetKeyFieldName();
        AttributeConfigPtr attrConfig = mSchema->GetRegionSchema(0)->CreateAttributeConfig(pkeyFieldName);
        mAttributeWriter.reset(AttributeWriterFactory::GetInstance()->CreateAttributeWriter(
            attrConfig, mOptions, mBuildResourceMetrics.get()));
        AttributeConvertorFactory factory;
        mPkeyConvertor.reset(factory.CreateAttrConvertor(attrConfig, tt_kv));
        mKeyHasherType = mKVConfig->GetKeyHashFunctionType();
    }

    IE_LOG(INFO, "need store pkey value = [%d]", mNeedStorePKeyValue);
}

bool KVSegmentWriter::AddDocument(const DocumentPtr& document)
{
    assert(document);
    assert(mSchema->GetIndexSchema());
    auto kvDoc = dynamic_cast<document::KVDocument*>(document.get());
    bool ret = false;
    for (auto iter = kvDoc->begin(); iter != kvDoc->end(); ++iter) {
        auto& kvIndexDoc = *iter;
        ret = BuildSingleMessage(&kvIndexDoc);

        if (!ret && GetStatus() == util::NO_SPACE) {
            return false;
        }
    }

    return ret;
}

bool KVSegmentWriter::AddKVIndexDocument(document::KVIndexDocument* doc) { return BuildSingleMessage(doc); }

bool KVSegmentWriter::BuildSingleMessage(document::KVIndexDocument* doc)
{
    if (NeedForceDump()) {
        SetStatus(util::NO_SPACE);
        return false;
    }
    keytype_t pkey;
    StringView value;
    bool isDeleted;
    if (!ExtractDocInfo(doc, pkey, value, isDeleted)) {
        SetStatus(INVALID_ARGUMENT);
        return false;
    }

    uint32_t timestamp;
    timestamp = mKvDocTimestampDecider.GetTimestamp(doc);

    if (timestamp >= std::numeric_limits<int32_t>::max()) {
        timestamp = std::numeric_limits<int32_t>::max() - 1;
    }
    if (!mWriter->Add(pkey, value, timestamp, isDeleted, doc->GetRegionId())) {
        IE_LOG(WARN, "add document failed, keyHash[%lu]", pkey);
        ERROR_COLLECTOR_LOG(WARN, "add document failed, keyHash[%lu]", pkey);
        SetStatus(NO_SPACE);
        return false;
    }

    if (mNeedStorePKeyValue) {
        if (mKeyHasherType != hft_int64 && mKeyHasherType != hft_uint64) {
            auto pkeyFieleValue = doc->GetPkFieldValue();
            bool hasFormatError = false;
            auto convertedValue = mPkeyConvertor->Encode(pkeyFieleValue, mPool.get(), hasFormatError);
            if (hasFormatError) {
                IE_LOG(WARN, "pkey encode failed, pkey hash [%lu]", pkey);
                return false;
            }
            mHashKeyValueMap[pkey] = convertedValue;
        }
    }

    MEMORY_BARRIER();
    mCurrentSegmentInfo->docCount = mCurrentSegmentInfo->docCount + 1;
    SetStatus(OK);
    return true;
}

void KVSegmentWriter::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode) {
        return;
    }

    mBuildResourceMetricsNode->Update(BMT_CURRENT_MEMORY_USE, mWriter->GetMemoryUse());
    mBuildResourceMetricsNode->Update(BMT_DUMP_FILE_SIZE, mWriter->GetDumpFileExSize());
    mBuildResourceMetricsNode->Update(BMT_DUMP_TEMP_MEMORY_SIZE, mWriter->GetDumpTempMemUse());
}

void KVSegmentWriter::EndSegment() {}

void KVSegmentWriter::CreateDumpItems(const DirectoryPtr& directory,
                                      vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    assert(mWriter);
    std::shared_ptr<framework::SegmentMetrics> segmentMetrics(new framework::SegmentMetrics);
    CollectSegmentMetrics(segmentMetrics);
    segmentMetrics->Store(directory);

    mWriter->SetHashKeyValueMap(&mHashKeyValueMap);
    mWriter->SetAttributeWriter(mAttributeWriter);
    file_system::DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, true);
    file_system::DirectoryPtr kvDir = indexDir->GetDirectory(mKVConfig->GetIndexName(), true);
    dumpItems.push_back(std::make_unique<index::KVIndexDumpItem>(kvDir, mWriter));
}

index_base::InMemorySegmentReaderPtr KVSegmentWriter::CreateInMemSegmentReader()
{
    // hash table wiil use file system, so not need this
    return index_base::InMemorySegmentReaderPtr();
}

bool KVSegmentWriter::ExtractDocInfo(document::KVIndexDocument* doc, keytype_t& key, StringView& value, bool& isDeleted)
{
    assert(mSchema->GetRegionCount() == 1);
    if (doc->GetRegionId() != DEFAULT_REGIONID) {
        IE_LOG(WARN, "invalid region_id [%d] for single region kv table [%s]", doc->GetRegionId(),
               mSchema->GetSchemaName().c_str());
        ERROR_COLLECTOR_LOG(WARN, "invalid region_id [%d] for single region kv table [%s]", doc->GetRegionId(),
                            mSchema->GetSchemaName().c_str());
        return false;
    }

    if (!mKVKeyExtractor->GetKey(doc, key)) {
        return false;
    }

    if (!GetDeletedDocFlag(doc, isDeleted)) {
        return false;
    }

    if (isDeleted) {
        return true;
    }
    // value
    StringView fieldValue = doc->GetValue();
    if (fieldValue.empty()) {
        value = StringView::empty_instance();
        IE_LOG(DEBUG, "empty field value, pkHash[%lu]", key);
        ERROR_COLLECTOR_LOG(ERROR, "empty field value, pkHash[%lu]", key);
        return true;
    }
    const AttrValueMeta& meta = mAttrConvertor->Decode(fieldValue);
    if (mEnableCompactPackAttribute) {
        StringView tempData = meta.data;
        size_t headerSize = VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*tempData.data());
        value = StringView(tempData.data() + headerSize, tempData.size() - headerSize);
    } else {
        value = meta.data;
        if (mPlainFormatEncoder) {
            mMemBuffer.Reserve(value.size());
            if (!mPlainFormatEncoder->Encode(value, mMemBuffer.GetBuffer(), mMemBuffer.GetBufferSize(), value)) {
                return false;
            }
        }
    }
    return true;
}

size_t KVSegmentWriter::EstimateInitMemUse(config::KVIndexConfigPtr KVConfig, uint32_t columnIdx,
                                           uint32_t totalColumnCount, const config::IndexPartitionOptions& options,
                                           const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                           const util::QuotaControlPtr& buildMemoryQuotaControler)
{
    KvResourceAssigner assigner(KVConfig, columnIdx, totalColumnCount);
    assigner.Init(buildMemoryQuotaControler, options);
    return assigner.Assign(metrics);
}

bool KVSegmentWriter::GetDeletedDocFlag(document::KVIndexDocument* doc, bool& isDeleted) const
{
    // operation
    auto op = doc->GetDocOperateType();
    isDeleted = (op == DELETE_DOC);
    if (op != DELETE_DOC && op != ADD_DOC) {
        IE_LOG(WARN, "not support operation type[%d]", op);
        ERROR_COLLECTOR_LOG(WARN, "not support operation type[%d]", op);
        return false;
    }
    return true;
}

bool KVSegmentWriter::IsDirty() const { return mCurrentSegmentInfo->docCount > 0; }
}} // namespace indexlib::partition
