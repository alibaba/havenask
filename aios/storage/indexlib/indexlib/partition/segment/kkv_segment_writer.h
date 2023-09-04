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
#ifndef __INDEXLIB_KKV_SEGMENT_WRITER_H
#define __INDEXLIB_KKV_SEGMENT_WRITER_H

#include <memory>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/plain_format_encoder.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/document/document_parser/kv_parser/kkv_keys_extractor.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kkv/building_kkv_segment_reader.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/kkv_index_dumper.h"
#include "indexlib/index/kkv/kkv_merger_typed.h"
#include "indexlib/index/kkv/prefix_key_resource_assigner.h"
#include "indexlib/index/kkv/prefix_key_table_creator.h"
#include "indexlib/index/kkv/prefix_key_table_seeker.h"
#include "indexlib/index/kkv/suffix_key_writer.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_doc_timestamp_decider.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_factory.h"
#include "indexlib/index/util/dump_item_typed.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyHasherTyped.h"
#include "indexlib/util/MemBuffer.h"
#include "indexlib/util/Status.h"
#include "indexlib/util/ValueWriter.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib { namespace partition {

std::string GetKKVWriterMetricsGroupName(uint32_t);

template <typename SKeyType>
class KKVSegmentWriter : public index_base::SegmentWriter
{
protected:
    typedef typename index::PrefixKeyTableBase<index::SKeyListInfo> PKeyTable;
    DEFINE_SHARED_PTR(PKeyTable);

    typedef typename index::BuildingKKVSegmentReader<SKeyType>::SKeyWriter SKeyWriter;
    DEFINE_SHARED_PTR(SKeyWriter);

public:
    KKVSegmentWriter(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                     uint32_t columnIdx = 0, uint32_t totalColumnCount = 1);
    ~KKVSegmentWriter();

public:
    static constexpr double KKV_HASH_TABLE_FACTOR = 1.66;
    static const size_t KKV_HASH_TABLE_INIT_SIZE = 1024 * 1024;

public:
    void Init(index_base::BuildingSegmentData& segmentData, const std::shared_ptr<framework::SegmentMetrics>& metrics,
              const util::QuotaControlPtr& buildMemoryQuotaControler,
              const util::BuildResourceMetricsPtr& buildResourceMetrics = util::BuildResourceMetricsPtr()) override;

    KKVSegmentWriter<SKeyType>* CloneWithNewSegmentData(index_base::BuildingSegmentData& segmentData,
                                                        bool isShared) const override;

    void CollectSegmentMetrics() override;
    void CollectSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics);
    bool AddDocument(const document::DocumentPtr& doc) override;
    bool AddKVIndexDocument(document::KVIndexDocument* doc) override;
    bool IsDirty() const override;
    void EndSegment() override;
    void CreateDumpItems(const file_system::DirectoryPtr& directory,
                         std::vector<std::unique_ptr<common::DumpItem>>& dumpItems) override;
    index_base::InMemorySegmentReaderPtr CreateInMemSegmentReader() override;
    size_t GetTotalMemoryUse() const override;
    const index_base::SegmentInfoPtr& GetSegmentInfo() const override;
    InMemorySegmentModifierPtr GetInMemorySegmentModifier() override;

    bool NeedForceDump() override;

protected:
    virtual void DoInit()
    {
        assert(mSchema->GetRegionCount() == 1);
        const config::IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
        mKKVConfig = DYNAMIC_POINTER_CAST(config::KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        mKKVKeysExtractor.reset(new document::KKVKeysExtractor(mKKVConfig));
        mPlainFormatEncoder.reset(common::PackAttributeFormatter::CreatePlainFormatEncoder(
            mKKVConfig->GetValueConfig()->CreatePackAttributeConfig()));
    }

    virtual bool GetKeys(document::KVIndexDocument* doc, index::PKeyType& pkey, SKeyType& skey,
                         document::KKVKeysExtractor::OperationType& operationType)
    {
        uint64_t uSkey = 0;
        if (!mKKVKeysExtractor->GetKeys(doc, pkey, uSkey, operationType)) {
            return false;
        }
        skey = (SKeyType)uSkey;
        return true;
    }

private:
    static double CalculateMemoryRatio(const std::shared_ptr<framework::SegmentGroupMetrics>& groupMetrics,
                                       int64_t maxMemoryUse);

private:
    void InitStorePkey();
    void UpdateBuildResourceMetrics(uint32_t valueLen);
    bool ExtractDocInfo(document::KVIndexDocument* doc, index::PKeyType& pkey, SKeyType& skey, autil::StringView& value,
                        document::KKVKeysExtractor::OperationType& operationType);
    bool DeleteSKey(index::PKeyType pkey, SKeyType skey, uint32_t ts, regionid_t regionId);
    bool DeletePKey(index::PKeyType pkey, uint32_t ts, regionid_t regionId);
    bool InnerAddDocument(index::PKeyType pkey, SKeyType skey, const autil::StringView& value, uint32_t ts,
                          regionid_t regionId, uint32_t expireTime);

    bool BuildSingleMessage(document::KVIndexDocument* doc, size_t* valueSize);
    bool StorePkeyValue(index::PKeyType pkey, document::KVIndexDocument* doc);

    uint32_t GetExpireTime(document::KVIndexDocument* doc) const;

    file_system::DirectoryPtr CreateKKVDirectory();
    void InitValueWriter(int64_t maxValueMemoryUse);
    size_t EstimateDumpFileSize();

    bool CheckRegionId(index::SKeyListInfo& listInfo, regionid_t id) const { return listInfo.regionId == id; }

    index::KKVIndexDumperBasePtr CreateIndexDumper(const PKeyTable::IteratorPtr& pkeyIterator)
    {
        index::KKVIndexDumper<SKeyType>* dumpWriter =
            new index::KKVIndexDumper<SKeyType>(mSchema, mKKVConfig, mOptions);
        dumpWriter->Init(pkeyIterator, mSKeyWriter, mValueWriter);
        return index::KKVIndexDumperBasePtr(dumpWriter);
    }

protected:
    config::KKVIndexConfigPtr mKKVConfig;
    index_base::BuildingSegmentData mSegmentData;
    index_base::SegmentInfoPtr mCurrentSegmentInfo;

    PKeyTablePtr mPKeyTable;
    SKeyWriterPtr mSKeyWriter;
    index::ValueWriterPtr mValueWriter;
    common::AttributeConvertorPtr mAttrConvertor;

    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;
    uint32_t mMaxValueLen;

    double mSKeyCompressRatio;
    double mValueCompressRatio;
    double mMemoryRatio;
    index::KvDocTimestampDecider mKvDocTimestampDecider;

    document::KKVKeysExtractorPtr mKKVKeysExtractor;
    common::PlainFormatEncoderPtr mPlainFormatEncoder;
    file_system::DirectoryPtr mKKVDir;

    std::vector<int32_t> mFixValueLens;
    util::MemBuffer mMemBuffer;
    std::shared_ptr<autil::mem_pool::Pool> mPool;

    // for store pkey raw value
    bool mNeedStorePKeyValue;
    docid_t mCurrentDocId;
    HashFunctionType mKeyHasherType;
    index::AttributeWriterPtr mAttributeWriter;
    common::AttributeConvertorPtr mPkeyConvertor;
    std::map<index::keytype_t, autil::StringView> mHashKeyValueMap;

private:
    static constexpr double MAX_VALUE_MEMORY_USE_RATIO = 0.99;
    static constexpr double NEW_MEMORY_RATIO_WEIGHT = 0.7;
    static constexpr double DEFAULT_SKEY_VALUE_MEM_RATIO = 0.01;

private:
    friend class KKVSegmentWriterTest;
    IE_LOG_DECLARE();
};
IE_LOG_SETUP_TEMPLATE(partition, KKVSegmentWriter);

///////////////////////////////////////////////
template <typename SKeyType>
KKVSegmentWriter<SKeyType>::KKVSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                                             const config::IndexPartitionOptions& options, uint32_t columnIdx,
                                             uint32_t totalColumnCount)
    : SegmentWriter(schema, options, columnIdx, totalColumnCount)
    , mBuildResourceMetricsNode(NULL)
    , mMaxValueLen(0)
    , mSKeyCompressRatio(1)
    , mValueCompressRatio(1)
    , mMemoryRatio(DEFAULT_SKEY_VALUE_MEM_RATIO)
    , mNeedStorePKeyValue(false)
    , mCurrentDocId(0)
{
    mPool = std::make_shared<autil::mem_pool::Pool>();
    mKvDocTimestampDecider.Init(mOptions);
}

template <typename SKeyType>
KKVSegmentWriter<SKeyType>::~KKVSegmentWriter()
{
}

template <typename SKeyType>
inline file_system::DirectoryPtr KKVSegmentWriter<SKeyType>::CreateKKVDirectory()
{
    if (mSegmentData.GetSegmentDirName().empty()) {
        mSegmentData.SetSegmentDirName("kv_segment");
    }
    mSegmentData.PrepareDirectory();
    file_system::DirectoryPtr segmentDir = mSegmentData.GetDirectory();
    file_system::DirectoryPtr indexDir = segmentDir->MakeDirectory(index::INDEX_DIR_NAME);
    return indexDir->MakeDirectory(mKKVConfig->GetIndexName());
}

template <typename SKeyType>
inline void KKVSegmentWriter<SKeyType>::Init(index_base::BuildingSegmentData& segmentData,
                                             const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                             const util::QuotaControlPtr& buildMemoryQuotaControler,
                                             const util::BuildResourceMetricsPtr& buildResourceMetrics)
{
    DoInit();
    mSegmentMetrics = metrics;
    if (!buildResourceMetrics) {
        mBuildResourceMetrics.reset(new util::BuildResourceMetrics());
        mBuildResourceMetrics->Init();
    } else {
        mBuildResourceMetrics = buildResourceMetrics;
    }
    mBuildResourceMetricsNode = mBuildResourceMetrics->AllocateNode();

    config::PackAttributeConfigPtr packAttrConfig = mKKVConfig->GetValueConfig()->CreatePackAttributeConfig();
    assert(packAttrConfig);
    mAttrConvertor.reset(common::AttributeConvertorFactory::GetInstance()->CreatePackAttrConvertor(packAttrConfig));

    mFixValueLens.clear();
    mFixValueLens.resize(mSchema->GetRegionCount());
    for (regionid_t id = 0; id < (regionid_t)mSchema->GetRegionCount(); id++) {
        const auto& indexSchema = mSchema->GetIndexSchema(id);
        auto kkvConfig = DYNAMIC_POINTER_CAST(config::KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        assert(kkvConfig);
        mFixValueLens[id] = kkvConfig->GetValueConfig()->GetFixedLength();
    }

    mSegmentData = segmentData;
    mCurrentSegmentInfo.reset(new index_base::SegmentInfo(*segmentData.GetSegmentInfo()));

    index::PrefixKeyResourceAssigner assigner(mKKVConfig, GetColumnIdx(), GetTotalColumnCount());
    assigner.Init(buildMemoryQuotaControler, mOptions);
    size_t distPkeyCount = assigner.EstimatePkeyCount(metrics);
    int64_t totalMemory = mOptions.GetBuildConfig().buildTotalMemory * 1024 * 1024;
    IE_LOG(INFO, "estimate pkey count is %lu", distPkeyCount);
    double segInitMemUseRatio = 1.0;
    if (mSegmentMetrics->Get(SEGMENT_MEM_CONTROL_GROUP, SEGMENT_INIT_MEM_USE_RATIO, segInitMemUseRatio)) {
        distPkeyCount = distPkeyCount * segInitMemUseRatio;
        totalMemory = totalMemory * segInitMemUseRatio;
        if (distPkeyCount == 0) {
            IE_LOG(ERROR, "not support distPkeyCount = 0, will use 1024 instead!");
            distPkeyCount = 1024;
        }
        IE_LOG(INFO, "set distinct pkey count [%lu] when segment init memory ratio is [%lf]", distPkeyCount,
               segInitMemUseRatio);

        if (totalMemory == 0) {
            IE_LOG(ERROR, "not support totalMemory = 0, will use 10 MB instead!");
            ERROR_COLLECTOR_LOG(ERROR, "not support totalMemory = 0, will use 10 MB instead!");
            totalMemory = 10 * 1024 * 1024;
        }
        IE_LOG(INFO, "set build total memory [%ld] when segment init memory ratio is [%lf]", totalMemory,
               segInitMemUseRatio);
    }

    const std::string& groupName = SegmentWriter::GetMetricsGroupName(GetColumnIdx());
    const std::shared_ptr<framework::SegmentGroupMetrics>& groupMetrics =
        mSegmentMetrics->GetSegmentGroupMetrics(groupName);
    mMemoryRatio = CalculateMemoryRatio(groupMetrics, totalMemory);

    int64_t skeyMemUse = 0;
    int64_t valueMemUse = 0;
    if (mKKVConfig->GetUseSwapMmapFile()) {
        skeyMemUse = totalMemory;
        valueMemUse = (int64_t)(totalMemory / mMemoryRatio) - totalMemory;
    } else {
        skeyMemUse = (int64_t)(totalMemory * mMemoryRatio);
        valueMemUse = totalMemory - skeyMemUse;
    }

    mPKeyTable.reset(index::PrefixKeyTableCreator<index::SKeyListInfo>::Create(
        mSegmentData.GetDirectory(), mKKVConfig->GetIndexPreference(), index::PKOT_RW, distPkeyCount, mOptions));
    mSKeyWriter.reset(new SKeyWriter);
    mSKeyWriter->Init(skeyMemUse, mKKVConfig->GetSuffixKeySkipListThreshold(),
                      mKKVConfig->GetSuffixKeyProtectionThreshold());
    InitValueWriter(valueMemUse);

    const config::KKVIndexPreference& kkvIndexPreference = mKKVConfig->GetIndexPreference();
    const config::KKVIndexPreference::SuffixKeyParam& skeyParam = kkvIndexPreference.GetSkeyParam();
    const config::KKVIndexPreference::ValueParam& valueParam = kkvIndexPreference.GetValueParam();
    if (skeyParam.EnableFileCompress()) {
        mSegmentMetrics->Get(index::KKV_COMPRESS_RATIO_GROUP_NAME, index::SUFFIX_KEY_FILE_NAME, mSKeyCompressRatio);
        IE_LOG(INFO, "kkv skey compress ratio : %lf", mSKeyCompressRatio);
    }

    if (valueParam.EnableFileCompress()) {
        mSegmentMetrics->Get(index::KKV_COMPRESS_RATIO_GROUP_NAME, index::KKV_VALUE_FILE_NAME, mValueCompressRatio);
        IE_LOG(INFO, "kkv value compress ratio : %lf", mValueCompressRatio);
    }

    InitStorePkey();
}

template <typename SKeyType>
inline KKVSegmentWriter<SKeyType>*
KKVSegmentWriter<SKeyType>::CloneWithNewSegmentData(index_base::BuildingSegmentData& segmentData, bool isShared) const
{
    assert(isShared);
    KKVSegmentWriter<SKeyType>* newSegmentWriter = new KKVSegmentWriter<SKeyType>(*this);
    newSegmentWriter->mSegmentData = segmentData;
    return newSegmentWriter;
}

template <typename SKeyType>
void KKVSegmentWriter<SKeyType>::InitStorePkey()
{
    bool storePKey = false;
    if (autil::EnvUtil::getEnvWithoutDefault(NEED_STORE_PKEY_VALUE, storePKey)) {
        mNeedStorePKeyValue = storePKey;
    }

    if (mNeedStorePKeyValue) {
        auto pkeyFieldName = mKKVConfig->GetKeyFieldName();
        config::AttributeConfigPtr attrConfig = mSchema->GetRegionSchema(0)->CreateAttributeConfig(pkeyFieldName);
        mAttributeWriter.reset(index::AttributeWriterFactory::GetInstance()->CreateAttributeWriter(
            attrConfig, mOptions, mBuildResourceMetrics.get()));
        common::AttributeConvertorFactory factory;
        mPkeyConvertor.reset(factory.CreateAttrConvertor(attrConfig, tt_kkv));
        mKeyHasherType = mKKVConfig->GetKeyHashFunctionType();
    }
}

template <typename SKeyType>
inline size_t KKVSegmentWriter<SKeyType>::EstimateDumpFileSize()
{
    if (mKKVConfig->GetUseSwapMmapFile()) {
        return index::KKVIndexDumper<SKeyType>::EstimateDumpFileSize(mPKeyTable, mSKeyWriter, index::ValueWriterPtr(),
                                                                     true, mSKeyCompressRatio, mValueCompressRatio);
    } else {
        return index::KKVIndexDumper<SKeyType>::EstimateDumpFileSize(mPKeyTable, mSKeyWriter, mValueWriter, true,
                                                                     mSKeyCompressRatio, mValueCompressRatio);
    }
}

template <typename SKeyType>
inline void KKVSegmentWriter<SKeyType>::UpdateBuildResourceMetrics(uint32_t valueLen)
{
    if (!mBuildResourceMetricsNode) {
        return;
    }

    if (valueLen > mMaxValueLen) {
        mMaxValueLen = valueLen;
        mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE,
                                          index::KKVIndexDumper<SKeyType>::EstimateTempDumpMem(mMaxValueLen));
    }

    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, GetTotalMemoryUse());
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, EstimateDumpFileSize());
}

template <typename SKeyType>
inline bool KKVSegmentWriter<SKeyType>::ExtractDocInfo(document::KVIndexDocument* doc, index::PKeyType& pkey,
                                                       SKeyType& skey, autil::StringView& value,
                                                       document::KKVKeysExtractor::OperationType& operationType)
{
    if (!GetKeys(doc, pkey, skey, operationType)) {
        return false;
    }

    // TODO: remove SKeyType
    if (operationType != document::KKVKeysExtractor::ADD) {
        return true;
    }

    autil::StringView fieldValue = doc->GetValue();
    common::AttrValueMeta meta = mAttrConvertor->Decode(fieldValue);
    if (mFixValueLens[doc->GetRegionId()] != -1) {
        autil::StringView tempData = meta.data;
        size_t headerSize = common::VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*tempData.data());
        value = autil::StringView(tempData.data() + headerSize, tempData.size() - headerSize);
        assert(!mPlainFormatEncoder);
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

template <typename SKeyType>
inline bool KKVSegmentWriter<SKeyType>::AddDocument(const document::DocumentPtr& document)
{
    assert(document);
    assert(mSchema->GetIndexSchema());

    size_t valueSize = 0;
    bool ret = false;
    auto kvDoc = dynamic_cast<document::KVDocument*>(document.get());
    for (auto iter = kvDoc->begin(); iter != kvDoc->end(); ++iter) {
        auto& kvIndexDoc = *iter;
        ret = BuildSingleMessage(&kvIndexDoc, &valueSize);
        if (!ret && GetStatus() == util::NO_SPACE) {
            return false;
        }
    }
    UpdateBuildResourceMetrics(valueSize);
    return ret;
}

template <typename SKeyType>
inline bool KKVSegmentWriter<SKeyType>::AddKVIndexDocument(document::KVIndexDocument* doc)
{
    size_t valueSize = 0;
    bool ret = BuildSingleMessage(doc, &valueSize);
    UpdateBuildResourceMetrics(valueSize);
    return ret;
}

template <typename SKeyType>
inline uint32_t KKVSegmentWriter<SKeyType>::GetExpireTime(document::KVIndexDocument* doc) const
{
    if (!mSchema->TTLEnabled(doc->GetRegionId()) || !mKKVConfig->StoreExpireTime()) {
        return UNINITIALIZED_EXPIRE_TIME;
    }
    auto docTTL = doc->GetTTL();
    if (docTTL == UNINITIALIZED_TTL) {
        return UNINITIALIZED_EXPIRE_TIME;
    }
    uint64_t expireTime = static_cast<uint64_t>(index::MicrosecondToSecond(doc->GetUserTimestamp())) + docTTL;
    return expireTime > index::MAX_EXPIRE_TIME ? index::MAX_EXPIRE_TIME : expireTime;
}

template <typename SKeyType>
inline bool KKVSegmentWriter<SKeyType>::BuildSingleMessage(document::KVIndexDocument* doc, size_t* valueSize)
{
    if (NeedForceDump()) {
        SetStatus(util::NO_SPACE);
        return false;
    }

    index::PKeyType pkey = 0;
    SKeyType skey = 0;
    document::KKVKeysExtractor::OperationType op = document::KKVKeysExtractor::UNKOWN;
    autil::StringView value;
    if (!ExtractDocInfo(doc, pkey, skey, value, op)) {
        return false;
    }

    uint32_t ts;
    ts = mKvDocTimestampDecider.GetTimestamp(doc);
    uint32_t expireTime = GetExpireTime(doc);
    bool ret = true;
    switch (op) {
    case document::KKVKeysExtractor::ADD:
        ret = InnerAddDocument(pkey, skey, value, ts, doc->GetRegionId(), expireTime);
        break;
    case document::KKVKeysExtractor::DELETE_PKEY:
        ret = DeletePKey(pkey, ts, doc->GetRegionId());
        break;
    case document::KKVKeysExtractor::DELETE_SKEY:
        ret = DeleteSKey(pkey, skey, ts, doc->GetRegionId());
        break;
    default:
        assert(false);
        break;
    }
    *valueSize += value.length();

    if (!ret) {
        return ret;
    }

    return StorePkeyValue(pkey, doc);
}

template <typename SKeyType>
inline bool KKVSegmentWriter<SKeyType>::StorePkeyValue(index::PKeyType pkey, document::KVIndexDocument* doc)
{
    // add pkey raw value
    if (mNeedStorePKeyValue) {
        if (mKeyHasherType != hft_int64 && mKeyHasherType != hft_uint64) {
            bool hasFormatError = false;
            auto pkeyFieldValue = doc->GetPkFieldValue();
            auto convertedValue = mPkeyConvertor->Encode(pkeyFieldValue, mPool.get(), hasFormatError);
            if (hasFormatError) {
                IE_LOG(WARN, "pkey encode failed, pkey hash [%lu]", pkey);
                return false;
            }
            mHashKeyValueMap[pkey] = convertedValue;
        }
    }
    return true;
}

template <typename SKeyType>
inline bool KKVSegmentWriter<SKeyType>::DeleteSKey(index::PKeyType pkey, SKeyType skey, uint32_t ts,
                                                   regionid_t regionId)
{
    uint32_t skeyOffset = mSKeyWriter->Append(typename index::SuffixKeyWriter<SKeyType>::SKeyNode(
        skey, index::INVALID_VALUE_OFFSET, ts, UNINITIALIZED_EXPIRE_TIME, index::INVALID_SKEY_OFFSET));
    index::SKeyListInfo* listInfo = index::PrefixKeyTableSeeker::FindForRW(mPKeyTable.get(), pkey);
    MEMORY_BARRIER();
    if (listInfo) {
        if (!CheckRegionId(*listInfo, regionId)) {
            IE_LOG(WARN, "link skeyNode key[%lu] skey[%s] to skeyWriter failed, regionId [%d/%d] mismatch!", pkey,
                   autil::StringUtil::toString(skey).c_str(), listInfo->regionId, regionId);
            ERROR_COLLECTOR_LOG(WARN,
                                "link skeyNode key[%lu] skey[%s] to skeyWriter failed, regionId [%d/%d] mismatch!",
                                pkey, autil::StringUtil::toString(skey).c_str(), listInfo->regionId, regionId);
            return false;
        }
        if (!mSKeyWriter->LinkSkeyNode(*listInfo, skeyOffset)) {
            IE_LOG(WARN, "link skeyNode key[%lu] skey[%s] to skeyWriter failed, no space!", pkey,
                   autil::StringUtil::toString(skey).c_str());
            SetStatus(util::NO_SPACE);
            return false;
        }
    } else if (!mPKeyTable->Insert(pkey, index::SKeyListInfo(skeyOffset, index::INVALID_SKEY_OFFSET, 1, regionId))) {
        IE_LOG(WARN, "Insert key[%lu] to pkeyTable failed, no space!", pkey);
        SetStatus(util::NO_SPACE);
        return false;
    }
    MEMORY_BARRIER();
    mCurrentSegmentInfo->docCount = mCurrentSegmentInfo->docCount + 1;
    SetStatus(util::OK);
    return true;
}

template <typename SKeyType>
inline bool KKVSegmentWriter<SKeyType>::DeletePKey(index::PKeyType pkey, uint32_t ts, regionid_t regionId)
{
    // delete pkey node : set skey = min
    uint32_t skeyOffset = mSKeyWriter->Append(typename index::SuffixKeyWriter<SKeyType>::SKeyNode(
        std::numeric_limits<SKeyType>::min(), index::SKEY_ALL_DELETED_OFFSET, ts, UNINITIALIZED_EXPIRE_TIME,
        index::INVALID_SKEY_OFFSET));

    index::SKeyListInfo* listInfo = index::PrefixKeyTableSeeker::FindForRW(mPKeyTable.get(), pkey);
    MEMORY_BARRIER();
    if (listInfo) {
        if (!CheckRegionId(*listInfo, regionId)) {
            IE_LOG(WARN, "link skeyNode when del pkey[%lu] to skeyWriter failed, regionId [%d/%d] mismatch!", pkey,
                   listInfo->regionId, regionId);
            ERROR_COLLECTOR_LOG(WARN,
                                "link skeyNode when del pkey[%lu] to skeyWriter failed, regionId [%d/%d] mismatch!",
                                pkey, listInfo->regionId, regionId);
            return false;
        }

        if (!mSKeyWriter->LinkSkeyNode(*listInfo, skeyOffset)) {
            IE_LOG(WARN, "link skeyNode when del pkey[%lu] to skeyWriter failed, no space!", pkey);
            SetStatus(util::NO_SPACE);
            return false;
        }
    } else if (!mPKeyTable->Insert(pkey, index::SKeyListInfo(skeyOffset, index::INVALID_SKEY_OFFSET, 1, regionId))) {
        IE_LOG(WARN, "Insert key[%lu] to pkeyTable failed, no space!", pkey);
        SetStatus(util::NO_SPACE);
        return false;
    }

    MEMORY_BARRIER();
    mCurrentSegmentInfo->docCount = mCurrentSegmentInfo->docCount + 1;
    SetStatus(util::OK);
    return true;
}

template <typename SKeyType>
inline bool KKVSegmentWriter<SKeyType>::InnerAddDocument(index::PKeyType pkey, SKeyType skey,
                                                         const autil::StringView& value, uint32_t ts,
                                                         regionid_t regionId, uint32_t expireTime)
{
    if (unlikely(value.size() >= index::MAX_KKV_VALUE_LEN)) {
        IE_LOG(WARN, "insert pkey[%lu] skey[%s] failed, value length [%lu] over limit [%lu]!", pkey,
               autil::StringUtil::toString(skey).c_str(), value.size(), index::MAX_KKV_VALUE_LEN);
        ERROR_COLLECTOR_LOG(WARN,
                            "insert pkey[%lu] skey[%s] failed, "
                            "value length [%lu] over limit [%lu]!",
                            pkey, autil::StringUtil::toString(skey).c_str(), value.size(), index::MAX_KKV_VALUE_LEN);
        return false;
    }

    if (unlikely(value.size() + mValueWriter->GetUsedBytes() > mValueWriter->GetReserveBytes())) {
        IE_LOG(WARN,
               "insert pkey[%lu] skey[%s] failed, value memory not enough, "
               "reserve[%lu], used[%lu], need[%lu]",
               pkey, autil::StringUtil::toString(skey).c_str(), mValueWriter->GetReserveBytes(),
               mValueWriter->GetUsedBytes(), value.size());
        SetStatus(util::NO_SPACE);
        return false;
    }
    uint64_t valueOffset = mValueWriter->Append(value);
    uint32_t skeyOffset = mSKeyWriter->Append(typename index::SuffixKeyWriter<SKeyType>::SKeyNode(
        skey, valueOffset, ts, expireTime, index::INVALID_SKEY_OFFSET));
    index::SKeyListInfo* listInfo = index::PrefixKeyTableSeeker::FindForRW(mPKeyTable.get(), pkey);
    MEMORY_BARRIER();
    if (listInfo) {
        if (!CheckRegionId(*listInfo, regionId)) {
            IE_LOG(WARN, "link skeyNode key[%lu] skey[%s] to skeyWriter failed, regionId [%d/%d] mismatch!", pkey,
                   autil::StringUtil::toString(skey).c_str(), listInfo->regionId, regionId);
            ERROR_COLLECTOR_LOG(WARN,
                                "link skeyNode key[%lu] skey[%s] to skeyWriter failed, regionId [%d/%d] mismatch!",
                                pkey, autil::StringUtil::toString(skey).c_str(), listInfo->regionId, regionId);
            return false;
        }

        if (!mSKeyWriter->LinkSkeyNode(*listInfo, skeyOffset)) {
            IE_LOG(WARN, "link skeyNode key[%lu] skey[%s] to skeyWriter failed, no space!", pkey,
                   autil::StringUtil::toString(skey).c_str());
            SetStatus(util::NO_SPACE);
            return false;
        }
    } else if (!mPKeyTable->Insert(pkey, index::SKeyListInfo(skeyOffset, index::INVALID_SKEY_OFFSET, 1, regionId))) {
        IE_LOG(WARN, "Insert key[%lu] to pkeyTable failed, no space!", pkey);
        SetStatus(util::NO_SPACE);
        return false;
    }
    MEMORY_BARRIER();
    mCurrentSegmentInfo->docCount = mCurrentSegmentInfo->docCount + 1;
    SetStatus(util::OK);
    return true;
}

template <typename SKeyType>
inline bool KKVSegmentWriter<SKeyType>::IsDirty() const
{
    return mCurrentSegmentInfo->docCount > 0;
}

template <typename SKeyType>
inline void
KKVSegmentWriter<SKeyType>::CollectSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics)
{
    std::string groupName = SegmentWriter::GetMetricsGroupName(GetColumnIdx());
    size_t dumpPkeyFileSize = index::PrefixKeyDumper::GetTotalDumpSize(mPKeyTable);
    size_t skeyMemUse = mSKeyWriter->GetUsedBytes();
    size_t valueMemUse = mValueWriter->GetUsedBytes();

    segmentMetrics->Set<size_t>(groupName, index::KKV_PKEY_MEM_USE, dumpPkeyFileSize);
    segmentMetrics->Set<size_t>(groupName, index::KKV_SEGMENT_MEM_USE, EstimateDumpFileSize());
    segmentMetrics->Set<double>(groupName, index::KKV_SKEY_VALUE_MEM_RATIO, mMemoryRatio);
    segmentMetrics->Set<size_t>(groupName, index::KKV_VALUE_MEM_USE, valueMemUse);
    segmentMetrics->Set<size_t>(groupName, index::KKV_SKEY_MEM_USE, skeyMemUse);

    index::KKVMerger::FillSegmentMetrics(segmentMetrics, GetColumnIdx(), mPKeyTable->Size(),
                                         mSKeyWriter->GetTotalSkeyCount(), mMaxValueLen,
                                         mSKeyWriter->GetMaxSkeyCount());
}

template <typename SKeyType>
inline void KKVSegmentWriter<SKeyType>::CollectSegmentMetrics()
{
    CollectSegmentMetrics(mSegmentMetrics);
}

template <typename SKeyType>
inline void KKVSegmentWriter<SKeyType>::EndSegment()
{
    assert(mPKeyTable);
    mCurrentSegmentInfo->docCount = mSKeyWriter->GetTotalSkeyCount();
}

template <typename SKeyType>
inline void KKVSegmentWriter<SKeyType>::CreateDumpItems(const file_system::DirectoryPtr& directory,
                                                        std::vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    std::shared_ptr<framework::SegmentMetrics> segmentMetrics(new framework::SegmentMetrics);
    CollectSegmentMetrics(segmentMetrics);
    segmentMetrics->Store(directory);
    if (!IsDirty()) {
        return;
    }

    PKeyTable::IteratorPtr pkeyIterator = mPKeyTable->CreateIterator();
    if (mOptions.IsOffline()) {
        pkeyIterator->SortByKey();
    }

    if (!mHashKeyValueMap.empty()) {
        while (pkeyIterator->IsValid()) {
            uint64_t pKeyHash = 0;
            index::SKeyListInfo listInfo;
            pkeyIterator->Get(pKeyHash, listInfo);
            pkeyIterator->MoveToNext();
            auto pkeyRawValue = mHashKeyValueMap[pKeyHash];
            mAttributeWriter->AddField(mCurrentDocId, pkeyRawValue);
            mCurrentDocId++;
        }
        pkeyIterator->Reset();
    }

    index::KKVIndexDumperBasePtr kkvIndexDumper = CreateIndexDumper(pkeyIterator);

    file_system::DirectoryPtr indexDirectory = directory->MakeDirectory(index::INDEX_DIR_NAME);
    file_system::DirectoryPtr dumpDir = indexDirectory->MakeDirectory(mKKVConfig->GetIndexName());

    dumpItems.push_back(std::make_unique<index::KKVIndexDumpItem>(dumpDir, kkvIndexDumper));
    if (mKKVConfig->GetUseSwapMmapFile() && mOptions.GetOnlineConfig().warmUpSwapFileWhenDump) {
        mValueWriter->WarmUp();
    }

    if (mNeedStorePKeyValue) {
        file_system::DirectoryPtr attributeDir = directory->MakeDirectory(index::ATTRIBUTE_DIR_NAME);
        dumpItems.push_back(std::make_unique<index::AttributeDumpItem>(attributeDir, mAttributeWriter));
    }
}

template <typename SKeyType>
inline index_base::InMemorySegmentReaderPtr KKVSegmentWriter<SKeyType>::CreateInMemSegmentReader()
{
    index_base::InMemorySegmentReaderPtr reader(new index_base::InMemorySegmentReader(mSegmentData.GetSegmentId()));
    index::BuildingKKVSegmentReader<SKeyType>* readerPtr = new index::BuildingKKVSegmentReader<SKeyType>();
    readerPtr->Init(mPKeyTable, mValueWriter, mSKeyWriter);
    index::KKVSegmentReaderPtr buildingReader(readerPtr);
    reader->Init(buildingReader, *mCurrentSegmentInfo);
    return reader;
}

template <typename SKeyType>
inline size_t KKVSegmentWriter<SKeyType>::GetTotalMemoryUse() const
{
    if (mKKVConfig->GetUseSwapMmapFile()) {
        return mPKeyTable->GetTotalMemoryUse() + mSKeyWriter->GetUsedBytes() + mMemBuffer.GetBufferSize();
    } else {
        return mPKeyTable->GetTotalMemoryUse() + mSKeyWriter->GetUsedBytes() + mValueWriter->GetUsedBytes() +
               mMemBuffer.GetBufferSize();
    }
}

template <typename SKeyType>
inline const index_base::SegmentInfoPtr& KKVSegmentWriter<SKeyType>::GetSegmentInfo() const
{
    return mCurrentSegmentInfo;
}

template <typename SKeyType>
inline InMemorySegmentModifierPtr KKVSegmentWriter<SKeyType>::GetInMemorySegmentModifier()
{
    return InMemorySegmentModifierPtr();
}

template <typename SKeyType>
inline void KKVSegmentWriter<SKeyType>::InitValueWriter(int64_t maxValueMemoryUse)
{
    if (mKKVConfig->GetUseSwapMmapFile()) {
        mKKVDir = CreateKKVDirectory();
        if (mKKVConfig->GetMaxSwapMmapFileSize() > 0) {
            maxValueMemoryUse = std::min(mKKVConfig->GetMaxSwapMmapFileSize() * 1024 * 1024, maxValueMemoryUse);
        }
    }

    mValueWriter.reset(new index::ValueWriter(mKKVConfig->GetUseSwapMmapFile()));
    size_t sliceSize = maxValueMemoryUse;
    size_t sliceCount = 1;
    int64_t maxSizePerSlice = 32 * 1024 * 1024;
    if (maxValueMemoryUse >= maxSizePerSlice) {
        sliceSize = maxSizePerSlice;
        sliceCount = (maxValueMemoryUse + maxSizePerSlice - 1) / maxSizePerSlice;
    }
    mValueWriter->Init(mKKVDir, sliceSize, sliceCount);
}

template <typename SKeyType>
inline double
KKVSegmentWriter<SKeyType>::CalculateMemoryRatio(const std::shared_ptr<framework::SegmentGroupMetrics>& groupMetrics,
                                                 int64_t totalMemory)
{
    double ratio = DEFAULT_SKEY_VALUE_MEM_RATIO;
    if (groupMetrics) {
        double prevMemoryRatio = groupMetrics->Get<double>(index::KKV_SKEY_VALUE_MEM_RATIO);
        size_t prevSkeyMemUse = groupMetrics->Get<size_t>(index::KKV_SKEY_MEM_USE);
        size_t prevValueMemUse = groupMetrics->Get<size_t>(index::KKV_VALUE_MEM_USE);
        size_t prevTotalMemUse = prevSkeyMemUse + prevValueMemUse;
        if (prevTotalMemUse > 0) {
            ratio = ((double)prevSkeyMemUse / prevTotalMemUse) * NEW_MEMORY_RATIO_WEIGHT +
                    prevMemoryRatio * (1 - NEW_MEMORY_RATIO_WEIGHT);
        }
        ratio = ratio < 0.0001 ? 0.0001 : ratio;
        ratio = ratio > 0.90 ? 0.90 : ratio;
        IE_LOG(INFO,
               "Allocate Memory by group Metrics, prevSKeyMem[%luB], prevValueMem[%luB], "
               "prevRatio[%.6f], ratio[%.6f], totalMem[%luB]",
               prevSkeyMemUse, prevValueMemUse, prevMemoryRatio, ratio, totalMemory);
    } else {
        IE_LOG(INFO, "Allocate Memory by default, ratio[%.6f], totalMem[%luB]", ratio, totalMemory);
    }
    return ratio;
}

template <typename SKeyType>
inline bool KKVSegmentWriter<SKeyType>::NeedForceDump()
{
    if (mPKeyTable && mPKeyTable->IsFull()) {
        return true;
    }
    if (mSKeyWriter && mSKeyWriter->IsFull()) {
        return true;
    }
    return false;
}
}} // namespace indexlib::partition

#endif //__INDEXLIB_KKV_SEGMENT_WRITER_H
