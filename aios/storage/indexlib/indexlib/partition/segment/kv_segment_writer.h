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
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_doc_timestamp_decider.h"
#include "indexlib/index/kv/kv_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyHasherTyped.h"
#include "indexlib/util/MemBuffer.h"

DECLARE_REFERENCE_CLASS(document, KVKeyExtractor);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(common, PlainFormatEncoder);
DECLARE_REFERENCE_CLASS(common, DumpItem);
DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(index, KvDocTimestampDecider);
DECLARE_REFERENCE_CLASS(index, KVWriter);

namespace indexlib { namespace partition {

class KVSegmentWriter : public index_base::SegmentWriter
{
public:
    KVSegmentWriter(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                    uint32_t columnIdx = 0, uint32_t totalColumnCount = 1)
        : SegmentWriter(schema, options, columnIdx, totalColumnCount)
        , mBuildResourceMetricsNode(NULL)
        , mNeedStorePKeyValue(false)
        , mValueFieldId(INVALID_FIELDID)
        , mEnableCompactPackAttribute(false)
    {
        mKvDocTimestampDecider.Init(mOptions);
    }
    ~KVSegmentWriter() {}

public:
    void Init(index_base::BuildingSegmentData& segmentData, const std::shared_ptr<framework::SegmentMetrics>& metrics,
              const util::QuotaControlPtr& buildMemoryQuotaControler,
              const util::BuildResourceMetricsPtr& buildResourceMetrics = util::BuildResourceMetricsPtr()) override;

    KVSegmentWriter* CloneWithNewSegmentData(index_base::BuildingSegmentData& segmentData,
                                             bool isShared) const override;
    void CollectSegmentMetrics() override { CollectSegmentMetrics(mSegmentMetrics); }
    void CollectSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics)
    {
        std::string groupName = SegmentWriter::GetMetricsGroupName(GetColumnIdx());
        mWriter->FillSegmentMetrics(segmentMetrics, groupName);
    }
    bool AddDocument(const document::DocumentPtr& doc) override;
    bool AddKVIndexDocument(document::KVIndexDocument* doc) override;
    bool IsDirty() const override;
    void EndSegment() override;
    void CreateDumpItems(const file_system::DirectoryPtr& directory,
                         std::vector<std::unique_ptr<common::DumpItem>>& dumpItems) override;
    index_base::InMemorySegmentReaderPtr CreateInMemSegmentReader() override;
    const index_base::SegmentInfoPtr& GetSegmentInfo() const override { return mCurrentSegmentInfo; }
    InMemorySegmentModifierPtr GetInMemorySegmentModifier() override
    {
        assert(false);
        return InMemorySegmentModifierPtr();
    }

    bool NeedForceDump() override { return mWriter ? mWriter->IsFull() : false; }

    size_t GetTotalMemoryUse() const override
    {
        size_t ret = mMemBuffer.GetBufferSize();
        if (mWriter) {
            ret += mWriter->GetMemoryUse();
        }
        return ret;
    }

public:
    static size_t EstimateInitMemUse(config::KVIndexConfigPtr KVConfig, uint32_t columnIdx, uint32_t totalColumnCount,
                                     const config::IndexPartitionOptions& options,
                                     const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                     const util::QuotaControlPtr& buildMemoryQuotaControler);

protected:
    virtual bool ExtractDocInfo(document::KVIndexDocument* doc, index::keytype_t& key, autil::StringView& value,
                                bool& isDeleted);
    virtual void InitConfig();

    void InitStorePkey();

    bool GetDeletedDocFlag(document::KVIndexDocument* doc, bool& isDeleted) const;

private:
    void UpdateBuildResourceMetrics();
    bool BuildSingleMessage(document::KVIndexDocument* doc);

protected:
    index::KVWriterPtr mWriter;
    config::KVIndexConfigPtr mKVConfig;
    index_base::BuildingSegmentData mSegmentData;
    index_base::SegmentInfoPtr mCurrentSegmentInfo;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;
    common::AttributeConvertorPtr mAttrConvertor;
    index::KvDocTimestampDecider mKvDocTimestampDecider;
    std::shared_ptr<common::PlainFormatEncoder> mPlainFormatEncoder;
    util::MemBuffer mMemBuffer;
    std::shared_ptr<autil::mem_pool::Pool> mPool;

    // for store pkey raw value
    bool mNeedStorePKeyValue;
    HashFunctionType mKeyHasherType;
    index::AttributeWriterPtr mAttributeWriter;
    common::AttributeConvertorPtr mPkeyConvertor;
    std::map<index::keytype_t, autil::StringView> mHashKeyValueMap;

private:
    document::KVKeyExtractorPtr mKVKeyExtractor;
    fieldid_t mValueFieldId; // invalid when only one field in value schema
    bool mEnableCompactPackAttribute;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVSegmentWriter);
}} // namespace indexlib::partition
