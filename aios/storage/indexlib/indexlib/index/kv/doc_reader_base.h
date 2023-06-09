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
#ifndef __INDEXLIB_DOC_READER_BASE_H
#define __INDEXLIB_DOC_READER_BASE_H

#include "indexlib/common_define.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(common, PlainFormatEncoder);
DECLARE_REFERENCE_CLASS(common, PackAttributeFormatter);
DECLARE_REFERENCE_CLASS(index, KVTTLDecider);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeIteratorBase);

namespace indexlib { namespace index {

class DocReaderBase
{
public:
    DocReaderBase();
    virtual ~DocReaderBase();

public:
    virtual bool Init(const config::IndexPartitionSchemaPtr& schema, index_base::PartitionDataPtr partData,
                      uint32_t targetShardId, int64_t currentTs, const std::string& ttlFieldName) = 0;
    virtual bool Read(document::RawDocument* doc, uint32_t& timestamp) = 0;
    virtual bool Seek(int64_t segmentIdx, int64_t offset) = 0;
    virtual std::pair<int64_t, int64_t> GetCurrentOffset() const = 0;
    virtual bool IsEof() const = 0;

    void SetSchema(config::IndexPartitionSchemaPtr& schema)
    {
        mSchema = schema;
        mKVIndexConfig = CreateDataKVIndexConfig(mSchema);
        mPkeyAttrConfig = mSchema->GetRegionSchema(0)->CreateAttributeConfig(mKVIndexConfig->GetKeyFieldName());
        mKeyHasherType = mKVIndexConfig->GetKeyHashFunctionType();
    }
    void SetSegmentDataVec(std::vector<index_base::SegmentData>& segmentDataVec) { mSegmentDataVec = segmentDataVec; }
    void SetSegmentIds(std::vector<segmentid_t>& segmentIds) { mSegmentIds = segmentIds; }
    void SetSegmentDocCountVec(std::vector<size_t>& segmentDocCountVec) { mSegmentDocCountVec = segmentDocCountVec; }

    bool CreateSegmentAttributeIteratorVec(const index_base::PartitionDataPtr& partData);
    bool ReadPkeyValue(const size_t currentSegmentIdx, docid_t currentDocId, const keytype_t pkeyHash,
                       document::RawDocument* doc);
    bool ReadPkeyValue(const size_t currentSegmentIdx, docid_t currentDocId, const keytype_t pkeyHash,
                       std::string& pkeyValue);

protected:
    bool Init(const config::IndexPartitionSchemaPtr& schema, const index_base::PartitionDataPtr partData,
              int64_t currentTs, const std::string& ttlFieldName);
    void CreateSegmentDataVec(const index_base::PartitionDataPtr& partData, uint32_t targetShardId);

private:
    bool ReadNumberTypePKeyValue(const size_t currentSegmentIdx, docid_t currentDocId, const keytype_t pkeyHash,
                                 std::string& pkeyValue);
    bool ReadStringTypePKeyValue(const size_t currentSegmentIdx, docid_t currentDocId, const keytype_t pkeyHash,
                                 std::string& pkeyValue);

protected:
    static const size_t MAX_RESET_POOL_MEMORY_THRESHOLD = 5 * 1024 * 1024;
    static const size_t MAX_RELEASE_POOL_MEMORY_THRESHOLD = 10 * 1024 * 1024;

    autil::mem_pool::Pool mPool;
    autil::mem_pool::Pool mSessionPool;
    config::IndexPartitionSchemaPtr mSchema;
    index_base::PartitionDataPtr mPartitionData;
    int64_t mCurrentTsInSecond = 0;
    std::string mTTLFieldName;

    config::KVIndexConfigPtr mKVIndexConfig;
    HashFunctionType mKeyHasherType;
    config::AttributeConfigPtr mPkeyAttrConfig;
    config::ValueConfigPtr mValueConfig;
    KVTTLDeciderPtr mTTLDecider;
    common::PackAttributeFormatterPtr mFormatter;
    common::PlainFormatEncoderPtr mPlainFormatEncoder;
    bool mNeedStorePKeyValue = false;

    std::vector<segmentid_t> mSegmentIds;
    std::vector<size_t> mSegmentDocCountVec;
    std::vector<index_base::SegmentData> mSegmentDataVec;
    std::vector<index::AttributeReaderPtr> mAttributeSegmentReaderVec; // hold attr reader
    std::vector<std::shared_ptr<index::AttributeIteratorBase>> mAttributeSegmentIteratorVec;

    size_t mCurrentSegmentIdx = 0;
    docid_t mCurrentDocId = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocReaderBase);
}} // namespace indexlib::index

#endif //__INDEXLIB_DOC_READER_BASE_H
