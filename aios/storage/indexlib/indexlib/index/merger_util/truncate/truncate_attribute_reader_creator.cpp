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
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader_creator.h"

#include "indexlib/config/attribute_schema.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, TruncateAttributeReaderCreator);

TruncateAttributeReaderCreator::TruncateAttributeReaderCreator(const SegmentDirectoryBasePtr& segmentDirectory,
                                                               const AttributeSchemaPtr& attrSchema,
                                                               const SegmentMergeInfos& segMergeInfos,
                                                               const ReclaimMapPtr& reclaimMap)
    : mSegmentDirectory(segmentDirectory)
    , mAttrSchema(attrSchema)
    , mSegMergeInfos(segMergeInfos)
    , mReclaimMap(reclaimMap)
{
}

TruncateAttributeReaderCreator::~TruncateAttributeReaderCreator() {}

TruncateAttributeReaderPtr TruncateAttributeReaderCreator::Create(const string& attrName)
{
    ScopedLock lock(mMutex);
    auto it = mTruncateAttributeReaders.find(attrName);
    if (it != mTruncateAttributeReaders.end()) {
        return it->second;
    }
    TruncateAttributeReaderPtr reader = CreateAttrReader(attrName);
    if (reader) {
        mTruncateAttributeReaders[attrName] = reader;
    }
    return reader;
}

TruncateAttributeReaderPtr TruncateAttributeReaderCreator::CreateAttrReader(const string& fieldName)
{
    assert(mSegMergeInfos.size() != 0);
    const AttributeConfigPtr& attrConfig = mAttrSchema->GetAttributeConfig(fieldName);
    if (!attrConfig) {
        return TruncateAttributeReaderPtr();
    }
    TruncateAttributeReaderPtr attrReader(new TruncateAttributeReader);
    attrReader->Init(mReclaimMap, attrConfig);
    for (size_t i = 0; i < mSegMergeInfos.size(); ++i) {
        if (mSegMergeInfos[i].segmentInfo.docCount == 0) {
            continue;
        }
        AttributeSegmentReaderPtr attrSegReader = CreateAttrSegmentReader(attrConfig, mSegMergeInfos[i]);
        attrReader->AddAttributeSegmentReader(mSegMergeInfos[i].segmentId, attrSegReader);
    }
    return attrReader;
}

AttributeSegmentReaderPtr TruncateAttributeReaderCreator::CreateAttrSegmentReader(const AttributeConfigPtr& attrConfig,
                                                                                  const SegmentMergeInfo& segMergeInfo)
{
    FieldType fieldType = attrConfig->GetFieldType();
    switch (fieldType) {
    case ft_int8:
        return CreateAttrSegmentReaderTyped<int8_t>(attrConfig, segMergeInfo);
    case ft_uint8:
        return CreateAttrSegmentReaderTyped<uint8_t>(attrConfig, segMergeInfo);
    case ft_int16:
        return CreateAttrSegmentReaderTyped<int16_t>(attrConfig, segMergeInfo);
    case ft_uint16:
        return CreateAttrSegmentReaderTyped<uint16_t>(attrConfig, segMergeInfo);
    case ft_int32:
        return CreateAttrSegmentReaderTyped<int32_t>(attrConfig, segMergeInfo);
    case ft_uint32:
        return CreateAttrSegmentReaderTyped<uint32_t>(attrConfig, segMergeInfo);
    case ft_int64:
        return CreateAttrSegmentReaderTyped<int64_t>(attrConfig, segMergeInfo);
    case ft_uint64:
        return CreateAttrSegmentReaderTyped<uint64_t>(attrConfig, segMergeInfo);
    case ft_float:
        return CreateAttrSegmentReaderTyped<float>(attrConfig, segMergeInfo);
    case ft_fp8:
        return CreateAttrSegmentReaderTyped<int8_t>(attrConfig, segMergeInfo);
    case ft_fp16:
        return CreateAttrSegmentReaderTyped<int16_t>(attrConfig, segMergeInfo);
    case ft_double:
        return CreateAttrSegmentReaderTyped<double>(attrConfig, segMergeInfo);
    case ft_date:
        return CreateAttrSegmentReaderTyped<uint32_t>(attrConfig, segMergeInfo);
    case ft_time:
        return CreateAttrSegmentReaderTyped<uint32_t>(attrConfig, segMergeInfo);
    case ft_timestamp:
        return CreateAttrSegmentReaderTyped<uint64_t>(attrConfig, segMergeInfo);
    default:
        assert(false);
    }

    return AttributeSegmentReaderPtr();
}

template <typename T>
AttributeSegmentReaderPtr
TruncateAttributeReaderCreator::CreateAttrSegmentReaderTyped(const AttributeConfigPtr& attrConfig,
                                                             const SegmentMergeInfo& segMergeInfo)
{
    SingleValueAttributeSegmentReader<T>* attrSegReader = new SingleValueAttributeSegmentReader<T>(attrConfig);
    // when open exception release it
    AttributeSegmentReaderPtr segReader(attrSegReader);
    PartitionDataPtr partitionData = mSegmentDirectory->GetPartitionData();
    SegmentData segData = partitionData->GetSegmentData(segMergeInfo.segmentId);

    // TODO: add patch iterator
    attrSegReader->Open(segData, PatchApplyOption::NoPatch(), file_system::DirectoryPtr(), true);
    return segReader;
}
} // namespace indexlib::index::legacy
