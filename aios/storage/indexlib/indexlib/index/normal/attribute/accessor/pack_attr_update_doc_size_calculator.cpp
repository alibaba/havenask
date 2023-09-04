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
#include "indexlib/index/normal/attribute/accessor/pack_attr_update_doc_size_calculator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"
#include "indexlib/index/normal/attribute/attribute_update_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PackAttrUpdateDocSizeCalculator);

size_t PackAttrUpdateDocSizeCalculator::EstimateUpdateDocSize(const Version& lastLoadVersion) const
{
    assert(mSchema);
    if (!HasUpdatablePackAttribute(mSchema)) {
        return 0;
    }

    Version version = mPartitionData->GetVersion();
    Version diffVersion = version - lastLoadVersion;
    size_t estimateSize = DoEstimateUpdateDocSize(mSchema, mPartitionData, diffVersion);

    const IndexPartitionSchemaPtr& subSchema = mSchema->GetSubIndexPartitionSchema();
    if (subSchema) {
        estimateSize += DoEstimateUpdateDocSize(subSchema, mPartitionData->GetSubPartitionData(), diffVersion);
    }

    IE_LOG(INFO, "EstimateUpdateDocSize :%lu", estimateSize);
    return estimateSize;
}

size_t PackAttrUpdateDocSizeCalculator::EstimateUpdateDocSizeInUnobseleteRtSeg(int64_t rtReclaimTimestamp) const
{
    assert(mSchema);
    if (!HasUpdatablePackAttribute(mSchema)) {
        return 0;
    }

    Version diffVersion = GenerateDiffVersionByTimestamp(mPartitionData, rtReclaimTimestamp);

    size_t estimateSize = DoEstimateUpdateDocSize(mSchema, mPartitionData, diffVersion);

    const IndexPartitionSchemaPtr& subSchema = mSchema->GetSubIndexPartitionSchema();
    if (subSchema) {
        estimateSize += DoEstimateUpdateDocSize(subSchema, mPartitionData->GetSubPartitionData(), diffVersion);
    }

    IE_LOG(INFO, "EstimateUpdateDocSizeInUnobseleteRtSeg :%lu", estimateSize);
    return estimateSize;
}

Version PackAttrUpdateDocSizeCalculator::GenerateDiffVersionByTimestamp(const PartitionDataPtr& partitionData,
                                                                        int64_t rtReclaimTimestamp) const
{
    Version diffVersion;
    PartitionData::Iterator iter = partitionData->Begin();
    for (; iter != partitionData->End(); ++iter) {
        const SegmentData& segData = *iter;
        segmentid_t segId = segData.GetSegmentId();
        if (RealtimeSegmentDirectory::IsRtSegmentId(segId) &&
            (segData.GetSegmentInfo()->timestamp >= rtReclaimTimestamp)) {
            diffVersion.AddSegment(segId);
        }
    }
    return diffVersion;
}

size_t PackAttrUpdateDocSizeCalculator::DoEstimateUpdateDocSize(const IndexPartitionSchemaPtr& schema,
                                                                const PartitionDataPtr& partitionData,
                                                                const Version& diffVersion) const
{
    assert(schema);
    assert(partitionData);
    size_t estimateSize = 0;
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    if (!attrSchema) {
        return estimateSize;
    }

    size_t packAttrCount = attrSchema->GetPackAttributeCount();
    for (packattrid_t packId = 0; packId < packAttrCount; packId++) {
        const PackAttributeConfigPtr& packAttrConfig = attrSchema->GetPackAttributeConfig(packId);
        if (packAttrConfig->IsDisabled()) {
            continue;
        }
        if (!packAttrConfig->IsPackAttributeUpdatable()) {
            continue;
        }
        estimateSize += EsitmateOnePackAttributeUpdateDocSize(packAttrConfig, partitionData, diffVersion);
    }
    return estimateSize;
}

size_t PackAttrUpdateDocSizeCalculator::EsitmateOnePackAttributeUpdateDocSize(
    const PackAttributeConfigPtr& packAttrConfig, const PartitionDataPtr& partitionData, const Version& diffVersion)
{
    assert(packAttrConfig);
    assert(partitionData);

    SegmentUpdateMap segUpdateMap;
    ConstructUpdateMap(packAttrConfig->GetPackName(), partitionData, diffVersion, segUpdateMap);

    return EstimateUpdateDocSizeInUpdateMap(packAttrConfig, segUpdateMap, partitionData);
}

void PackAttrUpdateDocSizeCalculator::ConstructUpdateMap(const string& packAttrName,
                                                         const PartitionDataPtr& partitionData,
                                                         const Version& diffVersion, SegmentUpdateMap& segUpdateMap)
{
    assert(partitionData);
    for (size_t i = 0; i < diffVersion.GetSegmentCount(); ++i) {
        SegmentData segData = partitionData->GetSegmentData(diffVersion[i]);
        DirectoryPtr packAttrDir = segData.GetAttributeDirectory(packAttrName, false);
        if (!packAttrDir || !packAttrDir->IsExist(ATTRIBUTE_UPDATE_INFO_FILE_NAME)) {
            continue;
        }
        AttributeUpdateInfo updateInfo;
        updateInfo.Load(packAttrDir);
        AttributeUpdateInfo::Iterator iter = updateInfo.CreateIterator();
        while (iter.HasNext()) {
            SegmentUpdateInfo segUpdateInfo = iter.Next();
            size_t updateCount = segUpdateInfo.updateDocCount;
            SegmentUpdateMap::const_iterator mapIter = segUpdateMap.find(segUpdateInfo.updateSegId);
            if (mapIter != segUpdateMap.end()) {
                updateCount += mapIter->second;
            }
            segUpdateMap[segUpdateInfo.updateSegId] = updateCount;
        }
    }
}

size_t PackAttrUpdateDocSizeCalculator::EstimateUpdateDocSizeInUpdateMap(const PackAttributeConfigPtr& packAttrConfig,
                                                                         const SegmentUpdateMap& segUpdateMap,
                                                                         const PartitionDataPtr& partitionData)
{
    size_t estimateSize = 0;
    PartitionData::Iterator iter = partitionData->Begin();
    for (; iter != partitionData->End(); ++iter) {
        const SegmentData& segData = *iter;
        SegmentUpdateMap::const_iterator mapIter = segUpdateMap.find(segData.GetSegmentId());
        if (mapIter != segUpdateMap.end()) {
            estimateSize += size_t(mapIter->second * GetAverageDocSize(segData, packAttrConfig));
        }
    }
    return estimateSize;
}

double PackAttrUpdateDocSizeCalculator::GetAverageDocSize(const SegmentData& segData,
                                                          const PackAttributeConfigPtr& packAttrConfig)
{
    assert(packAttrConfig);
    assert(packAttrConfig->IsPackAttributeUpdatable());
    const string& packAttrName = packAttrConfig->GetPackName();
    DirectoryPtr packAttrDir = segData.GetAttributeDirectory(packAttrName, false);
    if (!packAttrDir) {
        return 0;
    }
    size_t itemCount = segData.GetSegmentInfo()->docCount;
    CompressTypeOption compressTypeOption = packAttrConfig->GetCompressType();
    if (compressTypeOption.HasUniqEncodeCompress()) {
        string fileContent;
        packAttrDir->Load(ATTRIBUTE_DATA_INFO_FILE_NAME, fileContent);
        AttributeDataInfo dataInfo;
        dataInfo.FromString(fileContent);
        itemCount = dataInfo.uniqItemCount;
    }
    double fileLength = double(packAttrDir->GetFileLength(ATTRIBUTE_DATA_FILE_NAME));
    return itemCount == 0 ? 0 : fileLength / itemCount;
}

bool PackAttrUpdateDocSizeCalculator::HasUpdatablePackAttribute(const IndexPartitionSchemaPtr& schema) const
{
    assert(schema);
    if (HasUpdatablePackAttrInAttrSchema(schema->GetAttributeSchema())) {
        return true;
    }
    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema && HasUpdatablePackAttrInAttrSchema(subSchema->GetAttributeSchema())) {
        return true;
    }
    return false;
}

bool PackAttrUpdateDocSizeCalculator::HasUpdatablePackAttrInAttrSchema(const AttributeSchemaPtr& attrSchema) const
{
    if (!attrSchema) {
        return false;
    }
    size_t packAttrCount = attrSchema->GetPackAttributeCount();
    for (packattrid_t packId = 0; packId < packAttrCount; ++packId) {
        const PackAttributeConfigPtr& packConfig = attrSchema->GetPackAttributeConfig(packId);
        if (packConfig->IsDisabled()) {
            continue;
        }
        if (packConfig->IsPackAttributeUpdatable()) {
            return true;
        }
    }
    return false;
}
}} // namespace indexlib::index
