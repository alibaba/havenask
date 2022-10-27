#include "indexlib/partition/segment/compress_ratio_calculator.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/compress_file_info.h"
#include "indexlib/util/path_util.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CompressRatioCalculator);

CompressRatioCalculator::CompressRatioCalculator()
{
}

CompressRatioCalculator::~CompressRatioCalculator()
{
}

void CompressRatioCalculator::Init(
        const PartitionDataPtr& partData, const IndexPartitionSchemaPtr& schema)
{
    assert(partData);
    assert(schema);
}

double CompressRatioCalculator::GetCompressRatio(const string& fileName) const
{
    RatioMap::const_iterator iter = mRatioMap.find(fileName);
    return iter != mRatioMap.end() ? iter->second : DEFAULT_COMPRESS_RATIO;
}

double CompressRatioCalculator::GetRatio(
        const SegmentDataVector& segDataVec, const string& fileName)
{
    SegmentDataVector::const_reverse_iterator rIter = segDataVec.rbegin();
    for (; rIter != segDataVec.rend(); rIter++)
    {
        const SegmentData& segData = *rIter;
        file_system::DirectoryPtr segDirectory = segData.GetDirectory();;
        if (!segDirectory)
        {
            continue;
        }

        CompressFileInfoPtr compressFileInfo =
            segDirectory->GetCompressFileInfo(fileName);
        if (compressFileInfo && compressFileInfo->deCompressFileLen > 0)
        {
            return (double)compressFileInfo->compressFileLen /
                compressFileInfo->deCompressFileLen;
        }
    }
    return DEFAULT_COMPRESS_RATIO;
}

void CompressRatioCalculator::GetSegmentDatas(
        const PartitionDataPtr& partData, SegmentDataVector& segmentDataVec)
{
    for (index_base::PartitionData::Iterator iter = partData->Begin();
         iter != partData->End(); ++iter)
    {
        if (!(*iter).GetSegmentInfo().HasMultiSharding())
        {
            segmentDataVec.push_back(*iter);
            continue;
        }

        uint32_t segmentColumnCount = (*iter).GetSegmentInfo().shardingColumnCount;
        for (size_t inSegmentIdx = 0; inSegmentIdx < segmentColumnCount; ++inSegmentIdx)
        {
            const index_base::SegmentData& shardingSegmentData =
                (*iter).CreateShardingSegmentData(inSegmentIdx);
            segmentDataVec.push_back(shardingSegmentData);
        }
    }
}

IE_NAMESPACE_END(partition);
