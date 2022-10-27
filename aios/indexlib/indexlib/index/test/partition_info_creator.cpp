#include "indexlib/index/test/partition_info_creator.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PartitionInfoCreator);

PartitionInfo PartitionInfoCreator::CreatePartitionInfo(versionid_t versionId, 
                                                     const string& incSegmentIds, const string& rtSegmentIds,
                                                     PartitionMetaPtr partMeta, 
                                                     std::vector<InMemorySegmentPtr> dumpingSegments)
{
    Version version = VersionMaker::Make(versionId, incSegmentIds, rtSegmentIds);

    vector<docid_t> docCountVec;
    ExtractDocCount(incSegmentIds, docCountVec);
    ExtractDocCount(rtSegmentIds, docCountVec);
    assert(docCountVec.size() == version.GetSegmentCount());
    int64_t totalDocCount = 0;
    SegmentDataVector segmentDatas;
    for (size_t i = 0; i < version.GetSegmentCount(); ++i)
    {
        SegmentData segData;
        segData.SetSegmentId(version[i]);
        segData.SetBaseDocId(totalDocCount);
        SegmentInfo segInfo;
        segInfo.docCount = docCountVec[i];
        segData.SetSegmentInfo(segInfo);
        segmentDatas.push_back(segData);
        totalDocCount += segInfo.docCount;
    }

    for (size_t i = 0; i < dumpingSegments.size(); i++)
    {
        totalDocCount += dumpingSegments[i]->GetSegmentInfo()->docCount;
    }
    PartitionInfo partInfo;
    PartitionMetaPtr partitionMeta = partMeta;
    if (!partitionMeta)
    {
        partitionMeta.reset(new PartitionMeta);
    }
    
    DeletionMapReaderPtr delReader(new DeletionMapReader(totalDocCount));
    partInfo.Init(version, partitionMeta, segmentDatas, dumpingSegments, delReader);
    return partInfo;
}

PartitionInfoPtr PartitionInfoCreator::CreatePartitionInfo(const string& incDocCounts,
        const string& rtDocCounts)
{
    mProvider.reset(new SingleFieldPartitionDataProvider);
    mProvider->Init(mRootDir, "int32", SFP_ATTRIBUTE);
    mProvider->Build(ExtractDocString(incDocCounts), SFP_OFFLINE);
    mProvider->Build(ExtractDocString(rtDocCounts), SFP_REALTIME);
    PartitionDataPtr partData = mProvider->GetPartitionData();
    return partData->GetPartitionInfo();
}

string PartitionInfoCreator::ExtractDocString(const string& docCountsStr)
{
    if (docCountsStr.empty())
    {
        return "";
    }
    vector<size_t> docCounts;
    StringUtil::fromString(docCountsStr, docCounts, ",");
    string docString = "";
    for (size_t i = 0; i < docCounts.size(); ++i)
    {
        for (size_t j = 0; j < docCounts[i]; ++j)
        {
            docString += "0,";
        }
        docString[docString.size() - 1] = '#';
    }
    docString.erase(docString.end() - 1);
    return docString;
}

void PartitionInfoCreator::ExtractDocCount(const string& segmentInfos, 
                                        vector<docid_t> &docCount)
{
    vector<string> segInfoStrs = StringUtil::split(segmentInfos, ",");
    for (size_t i = 0; i < segInfoStrs.size(); ++i)
    {
        vector<int> segInfo;
        StringUtil::fromString(segInfoStrs[i], segInfo, ":");
        if (segInfo.size() == 1)
        {
            docCount.push_back(1);
        }
        else if (segInfo.size() == 2)
        {
            docCount.push_back(segInfo[1]);
        }
        else
        {
            assert(false);
        }
    }
}

IE_NAMESPACE_END(index);

