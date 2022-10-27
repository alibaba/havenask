#include "indexlib/merger/merge_plan.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_writer.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(merger);

const string MergePlan::MERGE_PLAN_FILE_NAME = "merge_plan";
const string MergePlan::LEGACY_MERGE_PLAN_META_FILE_NAME = "merge_plan_meta";

MergePlan::MergePlan()
{
    AddTargetSegment();
    mTargetSegmentDescs[0].segmentInfo.mergedSegment = true;
    mTargetSegmentDescs[0].segmentId = INVALID_SEGMENTID;    
}

MergePlan::~MergePlan() 
{
}

void MergePlan::SegmentDesc::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("segment_id", segmentId);
    json.Jsonize("topology_info", topologyInfo);
    json.Jsonize("segment_info", segmentInfo);
    json.Jsonize("sub_segment_info", subSegmentInfo);
    json.Jsonize("segment_customize_metrics", segmentCustomizeMetrics);
}

void MergePlan::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("merge_segment_id", mSegments);
    json.Jsonize("segment_merge_infos", mSegmentMergeInfos);
    json.Jsonize("sub_segment_merge_infos", mSubSegmentMergeInfos);

    if (json.GetMode() == FROM_JSON)
    {
        auto jsonMap = json.GetMap();
        auto iter = jsonMap.find("target_segment_id");
        if (iter == jsonMap.end())
        {
            json.Jsonize("target_segment_descs", mTargetSegmentDescs);
        }
        else
        {
            mTargetSegmentDescs.clear();
            AddTargetSegment();
            json.Jsonize("target_segment_id", mTargetSegmentDescs[0].segmentId);
            json.Jsonize("target_topology_info", mTargetSegmentDescs[0].topologyInfo);
            json.Jsonize("target_segment_info", mTargetSegmentDescs[0].segmentInfo);
        }
    }
    else
    {
        json.Jsonize("target_segment_descs", mTargetSegmentDescs);
    }
}

void MergePlan::AddSegment(const SegmentMergeInfo& segMergeInfo)
{
    mSegments.insert(segMergeInfo.segmentId);
    
    mSegmentMergeInfos.push_back(segMergeInfo);
    for (size_t i = mSegmentMergeInfos.size() - 1; i != 0; --i)
    {
        if (segMergeInfo < mSegmentMergeInfos[i - 1])
        {
            mSegmentMergeInfos[i] = mSegmentMergeInfos[i - 1];
            mSegmentMergeInfos[i - 1] = segMergeInfo;
        }
        else
        {
            break;
        }
    }
    mTargetSegmentDescs[0].segmentInfo.maxTTL = max(mTargetSegmentDescs[0].segmentInfo.maxTTL,
                                                    segMergeInfo.segmentInfo.maxTTL);
    mTargetSegmentDescs[0].segmentInfo.shardingColumnCount =
        segMergeInfo.segmentInfo.shardingColumnCount;
    
    const SegmentInfo& lastSegInfo = mSegmentMergeInfos.rbegin()->segmentInfo;
    mTargetSegmentDescs[0].segmentInfo.timestamp = lastSegInfo.timestamp;
    mTargetSegmentDescs[0].segmentInfo.locator = lastSegInfo.locator;
    // estimated docCount, to be updated during end merge
    mTargetSegmentDescs[0].segmentInfo.docCount += segMergeInfo.segmentInfo.docCount;
}

void MergePlan::Store(const string& rootPath) const
{
    string content = ToJsonString(*this);
    string filePath = storage::FileSystemWrapper::JoinPath(rootPath, MERGE_PLAN_FILE_NAME);
    storage::FileSystemWrapper::AtomicStore(filePath, content);
}

void MergePlan::Store(const DirectoryPtr& directory) const
{
    string content = ToJsonString(*this);
    FileWriterPtr writer = directory->CreateFileWriter(MERGE_PLAN_FILE_NAME);
    writer->Write(content.c_str(), content.size());
    writer->Close();
}

bool MergePlan::Load(const string& rootPath, const int64_t mergeMetaBinaryVersion)
{
    string content;
    if (mergeMetaBinaryVersion <= 1)
    {
        string filePath
            = storage::FileSystemWrapper::JoinPath(rootPath, LEGACY_MERGE_PLAN_META_FILE_NAME);
        storage::FileSystemWrapper::AtomicLoad(filePath, content);
        LoadLegacyMergePlan(content);
        return true;
    }
    string filePath = storage::FileSystemWrapper::JoinPath(rootPath, MERGE_PLAN_FILE_NAME);
    storage::FileSystemWrapper::AtomicLoad(filePath, content);
    FromJsonString(*this, content);
    return true;    
}

bool MergePlan::Load(const DirectoryPtr& directory, const int64_t mergeMetaBinaryVersion)
{
    string content;
    if (mergeMetaBinaryVersion <= 1)
    {
        directory->Load(LEGACY_MERGE_PLAN_META_FILE_NAME, content);
        LoadLegacyMergePlan(content);
        return true;
    }
    directory->Load(MERGE_PLAN_FILE_NAME, content);
    FromJsonString(*this, content);
    return true;    
}

void MergePlan::LoadLegacyMergePlan(const string& content)
{
    json::JsonMap jsonMap;
    FromJsonString(jsonMap, content);
    segmentid_t targetSegmentId = INVALID_SEGMENTID;
    FromJson(targetSegmentId, jsonMap.at("target_segment_id"));
    SegmentInfo targetSegmentInfo;
    FromJson(targetSegmentInfo, jsonMap.at("target_segment_info"));
    SegmentInfo subTargetSegmentInfo;
    FromJson(subTargetSegmentInfo, jsonMap.at("sub_target_segment_info"));
    SegmentMergeInfos segMergeInfos;
    FromJson(segMergeInfos, jsonMap.at("segment_merge_infos"));
    SegmentMergeInfos subSegMergeInfos;
    FromJson(subSegMergeInfos, jsonMap.at("sub_segment_merge_infos"));

    auto legacyMergePlanAny = jsonMap.at("merge_plan");
    json::JsonMap legacyMergePlan;
    FromJson(legacyMergePlan, legacyMergePlanAny);
    SegmentSet segments;
    FromJson(segments, legacyMergePlan.at("merge_segment_id"));
    SegmentTopologyInfo topo;
    FromJson(topo, legacyMergePlan.at("target_topology_info"));
        

    ClearTargetSegments();
    AddTargetSegment();
    mSegments = std::move(segments);
    auto& targetSegDesc = mTargetSegmentDescs[0];
    targetSegDesc.segmentId = targetSegmentId;
    targetSegDesc.topologyInfo = topo;
    targetSegDesc.segmentInfo = targetSegmentInfo;
    targetSegDesc.subSegmentInfo = subTargetSegmentInfo;
    mSegmentMergeInfos = std::move(segMergeInfos);
    mSubSegmentMergeInfos = std::move(subSegMergeInfos);    
}

IE_NAMESPACE_END(merger);

