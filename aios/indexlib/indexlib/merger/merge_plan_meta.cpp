#include "indexlib/merger/merge_plan_meta.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, LegacyMergePlanMeta);

const string LegacyMergePlanMeta::MERGE_PLAN_META_FILE_NAME = "merge_plan_meta";

LegacyMergePlanMeta::LegacyMergePlanMeta()
    : targetSegmentId(INVALID_SEGMENTID)
{
}

LegacyMergePlanMeta::~LegacyMergePlanMeta()
{
}

void LegacyMergePlanMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("merge_plan", mergePlan);
    json.Jsonize("target_segment_id", targetSegmentId);
    json.Jsonize("target_segment_info", targetSegmentInfo);
    json.Jsonize("sub_target_segment_info", subTargetSegmentInfo);
    json.Jsonize("segment_merge_infos", inPlanSegMergeInfos);
    json.Jsonize("sub_segment_merge_infos", subInPlanSegMergeInfos);
}

void LegacyMergePlanMeta::Store(const string& rootPath) const
{
    string content = ToJsonString(*this);
    string filePath = FileSystemWrapper::JoinPath(rootPath, MERGE_PLAN_META_FILE_NAME);
    FileSystemWrapper::AtomicStore(filePath, content);
}

bool LegacyMergePlanMeta::Load(const string& rootPath)
{
    string content;
    string filePath = FileSystemWrapper::JoinPath(rootPath, MERGE_PLAN_META_FILE_NAME);
    FileSystemWrapper::AtomicLoad(filePath, content);
    FromJsonString(*this, content);
    return true;
}

IE_NAMESPACE_END(merger);

