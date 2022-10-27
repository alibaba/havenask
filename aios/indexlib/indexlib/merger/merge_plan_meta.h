#ifndef __INDEXLIB_MERGE_PLAN_META_H
#define __INDEXLIB_MERGE_PLAN_META_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/merger/merge_plan.h"

IE_NAMESPACE_BEGIN(merger);

// Deprecated, only for test
struct LegacyMergePlanMeta : public autil::legacy::Jsonizable
{
public:
    static const std::string MERGE_PLAN_META_FILE_NAME;
public:
    LegacyMergePlanMeta();
    ~LegacyMergePlanMeta();
public:
    void Jsonize(JsonWrapper& json) override;

public:
    // only for test
    void Store(const std::string& rootPath) const;
    bool Load(const std::string& rootPath);
public:
    MergePlan mergePlan;
    segmentid_t targetSegmentId;
    index_base::SegmentInfo targetSegmentInfo;
    index_base::SegmentInfo subTargetSegmentInfo;

    index_base::SegmentMergeInfos inPlanSegMergeInfos;
    index_base::SegmentMergeInfos subInPlanSegMergeInfos;
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_PLAN_META_H
