#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/util/path_util.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeMerger);

void AttributeMerger::Init(const AttributeConfigPtr& attrConfig)
{
    mAttributeConfig = attrConfig;
    if (!mAttributeConfig->IsAttributeUpdatable())
    {
        mNeedMergePatch = false;
    }
}

void AttributeMerger::BeginMerge(const SegmentDirectoryBasePtr& segDir)
{
    mSegmentDirectory = segDir;
}

AttributeConfigPtr AttributeMerger::GetAttributeConfig() const
{
    return mAttributeConfig;
}

string AttributeMerger::GetAttributePath(const string &dir) const
{
    return PathUtil::JoinPath(dir, mAttributeConfig->GetAttrName());
}

vector<ParallelMergeItem> AttributeMerger::CreateParallelMergeItems(
    const SegmentDirectoryBasePtr& segDir,
    const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
    bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const
{
    vector<ParallelMergeItem> emptyItems;
    return emptyItems;
}

void AttributeMerger::EndParallelMerge(
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
    const vector<index_base::MergeTaskResourceVector>& instResourceVec)
{
    assert(false);
    INDEXLIB_FATAL_ERROR(UnSupported, "attribute not support parallel task merge!");
}

IE_NAMESPACE_END(index);

