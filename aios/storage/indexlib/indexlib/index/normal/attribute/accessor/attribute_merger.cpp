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
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"

#include "autil/StringUtil.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeMerger);

void AttributeMerger::Init(const AttributeConfigPtr& attrConfig)
{
    mAttributeConfig = attrConfig;
    if (!mAttributeConfig->IsAttributeUpdatable()) {
        mNeedMergePatch = false;
    }
}

void AttributeMerger::BeginMerge(const SegmentDirectoryBasePtr& segDir) { mSegmentDirectory = segDir; }

AttributeConfigPtr AttributeMerger::GetAttributeConfig() const { return mAttributeConfig; }

string AttributeMerger::GetAttributePath(const string& dir) const
{
    return PathUtil::JoinPath(dir, mAttributeConfig->GetAttrName());
}

vector<ParallelMergeItem> AttributeMerger::CreateParallelMergeItems(
    const SegmentDirectoryBasePtr& segDir, const index_base::SegmentMergeInfos& inPlanSegMergeInfos,
    uint32_t instanceCount, bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const
{
    vector<ParallelMergeItem> emptyItems;
    return emptyItems;
}

void AttributeMerger::EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                       int32_t totalParallelCount,
                                       const vector<index_base::MergeTaskResourceVector>& instResourceVec)
{
    assert(false);
    INDEXLIB_FATAL_ERROR(UnSupported, "attribute not support parallel task merge!");
}
}} // namespace indexlib::index
