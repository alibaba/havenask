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
#include "indexlib/merger/merge_plan_meta.h"

#include <iosfwd>
#include <vector>

#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::file_system;

using FSEC = indexlib::file_system::ErrorCode;

namespace indexlib { namespace merger {

IE_LOG_SETUP(merger, LegacyMergePlanMeta);

const string LegacyMergePlanMeta::MERGE_PLAN_META_FILE_NAME = "merge_plan_meta";

LegacyMergePlanMeta::LegacyMergePlanMeta() : targetSegmentId(INVALID_SEGMENTID) {}

LegacyMergePlanMeta::~LegacyMergePlanMeta() {}

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
    // legacy no fence
    string content = ToJsonString(*this);
    string filePath = util::PathUtil::JoinPath(rootPath, MERGE_PLAN_META_FILE_NAME);
    auto ec = FslibWrapper::AtomicStore(filePath, content).Code();
    THROW_IF_FS_ERROR(ec, "atomic store failed path[%s]", filePath.c_str());
}

bool LegacyMergePlanMeta::Load(const string& rootPath)
{
    string content;
    string filePath = util::PathUtil::JoinPath(rootPath, MERGE_PLAN_META_FILE_NAME);
    auto ec = FslibWrapper::AtomicLoad(filePath, content).Code();
    THROW_IF_FS_ERROR(ec, "atomic load failed, path[%s]", filePath.c_str());
    FromJsonString(*this, content);
    return true;
}
}} // namespace indexlib::merger
