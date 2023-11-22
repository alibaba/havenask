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
#pragma once

#include <string>

#include "autil/legacy/legacy_jsonizable_dec.h"
#include "indexlib/base/Types.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

// Deprecated, only for test
struct LegacyMergePlanMeta : public autil::legacy::Jsonizable {
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
}} // namespace indexlib::merger
