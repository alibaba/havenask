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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/patch_file_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class PatchFileFilter
{
public:
    PatchFileFilter(const index_base::PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
                    segmentid_t startLoadSegment);
    ~PatchFileFilter();

public:
    index_base::PatchInfos Filter(const index_base::PatchInfos& patchInfos);

private:
    index_base::PatchFileInfoVec FilterLoadedPatchFileInfos(const index_base::PatchFileInfoVec& patchInfosVec,
                                                            segmentid_t startLoadSegment);

private:
    index_base::PartitionDataPtr _partitionData;
    segmentid_t _startLoadSegment;
    bool _ignorePatchToOldIncSegment;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchFileFilter);
}} // namespace indexlib::index_base
