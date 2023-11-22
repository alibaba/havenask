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
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/segment/offline_segment_directory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class ParallelOfflineSegmentDirectory : public OfflineSegmentDirectory
{
public:
    ParallelOfflineSegmentDirectory(bool enableRecover = true);
    ParallelOfflineSegmentDirectory(const ParallelOfflineSegmentDirectory& other);
    ~ParallelOfflineSegmentDirectory();

public:
    segmentid_t CreateNewSegmentId() override;
    segmentid_t GetNextSegmentId(segmentid_t baseSegmentId) const override;
    void CommitVersion(const config::IndexPartitionOptions& options) override;

    ParallelOfflineSegmentDirectory* Clone() override;

private:
    void UpdateCounterMap(const util::CounterMapPtr& counterMap) const override;

protected:
    void DoInit(const file_system::DirectoryPtr& directory) override;

    index_base::Version GetLatestVersion(const file_system::DirectoryPtr& directory,
                                         const index_base::Version& emptyVersion) const override;

private:
    index_base::ParallelBuildInfo mParallelBuildInfo;
    segmentid_t mStartBuildingSegId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelOfflineSegmentDirectory);
}} // namespace indexlib::index_base
