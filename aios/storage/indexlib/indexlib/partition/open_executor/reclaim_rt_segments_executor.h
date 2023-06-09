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
#ifndef __INDEXLIB_RECLAIM_RT_SEGMENTS_EXECUTOR_H
#define __INDEXLIB_RECLAIM_RT_SEGMENTS_EXECUTOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/open_executor/open_executor.h"
#include "indexlib/partition/realtime_partition_data_reclaimer.h"

namespace indexlib { namespace partition {

class ReclaimRtSegmentsExecutor : public OpenExecutor
{
public:
    ReclaimRtSegmentsExecutor(bool needReclaimBuildingSegment = true);
    ~ReclaimRtSegmentsExecutor();

public:
    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource) override;

private:
    index_base::PartitionDataPtr mOriginalPartitionData;
    bool mNeedReclaimBuildingSegment;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReclaimRtSegmentsExecutor);
}} // namespace indexlib::partition

#endif //__INDEXLIB_RECLAIM_RT_SEGMENTS_EXECUTOR_H
