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
#ifndef __INDEXLIB_GENERATE_JOIN_SEGMENT_EXECUTOR_H
#define __INDEXLIB_GENERATE_JOIN_SEGMENT_EXECUTOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/open_executor/executor_resource.h"
#include "indexlib/partition/open_executor/open_executor.h"

DECLARE_REFERENCE_CLASS(partition, JoinSegmentWriter);

namespace indexlib { namespace partition {

class GenerateJoinSegmentExecutor : public OpenExecutor
{
public:
    GenerateJoinSegmentExecutor(const JoinSegmentWriterPtr& joinSegWriter) : mJoinSegWriter(joinSegWriter) {}
    ~GenerateJoinSegmentExecutor() {}

    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource) override;

private:
    JoinSegmentWriterPtr mJoinSegWriter;
    index_base::PartitionDataPtr mOriginalPartitionData;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(GenerateJoinSegmentExecutor);
}} // namespace indexlib::partition

#endif //__INDEXLIB_GENERATE_JOIN_SEGMENT_EXECUTOR_H
