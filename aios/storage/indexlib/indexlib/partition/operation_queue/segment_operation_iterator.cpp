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
#include "indexlib/partition/operation_queue/segment_operation_iterator.h"

#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SegmentOperationIterator);

SegmentOperationIterator::SegmentOperationIterator(const config::IndexPartitionSchemaPtr& schema,
                                                   const OperationMeta& operationMeta, segmentid_t segmentId,
                                                   size_t offset, int64_t timestamp)
    : mOpMeta(operationMeta)
    , mBeginPos(offset)
    , mTimestamp(timestamp)
    , mLastCursor(segmentId, -1)
{
    mOpFactory.Init(schema);
}
}} // namespace indexlib::partition
