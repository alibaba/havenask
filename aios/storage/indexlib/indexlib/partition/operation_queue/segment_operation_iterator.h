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
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/operation_cursor.h"
#include "indexlib/partition/operation_queue/operation_factory.h"
#include "indexlib/partition/operation_queue/operation_meta.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace partition {

class SegmentOperationIterator
{
public:
    SegmentOperationIterator(const config::IndexPartitionSchemaPtr& schema, const OperationMeta& operationMeta,
                             segmentid_t segmentId, size_t offset, int64_t timestamp);

    virtual ~SegmentOperationIterator() {}

public:
    bool HasNext() const { return mLastCursor.pos < int32_t(mOpMeta.GetOperationCount()) - 1; }

    virtual OperationBase* Next()
    {
        assert(false);
        return NULL;
    }
    segmentid_t GetSegmentId() { return mLastCursor.segId; }
    OperationCursor GetLastCursor() const { return mLastCursor; }

protected:
    OperationMeta mOpMeta;
    size_t mBeginPos; // points to the first operation to be read
    int64_t mTimestamp;
    OperationCursor mLastCursor;
    OperationFactory mOpFactory;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentOperationIterator);
}} // namespace indexlib::partition
