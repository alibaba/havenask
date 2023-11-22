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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/operation_queue/operation_redo_hint.h"
namespace future_lite {
class Executor;
}

namespace indexlib { namespace partition {

class OperationBase
{
public:
    enum SerializedOperationType { INVALID_SERIALIZE_OP = 0, REMOVE_OP = 1, UPDATE_FIELD_OP = 2, SUB_DOC_OP = 3 };

public:
    OperationBase(int64_t timestamp) : mTimestamp(timestamp) {}

    virtual ~OperationBase() {}

public:
    virtual bool Load(autil::mem_pool::Pool* pool, char*& cursor) = 0;
    virtual bool Process(const partition::PartitionModifierPtr& modifier, const OperationRedoHint& redoHint,
                         future_lite::Executor* executor) = 0;
    virtual OperationBase* Clone(autil::mem_pool::Pool* pool) = 0;
    virtual SerializedOperationType GetSerializedType() const { return INVALID_SERIALIZE_OP; }

    virtual DocOperateType GetDocOperateType() const { return UNKNOWN_OP; }

    int64_t GetTimestamp() const { return mTimestamp; }
    virtual size_t GetMemoryUse() const = 0;
    virtual segmentid_t GetSegmentId() const = 0;
    // virtual segmentid_t GetOriginalSegmentId() const = 0;
    virtual size_t GetSerializeSize() const = 0;
    virtual size_t Serialize(char* buffer, size_t bufferLen) const = 0;

protected:
    int64_t mTimestamp;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationBase);
}} // namespace indexlib::partition
