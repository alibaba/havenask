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
#ifndef __INDEXLIB_SUB_DOC_OPERATION_H
#define __INDEXLIB_SUB_DOC_OPERATION_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_base.h"

namespace indexlib { namespace partition {

class SubDocOperation : public OperationBase
{
public:
    SubDocOperation(int64_t timestamp, InvertedIndexType mainPkType, InvertedIndexType subPkType)
        : OperationBase(timestamp)
        , mMainOperation(NULL)
        , mSubOperations(NULL)
        , mSubOperationCount(0)
        , mDocOperateType(UNKNOWN_OP)
        , mMainPkType(mainPkType)
        , mSubPkType(subPkType)
    {
        assert(mainPkType == it_primarykey64 || mainPkType == it_primarykey128);
        assert(subPkType == it_primarykey64 || subPkType == it_primarykey128);
    }

    ~SubDocOperation();

public:
    void Init(DocOperateType docOperateType, OperationBase* mainOperation, OperationBase** subOperations,
              size_t subOperationCount);

    bool Load(autil::mem_pool::Pool* pool, char*& cursor) override;

    bool Process(const partition::PartitionModifierPtr& modifier, const OperationRedoHint& redoHint,
                 future_lite::Executor* mExecutor) override;

    OperationBase* Clone(autil::mem_pool::Pool* pool) override;

    SerializedOperationType GetSerializedType() const override { return SUB_DOC_OP; }

    DocOperateType GetDocOperateType() const override { return mDocOperateType; }

    size_t GetMemoryUse() const override;

    segmentid_t GetSegmentId() const override;

    size_t GetSerializeSize() const override;

    size_t Serialize(char* buffer, size_t bufferLen) const override;

private:
    OperationBase* LoadOperation(autil::mem_pool::Pool* pool, char*& cursor, DocOperateType docOpType,
                                 InvertedIndexType pkType);

private:
    OperationBase* mMainOperation;
    OperationBase** mSubOperations;
    size_t mSubOperationCount;
    DocOperateType mDocOperateType;
    InvertedIndexType mMainPkType;
    InvertedIndexType mSubPkType;

private:
    friend class SubDocOperationTest;
    friend class SubDocOperationCreatorTest;
    friend class OperationFactoryTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocOperation);
}} // namespace indexlib::partition

#endif //__INDEXLIB_SUB_DOC_OPERATION_H
