#pragma once

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/operation_factory.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class MockOperation : public OperationBase
{
public:
    MockOperation(int64_t timestamp) : OperationBase(timestamp) {}

    OperationBase* Clone(autil::mem_pool::Pool* pool) override
    {
        MockOperation* cloneOperation = IE_POOL_COMPATIBLE_NEW_CLASS(pool, MockOperation, mTimestamp);
        return cloneOperation;
    }

    MOCK_METHOD(bool, Load, (autil::mem_pool::Pool * pool, char*& cursor), (override));
    MOCK_METHOD(bool, Process,
                (const partition::PartitionModifierPtr& modifier, const OperationRedoHint& redoHint,
                 future_lite::Executor* executor),
                (override));
    MOCK_METHOD(size_t, Serialize, (char* buffer, size_t bufferLen), (const, override));
    MOCK_METHOD(size_t, GetMemoryUse, (), (const, override));
    MOCK_METHOD(segmentid_t, GetSegmentId, (), (const, override));
    MOCK_METHOD(size_t, GetSerializeSize, (), (const, override));
    MOCK_METHOD(DocOperateType, GetDocOperateType, (), (const, override));
};

class MockOperationFactory : public OperationFactory
{
public:
    MOCK_METHOD(bool, CreateOperation,
                (const document::NormalDocumentPtr& doc, autil::mem_pool::Pool* pool, OperationBase**), (override));
};

class MockOperationMaker
{
public:
    static MockOperation* MakeOperation(int64_t ts, autil::mem_pool::Pool* pool)
    {
        MockOperation* operation = IE_POOL_COMPATIBLE_NEW_CLASS(pool, MockOperation, ts);
        return operation;
    }
};
}} // namespace indexlib::partition
