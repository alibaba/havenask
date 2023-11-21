#pragma once

#include "autil/Log.h"
#include "indexlib/index/operation_log/OperationBase.h"
#include "indexlib/index/operation_log/OperationFactory.h"
#include "indexlib/util/PoolUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class MockOperation : public OperationBase
{
public:
    MockOperation(const indexlibv2::framework::Locator::DocInfo& docInfo) : OperationBase(docInfo) {}

    OperationBase* Clone(autil::mem_pool::Pool* pool) override
    {
        MockOperation* cloneOperation = IE_POOL_COMPATIBLE_NEW_CLASS(pool, MockOperation, _docInfo);
        return cloneOperation;
    }

    MOCK_METHOD(bool, Load, (autil::mem_pool::Pool * pool, char*& cursor), (override));
    MOCK_METHOD(bool, Process,
                (const PrimaryKeyIndexReader*, OperationLogProcessor*,
                 (const std::vector<std::pair<docid_t, docid_t>>&)),
                (const, override));

    MOCK_METHOD(size_t, Serialize, (char* buffer, size_t bufferLen), (const, override));
    MOCK_METHOD(const char*, GetPkHashPointer, (), (const, override));
    MOCK_METHOD(size_t, GetMemoryUse, (), (const, override));
    MOCK_METHOD(segmentid_t, GetSegmentId, (), (const, override));
    MOCK_METHOD(size_t, GetSerializeSize, (), (const, override));
    MOCK_METHOD(DocOperateType, GetDocOperateType, (), (const, override));
};

class MockOperationFactory : public OperationFactory
{
public:
    MOCK_METHOD(bool, CreateOperation,
                (const indexlibv2::document::NormalDocument* doc, autil::mem_pool::Pool* pool,
                 OperationBase** operation),
                (override));
};

class MockOperationMaker
{
public:
    static MockOperation* MakeOperation(int64_t ts, autil::mem_pool::Pool* pool)
    {
        MockOperation* operation =
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, MockOperation, {/*hashId*/ 0, ts, /*concurrentIdx*/ 0, /*sourceIdx*/ 0});
        return operation;
    }
};

} // namespace indexlib::index
