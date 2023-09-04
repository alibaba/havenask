#ifndef __INDEXLIB_VALUEINLINETEST_H
#define __INDEXLIB_VALUEINLINETEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class ValueInlineTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool> // fixedValueLen
{
public:
    ValueInlineTest();
    ~ValueInlineTest();

    DECLARE_CLASS_NAME(ValueInlineTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestSimpleProcessWithTs();
    void TestFullIncRt();

    void TestMultiChunk();

private:
    config::IndexPartitionSchemaPtr mSchema;
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

#endif //__INDEXLIB_VALUEINLINETEST_H
