#ifndef __INDEXLIB_FIXEDVALUEATTRIBUTEMERGERTEST_H
#define __INDEXLIB_FIXEDVALUEATTRIBUTEMERGERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_merger.h"

IE_NAMESPACE_BEGIN(index);

class MockFixedValueAttributeMerger : public SingleValueAttributeMerger<uint32_t>
{
public:
    MockFixedValueAttributeMerger()
        : SingleValueAttributeMerger<uint32_t>(true)
    {}
public:
    MOCK_METHOD1(FlushCompressDataBuffer, void(OutputData&));
    MOCK_METHOD0(DumpCompressDataBuffer, void());
};

class FixedValueAttributeMergerTest : public INDEXLIB_TESTBASE {
public:
    FixedValueAttributeMergerTest();
    ~FixedValueAttributeMergerTest();
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMergeData();
    void TestFlushBuffer();
private:
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FixedValueAttributeMergerTest, TestMergeData);
INDEXLIB_UNIT_TEST_CASE(FixedValueAttributeMergerTest, TestFlushBuffer);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FIXEDVALUEATTRIBUTEMERGERTEST_H
