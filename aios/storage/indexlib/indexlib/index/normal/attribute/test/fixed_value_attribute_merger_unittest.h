#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_merger.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class MockFixedValueAttributeMerger : public SingleValueAttributeMerger<uint32_t>
{
public:
    MockFixedValueAttributeMerger() : SingleValueAttributeMerger<uint32_t>(true) {}

public:
    MOCK_METHOD(void, FlushCompressDataBuffer, (OutputData&), (override));
    MOCK_METHOD(void, DumpCompressDataBuffer, (), (override));
};

class FixedValueAttributeMergerTest : public INDEXLIB_TESTBASE
{
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
}} // namespace indexlib::index
