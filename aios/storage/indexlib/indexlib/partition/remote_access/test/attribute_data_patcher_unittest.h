#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/remote_access/attribute_data_patcher.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class AttributeDataPatcherTest : public INDEXLIB_TESTBASE
{
public:
    AttributeDataPatcherTest();
    ~AttributeDataPatcherTest();

    DECLARE_CLASS_NAME(AttributeDataPatcherTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    template <typename T>
    void InnerTest(const std::string& fieldType, bool isMultiValue, int expectedLength);
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeDataPatcherTest, TestSimpleProcess);
}} // namespace indexlib::partition
