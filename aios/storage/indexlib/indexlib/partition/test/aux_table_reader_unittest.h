#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class AuxTableReaderTest : public INDEXLIB_TESTBASE
{
public:
    AuxTableReaderTest();
    ~AuxTableReaderTest();

    DECLARE_CLASS_NAME(AuxTableReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestReadFixLengthFloatFromKVTable();
    void TestReadFixLengthFloatFromIndexTable();
    void TestReadNullValueFromIndexTable();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AuxTableReaderTest, TestReadFixLengthFloatFromKVTable);
INDEXLIB_UNIT_TEST_CASE(AuxTableReaderTest, TestReadFixLengthFloatFromIndexTable);
INDEXLIB_UNIT_TEST_CASE(AuxTableReaderTest, TestReadNullValueFromIndexTable);
}} // namespace indexlib::partition
