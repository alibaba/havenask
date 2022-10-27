#ifndef __INDEXLIB_AUXTABLEREADERTEST_H
#define __INDEXLIB_AUXTABLEREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(partition);

class AuxTableReaderTest : public INDEXLIB_TESTBASE
{
public:
    AuxTableReaderTest();
    ~AuxTableReaderTest();

    DECLARE_CLASS_NAME(AuxTableReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestReadFixLengthFloatFromIndexTable();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AuxTableReaderTest, TestReadFixLengthFloatFromIndexTable);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_AUXTABLEREADERTEST_H
