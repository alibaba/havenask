#ifndef __INDEXLIB_DATAFLUSHCONTROLLERTEST_H
#define __INDEXLIB_DATAFLUSHCONTROLLERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/data_flush_controller.h"

IE_NAMESPACE_BEGIN(storage);

class DataFlushControllerTest : public INDEXLIB_TESTBASE
{
public:
    DataFlushControllerTest();
    ~DataFlushControllerTest();

    DECLARE_CLASS_NAME(DataFlushControllerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();
    void TestReserveQuota();
    void TestWriteFslibFile();

private:
    std::string mTempFilePath;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DataFlushControllerTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(DataFlushControllerTest, TestReserveQuota);
INDEXLIB_UNIT_TEST_CASE(DataFlushControllerTest, TestWriteFslibFile);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_DATAFLUSHCONTROLLERTEST_H
