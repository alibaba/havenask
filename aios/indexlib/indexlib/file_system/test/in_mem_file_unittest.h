#ifndef __INDEXLIB_INMEMFILETEST_H
#define __INDEXLIB_INMEMFILETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/in_mem_file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class InMemFileTest : public INDEXLIB_TESTBASE
{
public:
    InMemFileTest();
    ~InMemFileTest();

    DECLARE_CLASS_NAME(InMemFileTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestReadOutOfRange();
    void TestReservedSizeZero();

private:
    std::string mRootDir;
    util::BlockMemoryQuotaControllerPtr mMemoryController;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemFileTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(InMemFileTest, TestReadOutOfRange);
INDEXLIB_UNIT_TEST_CASE(InMemFileTest, TestReservedSizeZero);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INMEMFILETEST_H
