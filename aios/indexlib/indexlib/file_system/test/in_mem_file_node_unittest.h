#ifndef __INDEXLIB_INMEMFILENODETEST_H
#define __INDEXLIB_INMEMFILENODETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/in_mem_file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class InMemFileNodeTest : public INDEXLIB_TESTBASE
{
public:
    InMemFileNodeTest();
    ~InMemFileNodeTest();

    DECLARE_CLASS_NAME(InMemFileNodeTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestForByteSliceList();
    void TestInMemFileRead();
    void TestBigData();
    void TestClone();
    void TestReserveSize();
    
private:
    std::string mRootDir;
    util::BlockMemoryQuotaControllerPtr mMemoryController;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemFileNodeTest, TestForByteSliceList);
INDEXLIB_UNIT_TEST_CASE(InMemFileNodeTest, TestInMemFileRead);
INDEXLIB_UNIT_TEST_CASE(InMemFileNodeTest, TestBigData);
INDEXLIB_UNIT_TEST_CASE(InMemFileNodeTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(InMemFileNodeTest, TestReserveSize);


IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INMEMFILENODETEST_H
