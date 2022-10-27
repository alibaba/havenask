#ifndef __INDEXLIB_INMEMDELETIONMAPREADERTEST_H
#define __INDEXLIB_INMEMDELETIONMAPREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"

IE_NAMESPACE_BEGIN(index);

class InMemDeletionMapReaderTest : public INDEXLIB_TESTBASE
{
public:
    InMemDeletionMapReaderTest();
    ~InMemDeletionMapReaderTest();

    DECLARE_CLASS_NAME(InMemDeletionMapReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemDeletionMapReaderTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMDELETIONMAPREADERTEST_H
