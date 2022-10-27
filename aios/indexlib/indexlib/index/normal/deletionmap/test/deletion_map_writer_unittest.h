#ifndef __INDEXLIB_DELETIONMAPWRITERTEST_H
#define __INDEXLIB_DELETIONMAPWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"

IE_NAMESPACE_BEGIN(index);

class DeletionMapWriterTest : public INDEXLIB_TESTBASE
{
public:
    DeletionMapWriterTest();
    ~DeletionMapWriterTest();

    DECLARE_CLASS_NAME(DeletionMapWriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDeleteWithCachePool();
    void TestDeleteWithoutCachePool();
    void TestCopyOnDumpForAsyncFlushOperation();
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DeletionMapWriterTest, TestDeleteWithCachePool);
INDEXLIB_UNIT_TEST_CASE(DeletionMapWriterTest, TestDeleteWithoutCachePool);
INDEXLIB_UNIT_TEST_CASE(DeletionMapWriterTest, TestCopyOnDumpForAsyncFlushOperation);


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DELETIONMAPWRITERTEST_H
