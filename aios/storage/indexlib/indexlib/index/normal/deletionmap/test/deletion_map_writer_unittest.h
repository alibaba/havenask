#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

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
}} // namespace indexlib::index
