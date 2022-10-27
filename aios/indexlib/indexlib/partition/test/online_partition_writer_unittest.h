#ifndef __INDEXLIB_ONLINEPARTITIONWRITERTEST_H
#define __INDEXLIB_ONLINEPARTITIONWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/online_partition_writer.h"

IE_NAMESPACE_BEGIN(partition);

class OnlinePartitionWriterTest : public INDEXLIB_TESTBASE
{
public:
    OnlinePartitionWriterTest();
    ~OnlinePartitionWriterTest();

    DECLARE_CLASS_NAME(OnlinePartitionWriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();
    void TestClose();
    void TestOpenAfterClose();

    void TestAddDocument();
    void TestUpdateDocument();
    void TestRemoveDocument();
    void TestDumpSegment();
    void TestNeedDump();

private:
    void OpenWriter(OnlinePartitionWriter& writer, 
                    const config::IndexPartitionSchemaPtr& schema, 
                    const config::IndexPartitionOptions& options,
                    const index_base::PartitionDataPtr& partitionData);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnlinePartitionWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionWriterTest, TestClose);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionWriterTest, TestOpenAfterClose);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionWriterTest, TestAddDocument);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionWriterTest, TestUpdateDocument);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionWriterTest, TestRemoveDocument);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionWriterTest, TestDumpSegment);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionWriterTest, TestNeedDump);


IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINEPARTITIONWRITERTEST_H
