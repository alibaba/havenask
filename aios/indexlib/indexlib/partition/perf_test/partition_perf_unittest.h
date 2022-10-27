#ifndef __INDEXLIB_PARTITIONPERFTEST_H
#define __INDEXLIB_PARTITIONPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(partition);

class PartitionPerfTest : public INDEXLIB_TESTBASE
{
public:
    PartitionPerfTest();
    ~PartitionPerfTest();

    DECLARE_CLASS_NAME(PartitionPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFindAttributePatchPerf();
    void TestFindDeletionmapFilePerf();
    void TestInitReaderPerf();
    void TestUpdatePartitionInfo();
    void TestMap();

private:
    std::string MakeOneDoc(const config::IndexPartitionSchemaPtr& schema,
                           const std::string& cmdType = "add");

private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionPerfTest, TestFindAttributePatchPerf);
INDEXLIB_UNIT_TEST_CASE(PartitionPerfTest, TestFindDeletionmapFilePerf);
INDEXLIB_UNIT_TEST_CASE(PartitionPerfTest, TestInitReaderPerf);
INDEXLIB_UNIT_TEST_CASE(PartitionPerfTest, TestUpdatePartitionInfo);
INDEXLIB_UNIT_TEST_CASE(PartitionPerfTest, TestMap);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITIONPERFTEST_H
