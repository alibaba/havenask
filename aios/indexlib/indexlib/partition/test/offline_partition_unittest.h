#ifndef __INDEXLIB_OFFLINEPARTITIONTEST_H
#define __INDEXLIB_OFFLINEPARTITIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/offline_partition_writer.h"

IE_NAMESPACE_BEGIN(partition);

class OfflinePartitionTest : public INDEXLIB_TESTBASE
{
public:
    OfflinePartitionTest();
    ~OfflinePartitionTest();

    DECLARE_CLASS_NAME(OfflinePartitionTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();
    void TestInvalidOpen();
    void TestFullIndexOpen();
    void TestIncIndexOpen();
    void TestIncOpenWithIncompatibleFormatVersion();
    void TestOpenWithInvalidPartitionMeta();

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OfflinePartitionTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionTest, TestInvalidOpen);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionTest, TestFullIndexOpen);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionTest, TestIncIndexOpen);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionTest, TestIncOpenWithIncompatibleFormatVersion);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionTest, TestOpenWithInvalidPartitionMeta);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OFFLINEPARTITIONTEST_H
