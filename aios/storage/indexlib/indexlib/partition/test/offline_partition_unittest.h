#ifndef __INDEXLIB_OFFLINEPARTITIONTEST_H
#define __INDEXLIB_OFFLINEPARTITIONTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OfflinePartitionTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
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

INSTANTIATE_TEST_CASE_P(BuildMode, OfflinePartitionTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionTest, TestInvalidOpen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionTest, TestFullIndexOpen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionTest, TestIncIndexOpen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionTest, TestIncOpenWithIncompatibleFormatVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionTest, TestOpenWithInvalidPartitionMeta);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OFFLINEPARTITIONTEST_H
