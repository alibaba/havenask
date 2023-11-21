#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class SecondaryPathTest : public INDEXLIB_TESTBASE
{
public:
    SecondaryPathTest();
    ~SecondaryPathTest();

    DECLARE_CLASS_NAME(SecondaryPathTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess1();
    void TestSimpleProcess2();
    void TestSimpleProcess3();
    void TestSimpleProcess4();

private:
    void DoTestSimpleProcess(bool enablePackageFile, bool needReadRemoteIndex, int64_t checkSecondIndexIntervalInMin,
                             double expectedValue, int64_t subscibeDeployMetaIntervalInMin, bool expectSubscribeMode);

private:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SecondaryPathTest, TestSimpleProcess1);
INDEXLIB_UNIT_TEST_CASE(SecondaryPathTest, TestSimpleProcess2);
INDEXLIB_UNIT_TEST_CASE(SecondaryPathTest, TestSimpleProcess3);
INDEXLIB_UNIT_TEST_CASE(SecondaryPathTest, TestSimpleProcess4);
}} // namespace indexlib::partition
