#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OpenReaderByLoadconfigTest : public INDEXLIB_TESTBASE
{
public:
    OpenReaderByLoadconfigTest();
    ~OpenReaderByLoadconfigTest();

    DECLARE_CLASS_NAME(OpenReaderByLoadconfigTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestOpenReaderWithEmpty();
    void TestOpenReaderWithMmap();
    void TestOpenReaderWithCache();
    void TestOpenReaderWithGlobalCache();

private:
    void CheckFileStat(file_system::IFileSystemPtr fileSystem, std::string filePath,
                       file_system::FSOpenType expectOpenType, file_system::FSFileType expectFileType);
    void DoTestReaderWithLoadConfig(config::LoadConfigList loadConfigList, file_system::FSOpenType expectOpenType,
                                    file_system::FSFileType expectFileType);

private:
    std::string mRootDir;
    test::PartitionStateMachine mPsm;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OpenReaderByLoadconfigTest, TestOpenReaderWithEmpty);
INDEXLIB_UNIT_TEST_CASE(OpenReaderByLoadconfigTest, TestOpenReaderWithMmap);
INDEXLIB_UNIT_TEST_CASE(OpenReaderByLoadconfigTest, TestOpenReaderWithCache);
INDEXLIB_UNIT_TEST_CASE(OpenReaderByLoadconfigTest, TestOpenReaderWithGlobalCache);
}} // namespace indexlib::partition
