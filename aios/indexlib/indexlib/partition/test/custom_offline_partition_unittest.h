#ifndef __INDEXLIB_CUSTOMOFFLINEPARTITIONTEST_H
#define __INDEXLIB_CUSTOMOFFLINEPARTITIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/partition/custom_offline_partition.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(partition);

class CustomOfflinePartitionTest : public INDEXLIB_TESTBASE
{
public:
    CustomOfflinePartitionTest();
    ~CustomOfflinePartitionTest();

    DECLARE_CLASS_NAME(CustomOfflinePartitionTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleOpen();
    void TestSimpleBuildPackageFile();
    void TestCustomizeDpFileAndVersionFile();
    void TestBuildFromFailOver();
    void TestInitTableResourceFailForExceedMemQuota();
    void TestOpenFailForExceedMemQuota();
    void TestDumpSegmentWhenReachQuotaOrIsFull();
    void TestDumpSegmentWhenNotDirty();
    void TestDumpSegmentWhenBuildReturnNoSpace();
    void TestContinuousDocIdWhenBuildReturnSkip();
    void TestTableResourceReInitFail(); 

private:
    std::string CreateCustomDocuments(
            test::PartitionStateMachine& psm, const std::string& rawDocStr);
    std::vector<document::DocumentPtr> CreateDocuments(
            const std::string& rawDocString) const;
    
    void CheckDeployFile(
            const file_system::DirectoryPtr& directory,
            const std::string& segDirName,
            const std::vector<std::string>& onDiskFileList,
            const std::vector<std::string>& dpMetaFileList);
    
    void CheckVersionFile(
            const file_system::DirectoryPtr& rootDirectory,
            const std::string& versionFileName,
            const std::vector<segmentid_t>& segIdList); 
private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomOfflinePartitionTest, TestSimpleOpen);
INDEXLIB_UNIT_TEST_CASE(CustomOfflinePartitionTest, TestSimpleBuildPackageFile);
INDEXLIB_UNIT_TEST_CASE(CustomOfflinePartitionTest, TestCustomizeDpFileAndVersionFile);
INDEXLIB_UNIT_TEST_CASE(CustomOfflinePartitionTest, TestBuildFromFailOver);
INDEXLIB_UNIT_TEST_CASE(CustomOfflinePartitionTest, TestInitTableResourceFailForExceedMemQuota);
INDEXLIB_UNIT_TEST_CASE(CustomOfflinePartitionTest, TestOpenFailForExceedMemQuota);
INDEXLIB_UNIT_TEST_CASE(CustomOfflinePartitionTest, TestDumpSegmentWhenReachQuotaOrIsFull);
INDEXLIB_UNIT_TEST_CASE(CustomOfflinePartitionTest, TestDumpSegmentWhenNotDirty);
INDEXLIB_UNIT_TEST_CASE(CustomOfflinePartitionTest, TestDumpSegmentWhenBuildReturnNoSpace);
INDEXLIB_UNIT_TEST_CASE(CustomOfflinePartitionTest, TestContinuousDocIdWhenBuildReturnSkip);
INDEXLIB_UNIT_TEST_CASE(CustomOfflinePartitionTest, TestTableResourceReInitFail);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_CUSTOMOFFLINEPARTITIONTEST_H
