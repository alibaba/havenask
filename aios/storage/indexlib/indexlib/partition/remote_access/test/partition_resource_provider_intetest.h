#pragma once

#include <cmath>

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/partition/remote_access/partition_resource_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(partition, OnlinePartition);

namespace indexlib { namespace partition {

// first bool enable sharding index
class PartitionResourceProviderInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    PartitionResourceProviderInteTest();
    ~PartitionResourceProviderInteTest();

    DECLARE_CLASS_NAME(PartitionResourceProviderInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestPartitionIterator();
    void TestPartitionSeeker();
    void TestPartitionPatcher();
    void TestReadAndMergeWithPartitionPatchIndex();
    void TestAutoDeletePartitionPatchMeta();
    void TestValidateDeployIndexForPatchIndexData();
    void TestOnlineCleanOnDiskIndex();
    void TestPatchDataLoadConfig();
    void TestPartitionSizeCalculatorForPatch();
    void TestOnDiskSegmentSizeCalculatorForPatch();
    void TestPatchMetaFileExist();
    void TestGetSegmentSizeByDeployIndexWrapper();
    void TestPartitionPatchIndexAccessor();
    void TestDeployIndexWrapperGetDeployFiles();
    void TestCreateRollBackVersion();
    void TestBuildIndexWithSchemaIdNotZero();
    void TestAsyncWriteAttribute();

private:
    void CheckIterator(const PartitionIteratorPtr& iterator, segmentid_t segmentId, const std::string& attrName,
                       const std::string& attrValues);

    void CheckSeeker(const PartitionSeekerPtr& seeker, const std::string& key, const std::string& fieldValues);

    void PreparePatchIndex(const std::string& tableDirName = "index_table");

    // metaInfoStr: schemaId:seg0,seg1;...
    void CheckPatchMeta(const indexlib::file_system::DirectoryPtr& rootDir, versionid_t versionId,
                        const std::string& metaInfoStr);

    size_t GetMMapLockSize();

    OnlinePartitionPtr CreateOnlinePartition();

    void CheckFileExistInDeployIndexFile(const std::string& dpMetaFilePath, const std::string& filePath, bool isExist);

    void CheckFileExist(const std::string& targetFile, const std::vector<std::string>& fileLists, bool isExist);

    void CheckVersion(const indexlib::file_system::DirectoryPtr& versionDir, versionid_t vid,
                      const std::string& segIdStr, segmentid_t lastSegId);

private:
    config::IndexPartitionSchemaPtr mNewSchema;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition
