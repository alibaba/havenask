#ifndef __INDEXLIB_LOCALINDEXCLEANERTEST_H
#define __INDEXLIB_LOCALINDEXCLEANERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/partition/local_index_cleaner.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace partition {

class LocalIndexCleanerTest : public INDEXLIB_TESTBASE
{
public:
    LocalIndexCleanerTest();
    ~LocalIndexCleanerTest();

    DECLARE_CLASS_NAME(LocalIndexCleanerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    // ut from on_disk_index_cleaner_unittest.cpp
    void TestXSimpleProcess();
    void TestXDeployIndexMeta();
    void TestXCleanWithNoClean();
    void TestXCleanWithKeepCount0();
    void TestXCleanWithIncontinuousVersion();
    void TestXCleanWithInvalidSegments();
    void TestXCleanWithEmptyVersion();
    void TestXCleanWithAllEmptyVersion();
    void TestXCleanWithUsingVersion();
    void TestXCleanWithUsingOldVersion();
    void TestXCleanWithRollbackVersion();

    // new ut
    void TestSimpleProcess();
    void TestNotDpVersionFile();
    void TestCleanWhenSomeVersionDeploying();
    void TestProcessSimulation();
    // void TestRollbackVersion();
    // void TestWorkAsExecuteor();

    void TestCleanUnreferencedSegments();
    void TestCleanUnreferencedLocalFiles();
    void TestFslibWrapperListDir();
    void TestCleanWithKeepFiles();

private:
    void DpToOnline(const std::vector<versionid_t> versionIds, const std::vector<segmentid_t> segmentIds);
    void CheckOnlineVersions(const std::vector<versionid_t>& versionIds, const std::vector<segmentid_t>& segmentIds,
                             const std::vector<std::string>& otherExpectFiles = {});
    void Clean(const std::map<versionid_t, index_base::Version>& versions,
               const std::vector<versionid_t>& keepVersionIds,
               const std::vector<versionid_t>& readerContainerHoldVersionIds, uint32_t keepVersionCount);
    void ResetOnlineDirectory();
    void CheckFileState(const std::set<std::string>& whiteList, const std::set<std::string>& blackList);

private:
    file_system::DirectoryPtr mPrimaryDir;
    file_system::DirectoryPtr mSecondaryDir;
    std::string mPrimaryPath;
    std::string mSecondaryPath;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestXSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestXDeployIndexMeta);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestXCleanWithNoClean);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestXCleanWithKeepCount0);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestXCleanWithIncontinuousVersion);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestXCleanWithInvalidSegments);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestXCleanWithEmptyVersion);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestXCleanWithAllEmptyVersion);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestXCleanWithUsingVersion);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestXCleanWithUsingOldVersion);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestXCleanWithRollbackVersion);

INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestNotDpVersionFile);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestCleanWhenSomeVersionDeploying);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestProcessSimulation);
// INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestRollbackVersion);
// INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestWorkAsExecuteor);

INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestCleanUnreferencedSegments);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestCleanUnreferencedLocalFiles);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestFslibWrapperListDir);
INDEXLIB_UNIT_TEST_CASE(LocalIndexCleanerTest, TestCleanWithKeepFiles);

}} // namespace indexlib::partition

#endif //__INDEXLIB_LOCALINDEXCLEANERTEST_H
