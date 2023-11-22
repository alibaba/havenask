#pragma once

#include "indexlib/common_define.h"
#include "indexlib/merger/package_merge_file_system.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class PackageMergeFileSystemTest : public INDEXLIB_TESTBASE
{
public:
    PackageMergeFileSystemTest();
    ~PackageMergeFileSystemTest();

    DECLARE_CLASS_NAME(PackageMergeFileSystemTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestRecover();
    void TestRecoverWithBranchId();
    void TestCheckpointManager();
    void TestTagFunction();

    void TestRecoverPartialCommit();
    void TestRecoverTimeNotExpired();
    void TestRecoverTimeExpired();
    void TestMultiThread();
    void TestOneThreadMultipleSegment();

    // private:
    //     void PrepareBranchPath(const std::string& rootPath,
    //                            const std::string& branchName);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestRecover);
INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestRecoverWithBranchId);
INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestCheckpointManager);
INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestTagFunction);
INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestRecoverPartialCommit);
INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestRecoverTimeNotExpired);
INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestRecoverTimeExpired);
INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestMultiThread);
INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestOneThreadMultipleSegment);
}} // namespace indexlib::merger
