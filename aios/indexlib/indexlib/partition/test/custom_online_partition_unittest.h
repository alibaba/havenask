#ifndef __INDEXLIB_CUSTOMONLINEPARTITIONTEST_H
#define __INDEXLIB_CUSTOMONLINEPARTITIONTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/custom_online_partition.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(document, Document);

IE_NAMESPACE_BEGIN(partition);

class CustomOnlinePartitionTest : public INDEXLIB_TESTBASE
{
public:
    CustomOnlinePartitionTest();
    ~CustomOnlinePartitionTest();

    DECLARE_CLASS_NAME(CustomOnlinePartitionTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleOpen();
    void TestSimpleReopen();
    void TestMemoryControl();
    void TestCheckMemoryStatus();
    void TestDropRtDocWhenTsEarlyThanIncVersionTs();
    void TestOnlineKeepVersionCount();
    void TestEstimateDiffVersionLockSize();
    void TestAsyncDumpOnline();
    void TestBuildingSegmentReaderWillBeReleasedInAsyncDump();
    void TestAsynDumpWithReopen();
    
private:
    void PrepareBuildData(
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const std::string& docString,
        const std::string& rootDir);

    std::vector<document::DocumentPtr> CreateDocuments(const std::string& rawDocString) const;

private:
    std::string mRootDir;
    std::string mPluginRoot;    
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOfflineOptions;
    config::IndexPartitionOptions mOnlineOptions;
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestSimpleOpen);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestSimpleReopen);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestMemoryControl);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestCheckMemoryStatus);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestDropRtDocWhenTsEarlyThanIncVersionTs);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestOnlineKeepVersionCount);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestEstimateDiffVersionLockSize);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestAsyncDumpOnline);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestBuildingSegmentReaderWillBeReleasedInAsyncDump);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestAsynDumpWithReopen);


IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_CUSTOMONLINEPARTITIONTEST_H
