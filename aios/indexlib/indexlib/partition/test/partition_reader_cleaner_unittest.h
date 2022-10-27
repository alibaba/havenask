#ifndef __INDEXLIB_PARTITIONREADERCLEANERTEST_H
#define __INDEXLIB_PARTITIONREADERCLEANERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/partition_reader_cleaner.h"
#include "indexlib/partition/test/mock_index_partition_reader.h"
#include "indexlib/config/index_partition_options.h"
#include <autil/Lock.h>

IE_NAMESPACE_BEGIN(partition);

class PartitionReaderCleanerTest : public INDEXLIB_TESTBASE
{
public:
    PartitionReaderCleanerTest();
    ~PartitionReaderCleanerTest();

    DECLARE_CLASS_NAME(PartitionReaderCleanerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCleanWithUsingReader();
    void TestCleanCache();
    void TestCleanMultipleUnusedReader();

private:
    IndexPartitionReaderPtr MakeMockIndexPartitionReader(
            versionid_t versionId, const std::string& incSegmentIds,
            const std::string& rtSegmentIds, const std::string& joinSegmentIds);

private:
    file_system::DirectoryPtr mRootDir;
    file_system::IndexlibFileSystemPtr mFileSystem;
    autil::RecursiveThreadMutex mDataLock;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionReaderCleanerTest, TestCleanWithUsingReader);
INDEXLIB_UNIT_TEST_CASE(PartitionReaderCleanerTest, TestCleanCache);
INDEXLIB_UNIT_TEST_CASE(PartitionReaderCleanerTest, TestCleanMultipleUnusedReader);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITIONREADERCLEANERTEST_H
