#ifndef __INDEXLIB_PARALLELPARTITIONDATAMERGERTEST_H
#define __INDEXLIB_PARALLELPARTITIONDATAMERGERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/parallel_partition_data_merger.h"

IE_NAMESPACE_BEGIN(merger);

class ParallelPartitionDataMergerTest : public INDEXLIB_TESTBASE
{
public:
    ParallelPartitionDataMergerTest();
    ~ParallelPartitionDataMergerTest();

    DECLARE_CLASS_NAME(ParallelPartitionDataMergerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestLocatorAndTimestamp();
    void TestNoNeedMerge();
    void TestMergeEmptyInstanceDir();
    
private:
    void SetLastSegmentInfo(const std::string& rootPath, int src, int offset,
                            int64_t ts, int64_t maxTTL);
    int64_t GetLastSegmentLocator(const std::string& rootPath);

    // info : segId,segId,segId;....
    std::vector<std::pair<index_base::Version, size_t> > MakeVersionPair(
            const std::string& versionInfos, versionid_t baseVersion);
private:
    config::IndexPartitionSchemaPtr mSchema;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestLocatorAndTimestamp);
INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestNoNeedMerge);
INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestMergeEmptyInstanceDir);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_PARALLELPARTITIONDATAMERGERTEST_H
