#ifndef __INDEXLIB_PARALLELOFFLINESEGMENTDIRECTORYTEST_H
#define __INDEXLIB_PARALLELOFFLINESEGMENTDIRECTORYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/segment/parallel_offline_segment_directory.h"

IE_NAMESPACE_BEGIN(index_base);

class ParallelOfflineSegmentDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    ParallelOfflineSegmentDirectoryTest();
    ~ParallelOfflineSegmentDirectoryTest();

    DECLARE_CLASS_NAME(ParallelOfflineSegmentDirectoryTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestParallelIncBuildFromEmptyFullIndex();
    void TestCreateNewSegmentAfterRecover();
    void TestCheckValidBaseVersion();

private:
    index_base::Version PrepareRootDir(const std::string& path,
                                  const std::string& segmentIds);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ParallelOfflineSegmentDirectoryTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ParallelOfflineSegmentDirectoryTest, TestParallelIncBuildFromEmptyFullIndex);
INDEXLIB_UNIT_TEST_CASE(ParallelOfflineSegmentDirectoryTest, TestCreateNewSegmentAfterRecover);
INDEXLIB_UNIT_TEST_CASE(ParallelOfflineSegmentDirectoryTest, TestCheckValidBaseVersion);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PARALLELOFFLINESEGMENTDIRECTORYTEST_H
