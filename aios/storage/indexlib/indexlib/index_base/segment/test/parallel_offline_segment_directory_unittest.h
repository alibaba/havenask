#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/parallel_offline_segment_directory.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

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
    void TestRecover();
    void TestParallelIncBuildFromEmptyFullIndex();
    void TestCreateNewSegmentAfterRecover();
    void TestGetNextSegmentId();
    void TestCheckValidBaseVersion();

private:
    index_base::Version PrepareRootDir(const std::string& path, const std::string& segmentIds);
    void CreateEmptyFile(const std::string& dir);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ParallelOfflineSegmentDirectoryTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ParallelOfflineSegmentDirectoryTest, TestRecover);
INDEXLIB_UNIT_TEST_CASE(ParallelOfflineSegmentDirectoryTest, TestParallelIncBuildFromEmptyFullIndex);
INDEXLIB_UNIT_TEST_CASE(ParallelOfflineSegmentDirectoryTest, TestCreateNewSegmentAfterRecover);
INDEXLIB_UNIT_TEST_CASE(ParallelOfflineSegmentDirectoryTest, TestCheckValidBaseVersion);
INDEXLIB_UNIT_TEST_CASE(ParallelOfflineSegmentDirectoryTest, TestGetNextSegmentId);
}} // namespace indexlib::index_base
