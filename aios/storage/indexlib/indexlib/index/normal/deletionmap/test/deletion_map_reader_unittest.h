#ifndef __INDEXLIB_DELETIONMAPREADERTEST_H
#define __INDEXLIB_DELETIONMAPREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class DeletionMapReaderTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    DeletionMapReaderTest();
    ~DeletionMapReaderTest();

    DECLARE_CLASS_NAME(DeletionMapReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForReadAndWrite();
    void TestCaseForGetDeletedDocCount();
    void TestCaseForReaderDelete();
    void TestCaseForMultiInMemorySegments();

private:
    void InnerTestDeletionmapReadAndWrite(uint32_t segmentDocCount, const std::string& toDeleteDocs,
                                          const std::string& deletedDocs,
                                          const index_base::PartitionDataPtr& partitionData);

    void CheckDeletedDocs(const std::string& deletedDocs, const index_base::PartitionDataPtr& partitionData);

    void CheckResult(const index_base::PartitionDataPtr& partitionData, const std::string& resultStr);

private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DeletionMapReaderTest, TestCaseForReadAndWrite);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DeletionMapReaderTest, TestCaseForGetDeletedDocCount);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DeletionMapReaderTest, TestCaseForReaderDelete);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DeletionMapReaderTest, TestCaseForMultiInMemorySegments);

INSTANTIATE_TEST_CASE_P(BuildMode, DeletionMapReaderTest, testing::Values(0, 1, 2));
}} // namespace indexlib::index

#endif //__INDEXLIB_DELETIONMAPREADERTEST_H
