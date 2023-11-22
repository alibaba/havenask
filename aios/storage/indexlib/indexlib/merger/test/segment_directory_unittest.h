#pragma once

#include "indexlib/common_define.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class SegmentDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    SegmentDirectoryTest() {}
    ~SegmentDirectoryTest() {}

    DECLARE_CLASS_NAME(SegmentDirectoryTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForCreateSubSegmentDir();
    void TestCaseForIterator();
    void TestGetBaseDocIds();
    void TestGetBaseDocIdsForEmpty();
    void TestLoadSegmentInfo();
    void TestRemoveUselessSegment();

private:
    index_base::SegmentInfo MakeSegmentInfo(uint32_t docCount, int64_t timestamp);

private:
    std::string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(SegmentDirectoryTest, TestCaseForCreateSubSegmentDir);
INDEXLIB_UNIT_TEST_CASE(SegmentDirectoryTest, TestCaseForIterator);
INDEXLIB_UNIT_TEST_CASE(SegmentDirectoryTest, TestGetBaseDocIds);
INDEXLIB_UNIT_TEST_CASE(SegmentDirectoryTest, TestGetBaseDocIdsForEmpty);
INDEXLIB_UNIT_TEST_CASE(SegmentDirectoryTest, TestLoadSegmentInfo);
INDEXLIB_UNIT_TEST_CASE(SegmentDirectoryTest, TestRemoveUselessSegment);
}} // namespace indexlib::merger
