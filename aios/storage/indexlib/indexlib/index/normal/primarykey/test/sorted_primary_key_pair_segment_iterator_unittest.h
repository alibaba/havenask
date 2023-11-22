#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_pair_segment_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SortedPrimaryKeyPairSegmentIteratorTest : public INDEXLIB_TESTBASE
{
public:
    SortedPrimaryKeyPairSegmentIteratorTest();
    ~SortedPrimaryKeyPairSegmentIteratorTest();

    DECLARE_CLASS_NAME(SortedPrimaryKeyPairSegmentIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void InnerTestIterator(file_system::FSOpenType fsOpenType, const std::string& docStr,
                           const std::string& expectResultStr);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyPairSegmentIteratorTest, TestSimpleProcess);
}} // namespace indexlib::index
