#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/building_segment_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class BuildingSegmentIteratorTest : public INDEXLIB_TESTBASE
{
public:
    BuildingSegmentIteratorTest();
    ~BuildingSegmentIteratorTest();

    DECLARE_CLASS_NAME(BuildingSegmentIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void CheckIterator(BuildingSegmentIterator& iter, const std::string& inMemInfoStr, docid_t baseDocId);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BuildingSegmentIteratorTest, TestSimpleProcess);
}} // namespace indexlib::index_base
