#ifndef __INDEXLIB_SORTED_PRIMARY_KEY_INDEX_MERGER_TYPED_UNITTEST_H
#define __INDEXLIB_SORTED_PRIMARY_KEY_INDEX_MERGER_TYPED_UNITTEST_H

#include "indexlib/index/normal/primarykey/test/primary_key_index_merger_typed_unittest.h"

namespace indexlib { namespace index {

class SortedPrimaryKeyIndexMergerTypedTest : public PrimaryKeyIndexMergerTypedTest
{
public:
    SortedPrimaryKeyIndexMergerTypedTest() {};
    virtual ~SortedPrimaryKeyIndexMergerTypedTest() {};

    DECLARE_CLASS_NAME(SortedPrimaryKeyIndexMergerTypedTest);

public:
    void TestCaseForSortedMerge();

protected:
    virtual PrimaryKeyIndexType GetPrimaryKeyIndexType() const override { return pk_sort_array; }
};

INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64WithDelete);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128WithDelete);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64PKAttr);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64WithDeletePKAttr);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128PKAttr);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128WithDeletePKAttr);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexMergerTypedTest, TestMergeFromIncontinuousSegments);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexMergerTypedTest, TestMergeToSegmentWithoutDoc);

INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexMergerTypedTest, TestCaseForSortedMerge);
}} // namespace indexlib::index

#endif // __INDEXLIB_SORTED_PRIMARY_KEY_INDEX_MERGER_TYPED_UNITTEST_H
