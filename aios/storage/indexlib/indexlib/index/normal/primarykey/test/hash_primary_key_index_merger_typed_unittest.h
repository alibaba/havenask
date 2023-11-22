#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/test/primary_key_index_merger_typed_unittest.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class HashPrimaryKeyIndexMergerTypedTest : public PrimaryKeyIndexMergerTypedTest
{
public:
    HashPrimaryKeyIndexMergerTypedTest();
    ~HashPrimaryKeyIndexMergerTypedTest();

    DECLARE_CLASS_NAME(HashPrimaryKeyIndexMergerTypedTest);

protected:
    PrimaryKeyIndexType GetPrimaryKeyIndexType() const override { return pk_hash_table; }
};

INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64WithDelete);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128WithDelete);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64PKAttr);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64WithDeletePKAttr);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128PKAttr);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128WithDeletePKAttr);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyIndexMergerTypedTest, TestMergeFromIncontinuousSegments);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyIndexMergerTypedTest, TestMergeToSegmentWithoutDoc);
}} // namespace indexlib::index
