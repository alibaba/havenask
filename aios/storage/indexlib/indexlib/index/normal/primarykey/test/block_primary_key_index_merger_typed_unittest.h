#pragma once

#include "indexlib/index/normal/primarykey/test/primary_key_index_merger_typed_unittest.h"

namespace indexlib { namespace index {

class BlockPrimaryKeyIndexMergerTypedTest : public PrimaryKeyIndexMergerTypedTest
{
public:
    BlockPrimaryKeyIndexMergerTypedTest() {};
    virtual ~BlockPrimaryKeyIndexMergerTypedTest() {};

    DECLARE_CLASS_NAME(BlockPrimaryKeyIndexMergerTypedTest);

public:
    void TestCaseForBlockMerge();

protected:
    virtual PrimaryKeyIndexType GetPrimaryKeyIndexType() const override { return pk_block_array; }
};

INDEXLIB_UNIT_TEST_CASE(BlockPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64);
INDEXLIB_UNIT_TEST_CASE(BlockPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64WithDelete);
INDEXLIB_UNIT_TEST_CASE(BlockPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128);
INDEXLIB_UNIT_TEST_CASE(BlockPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128WithDelete);
INDEXLIB_UNIT_TEST_CASE(BlockPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64PKAttr);
INDEXLIB_UNIT_TEST_CASE(BlockPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64WithDeletePKAttr);
INDEXLIB_UNIT_TEST_CASE(BlockPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128PKAttr);
INDEXLIB_UNIT_TEST_CASE(BlockPrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128WithDeletePKAttr);
INDEXLIB_UNIT_TEST_CASE(BlockPrimaryKeyIndexMergerTypedTest, TestMergeFromIncontinuousSegments);
INDEXLIB_UNIT_TEST_CASE(BlockPrimaryKeyIndexMergerTypedTest, TestMergeToSegmentWithoutDoc);

INDEXLIB_UNIT_TEST_CASE(BlockPrimaryKeyIndexMergerTypedTest, TestCaseForBlockMerge);
}} // namespace indexlib::index
