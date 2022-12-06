#ifndef __INDEX_SEGMNET_BUILDER_TEST_H
#define __INDEX_SEGMNET_BUILDER_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class IndexSegmentBuilderTest : public AithetaTestBase {
 public:
    IndexSegmentBuilderTest() = default;
    ~IndexSegmentBuilderTest() = default;

 public:
    void TestMergeInc();
    void TestReducerMergeInc();
    void TestReducerMergeEmpty();
    void TestCreateLocalDumpPath();
    DECLARE_CLASS_NAME(IndexSegmentBuilderTest);
};

INDEXLIB_UNIT_TEST_CASE(IndexSegmentBuilderTest, TestMergeInc);
INDEXLIB_UNIT_TEST_CASE(IndexSegmentBuilderTest, TestReducerMergeInc);
INDEXLIB_UNIT_TEST_CASE(IndexSegmentBuilderTest, TestReducerMergeEmpty);
INDEXLIB_UNIT_TEST_CASE(IndexSegmentBuilderTest, TestCreateLocalDumpPath);
IE_NAMESPACE_END(aitheta_plugin);
#endif  // __INDEX_SEGMNET_BUILDER_TEST_H
