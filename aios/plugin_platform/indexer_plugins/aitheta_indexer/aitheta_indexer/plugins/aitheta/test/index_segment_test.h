#ifndef __INDEX_SEGMNET_TEST_H
#define __INDEX_SEGMNET_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class IndexSegmentTest : public AithetaTestBase {
 public:
    IndexSegmentTest() = default;
    ~IndexSegmentTest() = default;

 public:
    DECLARE_CLASS_NAME(IndexSegmentTest);
    void TestMergeRawSegments();
    void TestMergeRawSegments2();
    void TestBuildPkMap4RawSegments();
};

INDEXLIB_UNIT_TEST_CASE(IndexSegmentTest, TestMergeRawSegments);
INDEXLIB_UNIT_TEST_CASE(IndexSegmentTest, TestMergeRawSegments2);
INDEXLIB_UNIT_TEST_CASE(IndexSegmentTest, TestBuildPkMap4RawSegments);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __INDEX_SEGMNET_TEST_H
