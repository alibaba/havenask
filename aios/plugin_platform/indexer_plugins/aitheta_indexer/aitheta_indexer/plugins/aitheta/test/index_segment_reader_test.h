#ifndef __INDEX_SEGMNET_READER_TEST_H
#define __INDEX_SEGMNET_READER_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class IndexSegmentReaderTest : public AithetaTestBase {
 public:
    IndexSegmentReaderTest() = default;
    ~IndexSegmentReaderTest() = default;

 public:
    void TestParallelSearch();
    void TestSequentialSearch();
    DECLARE_CLASS_NAME(IndexSegmentReaderTest);

};

INDEXLIB_UNIT_TEST_CASE(IndexSegmentReaderTest, TestParallelSearch);
INDEXLIB_UNIT_TEST_CASE(IndexSegmentReaderTest, TestSequentialSearch);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __INDEX_SEGMNET_READER_TEST_H
