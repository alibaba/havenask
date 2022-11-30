#ifndef __RAW_SEGMNET_TEST_H
#define __RAW_SEGMNET_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class RawSegmentTest : public AithetaTestBase {
 public:
    RawSegmentTest() = default;
    ~RawSegmentTest() = default;

 public:
    DECLARE_CLASS_NAME(RawSegmentTest);
    void Test();
};

INDEXLIB_UNIT_TEST_CASE(RawSegmentTest, Test);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __RAW_SEGMNET_TEST_H
