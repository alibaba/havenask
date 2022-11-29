#ifndef __AITHETA_TEST_H
#define __AITHETA_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaIndexerTest : public AithetaTestBase {
 public:
    AithetaIndexerTest() = default;
    ~AithetaIndexerTest() = default;

 public:
    DECLARE_CLASS_NAME(AithetaIndexerTest);
    void TestPQ();
    void TestHC();
    void TestLinear();
    void TestPQWithoutCatId();
    void TestHCWithoutCatId();
    void TestMinHeapUsedInReduce();
    void TestGraph();
    void TestGraphWithoutCatId();

 private:
    void InnerTestIndex(const util::KeyValueMap &params);
    void InnerTestIndexWithoutCatId(const util::KeyValueMap &params);
};

INDEXLIB_UNIT_TEST_CASE(AithetaIndexerTest, TestPQ);
INDEXLIB_UNIT_TEST_CASE(AithetaIndexerTest, TestHC);
INDEXLIB_UNIT_TEST_CASE(AithetaIndexerTest, TestLinear);
INDEXLIB_UNIT_TEST_CASE(AithetaIndexerTest, TestPQWithoutCatId);
INDEXLIB_UNIT_TEST_CASE(AithetaIndexerTest, TestHCWithoutCatId);
INDEXLIB_UNIT_TEST_CASE(AithetaIndexerTest, TestMinHeapUsedInReduce);
INDEXLIB_UNIT_TEST_CASE(AithetaIndexerTest, TestGraph);
INDEXLIB_UNIT_TEST_CASE(AithetaIndexerTest, TestGraphWithoutCatId);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __AITHETA_TEST_H
