#ifndef __AITHETA_TEST_H
#define __AITHETA_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaMipsIndexTest : public AithetaTestBase {
 public:
    AithetaMipsIndexTest() = default;
    ~AithetaMipsIndexTest() = default;

 public:
    DECLARE_CLASS_NAME(AithetaMipsIndexTest);
    void TestLinear();
    void TestQP();
    void TestHC();
    void TestGraph();
 private:
    void InnerTestIndex(const util::KeyValueMap &params);
};

INDEXLIB_UNIT_TEST_CASE(AithetaMipsIndexTest, TestLinear);
INDEXLIB_UNIT_TEST_CASE(AithetaMipsIndexTest, TestQP);
INDEXLIB_UNIT_TEST_CASE(AithetaMipsIndexTest, TestHC);
INDEXLIB_UNIT_TEST_CASE(AithetaMipsIndexTest, TestGraph);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __AITHETA_TEST_H
