#ifndef __AITHETA_TEST_H
#define __AITHETA_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaIndexerTest2 : public AithetaTestBase {
 public:
    AithetaIndexerTest2() = default;
    ~AithetaIndexerTest2() = default;

 public:
    void TestIncBuild();
    void TestBuildWithUnexpectedField();
    DECLARE_CLASS_NAME(AithetaIndexerTest2);
};

INDEXLIB_UNIT_TEST_CASE(AithetaIndexerTest2, TestIncBuild);
INDEXLIB_UNIT_TEST_CASE(AithetaIndexerTest2, TestBuildWithUnexpectedField);
IE_NAMESPACE_END(aitheta_plugin);
#endif  // __AITHETA_TEST_H
