#ifndef __COMMON_DEFINE_TEST_H
#define __COMMON_DEFINE_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class CommonDefineTest : public AithetaTestBase {
 public:
    CommonDefineTest() = default;
    ~CommonDefineTest() = default;

 public:
    DECLARE_CLASS_NAME(CommonDefineTest);
    void Test();
};

INDEXLIB_UNIT_TEST_CASE(CommonDefineTest, Test);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __COMMON_DEFINE_TEST_H
