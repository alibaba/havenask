#ifndef __AITHETA_ONLINE_TEST_H
#define __AITHETA_ONLINE_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class OnlineIndexTest : public AithetaTestBase {
 public:
    OnlineIndexTest() = default;
    ~OnlineIndexTest() = default;

 public:
    DECLARE_CLASS_NAME(OnlineIndexTest);

    void TestHC();

 private:
    void InnerTestIndex(const util::KeyValueMap &params);

};

INDEXLIB_UNIT_TEST_CASE(OnlineIndexTest, TestHC);
IE_NAMESPACE_END(aitheta_plugin);
#endif  // __AITHETA_ONLINE_TEST_H
