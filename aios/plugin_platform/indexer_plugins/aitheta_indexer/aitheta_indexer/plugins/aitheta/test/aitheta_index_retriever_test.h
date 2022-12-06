#ifndef __AITHETA_INDEX_RETRIEVER_TEST_H
#define __AITHETA_INDEX_RETRIEVER_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_retriever.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaIndexRetrieverTest : public AithetaTestBase {
 public:
    AithetaIndexRetrieverTest() = default;
    ~AithetaIndexRetrieverTest() = default;

 public:
    DECLARE_CLASS_NAME(AithetaIndexRetrieverTest);
    void TestInit();
};

INDEXLIB_UNIT_TEST_CASE(AithetaIndexRetrieverTest, TestInit);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __AITHETA_INDEX_RETRIEVER_TEST_H
