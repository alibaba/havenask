#ifndef __AITHETA_INC_TEST_H
#define __AITHETA_INC_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaIncTest : public AithetaTestBase {
 public:
    AithetaIncTest() = default;
    ~AithetaIncTest() = default;

 public:
    DECLARE_CLASS_NAME(AithetaIncTest);

 public:
    void TestPQInc();
    void TestHCInc();
    void TestGraph();
    void TestBoundary();

 private:
    void InnerTestInc(const util::KeyValueMap &params);
    IE_NAMESPACE(file_system)::DirectoryPtr
        CreateReduceItem(const util::KeyValueMap &params, std::string srcs, docid_t beginId, const DocIdMap &docIdMap,
                         IE_NAMESPACE(index)::IndexReduceItemPtr &reduceItem);
};

INDEXLIB_UNIT_TEST_CASE(AithetaIncTest, TestPQInc);
INDEXLIB_UNIT_TEST_CASE(AithetaIncTest, TestHCInc);
INDEXLIB_UNIT_TEST_CASE(AithetaIncTest, TestGraph);
INDEXLIB_UNIT_TEST_CASE(AithetaIncTest, TestBoundary);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __AITHETA_INC_TEST_H
