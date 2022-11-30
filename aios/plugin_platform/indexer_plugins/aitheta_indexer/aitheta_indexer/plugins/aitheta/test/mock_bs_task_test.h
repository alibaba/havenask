#ifndef __BS_BUILD_MOCK_TEST_H
#define __BS_BUILD_MOCK_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class MockBsTaskTest : public AithetaTestBase {
 public:
    MockBsTaskTest() = default;
    ~MockBsTaskTest() = default;

 public:
    DECLARE_CLASS_NAME(MockBsTaskTest);

 public:
    void MockFullMerge();
    void MockIncMerge();
    void MockOptimizeMerge();
    void MockFullMergeWithMultiIndex();
    void MockIncMergeWithMultiIndex();
    void MockOptimizeMergeWithMultiIndex();
    void MockFullMergeEndParallelReduce();
    void MockOptMergeEndParallelReduce();
    void MockIncMergeEndParallelReduce();
    void MockFullMergeEndParallelReduceRetry();
    void MockOptMergeEndParallelReduceRetry();
    void MockIncMergeEndParallelReduceRetry();

 private:
    void FullMerge(uint32_t parallelCount);
    void IncMerge(uint32_t parallelCount);
    void OptimizeMerge(uint32_t parallelCount);
};

INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockFullMerge);
INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockIncMerge);
INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockOptimizeMerge);
INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockFullMergeWithMultiIndex);
INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockIncMergeWithMultiIndex);
INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockOptimizeMergeWithMultiIndex);
INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockFullMergeEndParallelReduce);
INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockOptMergeEndParallelReduce);
INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockIncMergeEndParallelReduce);
INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockFullMergeEndParallelReduceRetry);
INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockOptMergeEndParallelReduceRetry);
INDEXLIB_UNIT_TEST_CASE(MockBsTaskTest, MockIncMergeEndParallelReduceRetry);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __BS_BUILD_MOCK_TEST_H
