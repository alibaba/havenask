#ifndef __REDUCE_TASK_CREATER_TEST_H
#define __REDUCE_TASK_CREATER_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"
#include "aitheta_indexer/plugins/aitheta/util/reduce_task/reduce_task_creater.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class ReduceTaskCreaterTest : public AithetaTestBase {
 public:
    ReduceTaskCreaterTest() = default;
    ~ReduceTaskCreaterTest() = default;

 public:
    DECLARE_CLASS_NAME(ReduceTaskCreaterTest);
    void TestCreateForInc();
    void TestCreateForFull();
    void TestCreateForOpt();
    void TestCreateEmpty();

 private:
    bool GetTask(index_base::resourceid_t resId, CustomReduceTask& task);
};

INDEXLIB_UNIT_TEST_CASE(ReduceTaskCreaterTest, TestCreateForInc);
INDEXLIB_UNIT_TEST_CASE(ReduceTaskCreaterTest, TestCreateForFull);
INDEXLIB_UNIT_TEST_CASE(ReduceTaskCreaterTest, TestCreateForOpt);
INDEXLIB_UNIT_TEST_CASE(ReduceTaskCreaterTest, TestCreateEmpty);
IE_NAMESPACE_END(aitheta_plugin);

#endif  // __REDUCE_TASK_CREATER_TEST_H
