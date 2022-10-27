#ifndef __INDEXLIB_DENSEHASHTABLEMULTITHREADTEST_H
#define __INDEXLIB_DENSEHASHTABLEMULTITHREADTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/hash_table/dense_hash_table.h"

IE_NAMESPACE_BEGIN(common);

class DenseHashTableMultiThreadTest : public INDEXLIB_TESTBASE
{
public:
    DenseHashTableMultiThreadTest();
    ~DenseHashTableMultiThreadTest();

    DECLARE_CLASS_NAME(DenseHashTableMultiThreadTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TsetForTimestampBucket();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DenseHashTableMultiThreadTest, TsetForTimestampBucket);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DENSEHASHTABLEMULTITHREADTEST_H
