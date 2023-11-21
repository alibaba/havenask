#pragma once

#include "indexlib/common/hash_table/dense_hash_table.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

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
}} // namespace indexlib::common
