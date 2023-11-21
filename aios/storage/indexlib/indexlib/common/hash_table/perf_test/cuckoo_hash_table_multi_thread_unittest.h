#pragma once

#include "indexlib/common/hash_table/cuckoo_hash_table.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class CuckooHashTableMultiThreadTest : public INDEXLIB_TESTBASE
{
public:
    CuckooHashTableMultiThreadTest();
    ~CuckooHashTableMultiThreadTest();

    DECLARE_CLASS_NAME(CuckooHashTableMultiThreadTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMultiThreadWithTs();
    void TestMultiThreadWithOffset();

private:
    IE_LOG_DECLARE();
};

// INDEXLIB_UNIT_TEST_CASE(CuckooHashTableMultiThreadTest, TestMultiThreadWithTs);
// INDEXLIB_UNIT_TEST_CASE(CuckooHashTableMultiThreadTest, TestMultiThreadWithOffset);
}} // namespace indexlib::common
