#ifndef __INDEXLIB_ONDISKCLOSEDHASHITERATORTEST_H
#define __INDEXLIB_ONDISKCLOSEDHASHITERATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/on_disk_closed_hash_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class OnDiskClosedHashIteratorTest : public INDEXLIB_TESTBASE
{
public:
    OnDiskClosedHashIteratorTest();
    ~OnDiskClosedHashIteratorTest();

    DECLARE_CLASS_NAME(OnDiskClosedHashIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDenseProcess();
    void TestCuckooProcess();

private:
    template <PKeyTableType Type>
    void PrepareData(size_t count);
    template <PKeyTableType Type>
    void CheckData(size_t count);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::KKVIndexConfigPtr mKKVIndexConfig;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnDiskClosedHashIteratorTest, TestDenseProcess);
INDEXLIB_UNIT_TEST_CASE(OnDiskClosedHashIteratorTest, TestCuckooProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_ONDISKCLOSEDHASHITERATORTEST_H
