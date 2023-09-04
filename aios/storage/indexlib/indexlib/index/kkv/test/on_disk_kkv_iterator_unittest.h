#ifndef __INDEXLIB_ONDISKKKVITERATORTEST_H
#define __INDEXLIB_ONDISKKKVITERATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/kkv/on_disk_kkv_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class OnDiskKKVIteratorTest : public INDEXLIB_TESTBASE
{
public:
    OnDiskKKVIteratorTest();
    ~OnDiskKKVIteratorTest();

    DECLARE_CLASS_NAME(OnDiskKKVIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestSamePKeyInMultiSegments();
    void TestDuplicateSeyInMultiSegments();
    void TestDuplicateSeyInMultiSegments_13227243();

private:
    void InnerTest(const std::string& docStrs, const std::string& dataInfoStr, PKeyTableType tableType,
                   size_t expectPkeyCount);

    OnDiskKKVIteratorPtr CreateIterator(const std::vector<std::string>& docStrVec, PKeyTableType tableType);

    void CheckIterator(const OnDiskKKVIteratorPtr& iter, const std::string& dataInfoStr, size_t expectPkeyCount);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    config::KKVIndexConfigPtr mKKVConfig;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnDiskKKVIteratorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OnDiskKKVIteratorTest, TestSamePKeyInMultiSegments);
INDEXLIB_UNIT_TEST_CASE(OnDiskKKVIteratorTest, TestDuplicateSeyInMultiSegments);
INDEXLIB_UNIT_TEST_CASE(OnDiskKKVIteratorTest, TestDuplicateSeyInMultiSegments_13227243);
}} // namespace indexlib::index

#endif //__INDEXLIB_ONDISKKKVITERATORTEST_H
