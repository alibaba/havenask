#ifndef __INDEXLIB_KKVREADERTEST_H
#define __INDEXLIB_KKVREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KKVReaderTest : public INDEXLIB_TESTBASE
{
public:
    KKVReaderTest();
    ~KKVReaderTest();

    DECLARE_CLASS_NAME(KKVReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestBlockCache();
    void TestSearchCache();
    void TestTableTypeWithCuckoo();
    void TestMultiInMemSegments();
    void TestMultiRegion();
    void TestCompatibleIndex();
    void TestBatchLookup();
    void TestBug35656866();

private:
    void CheckResult(const KKVReaderPtr& kkvReader, const std::string& pkeyStr, const std::string& valueResult);

private:
    config::IndexPartitionOptions mOptions;
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KKVReaderTest, TestBlockCache);
INDEXLIB_UNIT_TEST_CASE(KKVReaderTest, TestSearchCache);
INDEXLIB_UNIT_TEST_CASE(KKVReaderTest, TestTableTypeWithCuckoo);
INDEXLIB_UNIT_TEST_CASE(KKVReaderTest, TestMultiInMemSegments);
INDEXLIB_UNIT_TEST_CASE(KKVReaderTest, TestMultiRegion);
INDEXLIB_UNIT_TEST_CASE(KKVReaderTest, TestCompatibleIndex);
INDEXLIB_UNIT_TEST_CASE(KKVReaderTest, TestBatchLookup);
INDEXLIB_UNIT_TEST_CASE(KKVReaderTest, TestBug35656866);
}} // namespace indexlib::index

#endif //__INDEXLIB_KKVREADERTEST_H
