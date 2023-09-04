#ifndef __INDEXLIB_KVREADERTEST_H
#define __INDEXLIB_KVREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/kv/kv_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KVReaderTest : public INDEXLIB_TESTBASE
{
public:
    KVReaderTest();
    ~KVReaderTest();

    DECLARE_CLASS_NAME(KVReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestBlockCache();
    void TestSearchCache();
    void TestSearchCacheIncReclaimAllRtBug();
    void TestCuckoo();

    void TestGetFixLenValue();
    void TestGetWithRegionId();
    void TestBatchGet();

private:
    template <typename Reader>
    void PrepareSegmentReader(const std::string& offlineReaderStr, const std::string& onlineReaderStr, Reader& reader,
                              size_t fixLen);

    // fixValueStr: 1#3#4#2
    std::string MakeFixValue(const std::string& fixValueStr, size_t fixLen);

    template <typename ValueType>
    void CheckValue(const KVReader* reader, uint64_t key, const std::string& resultStr, const KVReadOptions& options);

private:
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KVReaderTest, TestBlockCache);
INDEXLIB_UNIT_TEST_CASE(KVReaderTest, TestSearchCache);
INDEXLIB_UNIT_TEST_CASE(KVReaderTest, TestCuckoo);
INDEXLIB_UNIT_TEST_CASE(KVReaderTest, TestGetFixLenValue);
INDEXLIB_UNIT_TEST_CASE(KVReaderTest, TestGetWithRegionId);
INDEXLIB_UNIT_TEST_CASE(KVReaderTest, TestBatchGet);
// INDEXLIB_UNIT_TEST_CASE(KVReaderTest, TestSearchCacheIncReclaimAllRtBug);
}} // namespace indexlib::index

#endif //__INDEXLIB_KVREADERTEST_H
