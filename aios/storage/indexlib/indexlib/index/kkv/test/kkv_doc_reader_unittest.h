#ifndef __INDEXLIB_KKVDOCREADERTEST_H
#define __INDEXLIB_KKVDOCREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

struct KeyData {
    std::string pkey;
    std::string skey;
    bool operator<(const KeyData& k) const
    {
        if (pkey < k.pkey)
            return true;
        else if (pkey > k.pkey)
            return false;
        if (skey < k.skey)
            return true;
        else if (skey > k.skey)
            return false;
        return false;
    }
};

struct ValueData {
    std::string value1, value2;
    uint32_t timestamp;
};

class KKVDocReaderTest : public INDEXLIB_TESTBASE
{
public:
    KKVDocReaderTest();
    ~KKVDocReaderTest();

    DECLARE_CLASS_NAME(KKVDocReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNumberHash();
    void TestNotNumberHash();
    void TestReadWithOptimizedStoreSKeyUInt8();
    void TestReadWithOptimizedStoreSKeyUInt16();
    void TestReadWithOptimizedStoreSKeyUInt32();
    void TestReadWithOptimizedStoreSKeyUInt64();
    void TestReadWithOptimizedStoreSKeyInt8();
    void TestReadWithOptimizedStoreSKeyInt16();
    void TestReadWithOptimizedStoreSKeyInt32();
    void TestReadWithOptimizedStoreSKeyInt64();
    void TestReadWithNotOptimizedStoreFloatSKey();
    void TestReadWithNotOptimizedStoreDoubleSKey();
    void TestReadWithNotOptimizedStoreStringSKey();

private:
    void PrepareSchema(const std::string& fields);
    void BuildKKVTable(const std::string& docString);
    void CheckKKVResult(const std::map<KeyData, ValueData>& expectedResult);
    void CheckKKVResultForMultiField(const std::map<KeyData, ValueData>& expectedResult);
    void TestKKVHelper(const std::string& fields, const std::string& docString,
                       const std::map<KeyData, ValueData>& expectedResult);
    void InnerTestReadValue(const std::string& fields, bool optimizedStoreSKey, const std::string& valueFormat);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::KVIndexConfigPtr mKVConfig;
    config::IndexPartitionOptions mOptions;
    partition::OnDiskPartitionDataPtr mPartitionData;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestNumberHash);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestNotNumberHash);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestReadWithOptimizedStoreSKeyUInt8);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestReadWithOptimizedStoreSKeyUInt16);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestReadWithOptimizedStoreSKeyUInt32);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestReadWithOptimizedStoreSKeyUInt64);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestReadWithOptimizedStoreSKeyInt8);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestReadWithOptimizedStoreSKeyInt16);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestReadWithOptimizedStoreSKeyInt32);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestReadWithOptimizedStoreSKeyInt64);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestReadWithNotOptimizedStoreFloatSKey);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestReadWithNotOptimizedStoreDoubleSKey);
INDEXLIB_UNIT_TEST_CASE(KKVDocReaderTest, TestReadWithNotOptimizedStoreStringSKey);
}} // namespace indexlib::index

#endif //__INDEXLIB_KKVDOCREADERTEST_H
