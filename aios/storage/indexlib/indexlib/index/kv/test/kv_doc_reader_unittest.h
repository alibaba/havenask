#ifndef __INDEXLIB_KVDOCREADERTEST_H
#define __INDEXLIB_KVDOCREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

struct ValueData {
    std::string value;
    uint32_t timestamp;
};

class KVDocReaderTest : public INDEXLIB_TESTBASE
{
public:
    KVDocReaderTest();
    ~KVDocReaderTest();

    DECLARE_CLASS_NAME(KVDocReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNumberHashAndFixedLenKV();
    void TestNumberHashAndVarLenKV();
    void TestKVMerge();
    void TestNotNumberHashAndFixedLenKV();
    void TestNotNumberHashAndVarLenKV();
    void TestSimpleValue();
    void TestPackValue();

private:
    void PrepareSchema(std::string& fields);
    void BuildKVTable(std::string& docString, bool needMerge);
    void CheckResult(std::map<std::string, ValueData>& expectedResult);
    void TestKVHelper(std::string& fields, std::string& docString, bool needMerge,
                      std::map<std::string, ValueData>& expectedResult);
    void InnerTestPackValue(const std::string& valueFormat);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::KVIndexConfigPtr mKVConfig;
    config::IndexPartitionOptions mOptions;
    partition::OnDiskPartitionDataPtr mPartitionData;

    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KVDocReaderTest, TestNumberHashAndFixedLenKV);
INDEXLIB_UNIT_TEST_CASE(KVDocReaderTest, TestNumberHashAndVarLenKV);
INDEXLIB_UNIT_TEST_CASE(KVDocReaderTest, TestNotNumberHashAndFixedLenKV);
INDEXLIB_UNIT_TEST_CASE(KVDocReaderTest, TestNotNumberHashAndVarLenKV);
INDEXLIB_UNIT_TEST_CASE(KVDocReaderTest, TestSimpleValue);
INDEXLIB_UNIT_TEST_CASE(KVDocReaderTest, TestPackValue);
INDEXLIB_UNIT_TEST_CASE(KVDocReaderTest, TestKVMerge);

}} // namespace indexlib::index

#endif //__INDEXLIB_KVDOCREADERTEST_H
