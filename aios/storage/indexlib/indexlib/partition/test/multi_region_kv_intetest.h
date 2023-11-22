#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/test/kv_table_intetest.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
namespace indexlib { namespace partition {

class MultiRegionKVInteTest : public KVTableInteTest
{
public:
    MultiRegionKVInteTest();
    ~MultiRegionKVInteTest();

    DECLARE_CLASS_NAME(MultiRegionKVInteTest);

protected:
    config::IndexPartitionSchemaPtr CreateSchema(const std::string& fields, const std::string& key,
                                                 const std::string& values, int64_t ttl = INVALID_TTL) override;
    std::vector<document::DocumentPtr> CreateKVDocuments(const config::IndexPartitionSchemaPtr& schema,
                                                         const std::string& docStrs) override;

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestAddAndDel();
    void TestSetGlobalRegionPreference();
    void TestDifferentTTL();
    void TestNoTTL();
    void TestTTL();
    void TestMultiField();
    void TestSetOneRegionFieldSchema();
    void TestSetTwoRegionFieldSchema();
    void TestMultiRegionWithFixLengthAttribute();

private:
    void MakeGlobalRegionIndexPreference(const config::IndexPartitionSchemaPtr& schema, const std::string& hashDictType,
                                         const std::string& valueCompressType);

    void CheckCompress(file_system::DirectoryPtr dir, const std::string& filePath,
                       const std::string& valueCompressType);

    void InnerTestSetGlobalRegionPreference(const std::string& valueCompressType);

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition
