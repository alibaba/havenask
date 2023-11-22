#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/test/kkv_table_intetest.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
namespace indexlib { namespace partition {

class MultiRegionKKVInteTest : public KKVTableTest
{
public:
    MultiRegionKKVInteTest();
    ~MultiRegionKKVInteTest();

    DECLARE_CLASS_NAME(MultiRegionKKVInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

protected:
    config::IndexPartitionSchemaPtr CreateSchema(const std::string& fields, const std::string& pkey,
                                                 const std::string& skey, const std::string& values) override;

    std::vector<document::DocumentPtr> CreateKKVDocuments(const config::IndexPartitionSchemaPtr& schema,
                                                          const std::string& docStrs) override;

private:
    void MakeGlobalRegionIndexPreference(const config::IndexPartitionSchemaPtr& schema, const std::string& hashDictType,
                                         const std::string& skeyCompressType, const std::string& valueCompressType);

    void CheckCompress(file_system::DirectoryPtr dir, const std::string& skeyCompressType,
                       const std::string& valueCompressType);

    void InnerTestSetGlobalRegionPreference(const std::string& skeyCompressType, const std::string valueCompressType);

public:
    void TestSimpleProcess();
    void TestMergeTruncWithMultiRegion();
    void TestMergeTruncWithDiffenertFieldSchema();
    void TestMergeSortWithMultiRegion();
    void TestMergeSortWithDiferentFieldSchema();
    void TestTTL();
    void TestCompress();
    void TestSetGlobalRegionPreference();

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition
