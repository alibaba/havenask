#include "indexlib/config/test/index_config_unittest.h"

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace config {

class IndexConfigMock : public IndexConfig
{
public:
    IndexConfigMock()
    {
        FieldConfigPtr fieldConfig(new FieldConfig("f1", ft_int8, false));
        mFieldConfig = fieldConfig;
    }
    ~IndexConfigMock() {}

public:
    uint32_t GetFieldCount() const override { return 0; }
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> GetFieldConfigs() const override { return {}; }
    Iterator CreateIterator() const override { return Iterator(FieldConfigPtr()); }
    std::map<std::string, std::string> GetDictHashParams() const override { return {}; }

    bool IsInIndex(fieldid_t fieldId) const override { return true; }
    void AssertCompatible(const IndexConfig& other) const override {}
    IndexConfig* Clone() const override { return NULL; }
    bool CheckFieldType(FieldType ft) const override { return true; }
    std::unique_ptr<indexlibv2::config::InvertedIndexConfig> ConstructConfigV2() const override { return nullptr; }

private:
    FieldConfigPtr mFieldConfig;
    AUTIL_LOG_DECLARE();
};

void IndexConfigTest::CaseSetUp()
{
    mRootDir = IDirectory::Get(FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow());
}

void IndexConfigTest::CaseTearDown() {}

void IndexConfigTest::GenerateTruncateMetaFile(uint32_t lineNum, string fileName, uint32_t repeatNum)
{
    ArchiveFolderPtr folder(new ArchiveFolder(true));
    ASSERT_EQ(FSEC_OK, folder->Open(mRootDir, ""));
    string content;
    for (uint32_t k = 0; k < repeatNum; k++) {
        for (uint32_t i = 0; i < lineNum; i++) {
            dictkey_t key = i;
            int64_t value = rand();

            string keyStr = StringUtil::toString(key);
            string valueStr = StringUtil::toString(value);
            string line = keyStr + "\t" + valueStr + "\n";
            content += line;
        }

        string line = index::DictKeyInfo::NULL_TERM.ToString() + "\t" + StringUtil::toString(rand()) + "\n";
        content += line;
    }
    auto fileWrapper = folder->CreateFileWriter(fileName).GetOrThrow();
    fileWrapper->Write(content.c_str(), content.size()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWrapper->Close());
    ASSERT_EQ(FSEC_OK, folder->Close());
}

void IndexConfigTest::TestCaseForIsTruncateTerm()
{
    {
        IndexConfigMock indexConfig;
        ASSERT_TRUE(!indexConfig.IsTruncateTerm(index::DictKeyInfo(1)));
    }

    {
        IndexConfigMock indexConfig;
        string truncateMetaFile = "truncate_meta";
        GenerateTruncateMetaFile(3, "truncate_meta");
        vector<string> truncIndexNames;
        truncIndexNames.push_back("truncate_meta");
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        ASSERT_EQ(FSEC_OK, folder->Open(mRootDir, ""));
        ASSERT_TRUE(indexConfig.LoadTruncateTermVocabulary(folder, truncIndexNames).IsOK());
        ASSERT_EQ(FSEC_OK, folder->Close());
        ASSERT_TRUE(indexConfig.IsTruncateTerm(index::DictKeyInfo(1)));
        ASSERT_TRUE(indexConfig.IsTruncateTerm(index::DictKeyInfo(2)));
        ASSERT_TRUE(indexConfig.IsTruncateTerm(index::DictKeyInfo(0)));
        ASSERT_TRUE(indexConfig.IsTruncateTerm(index::DictKeyInfo::NULL_TERM));
        ASSERT_TRUE(!indexConfig.IsTruncateTerm(index::DictKeyInfo(3)));
    }
}

void IndexConfigTest::TestCaseForGetTruncatePostingCount()
{
    int32_t count;
    {
        IndexConfigMock indexConfig;
        ASSERT_TRUE(!indexConfig.GetTruncatePostingCount(index::DictKeyInfo(1), count));
        ASSERT_EQ(0, count);
    }

    {
        IndexConfigMock indexConfig;
        GenerateTruncateMetaFile(3, "truncate_meta", 2);
        vector<string> truncIndexNames;
        truncIndexNames.push_back("truncate_meta");
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        ASSERT_EQ(FSEC_OK, folder->Open(mRootDir, ""));
        ASSERT_TRUE(indexConfig.LoadTruncateTermVocabulary(folder, truncIndexNames).IsOK());
        ASSERT_EQ(FSEC_OK, folder->Close());
        ASSERT_TRUE(indexConfig.GetTruncatePostingCount(index::DictKeyInfo(1), count));
        ASSERT_EQ(2, count);
        ASSERT_TRUE(indexConfig.GetTruncatePostingCount(index::DictKeyInfo(2), count));
        ASSERT_EQ(2, count);
        ASSERT_TRUE(indexConfig.GetTruncatePostingCount(index::DictKeyInfo(0), count));
        ASSERT_EQ(2, count);
        ASSERT_TRUE(indexConfig.GetTruncatePostingCount(index::DictKeyInfo::NULL_TERM, count));
        ASSERT_EQ(2, count);

        ASSERT_TRUE(!indexConfig.GetTruncatePostingCount(index::DictKeyInfo(3), count));
        ASSERT_EQ(0, count);
    }
}

void IndexConfigTest::TestCaseForIsBitmapOnlyTerm()
{
    {
        IndexConfigMock indexConfig;
        ASSERT_TRUE(!indexConfig.IsBitmapOnlyTerm(index::DictKeyInfo(1)));
    }

    {
        IndexConfigMock indexConfig;
        std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig(
            /*dictName =*/"",
            /*content =*/""));
        indexConfig.SetDictConfig(dictConfig);
        indexConfig.SetHighFreqencyTermPostingType(indexlib::index::hp_both);
        ASSERT_TRUE(!indexConfig.IsBitmapOnlyTerm(index::DictKeyInfo(1)));
    }

    {
        std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig(
            /*dictName =*/"",
            /*content =*/""));
        IndexConfigMock indexConfig;
        indexConfig.SetDictConfig(dictConfig);
        std::shared_ptr<HighFrequencyVocabulary> vocabulary = indexConfig.GetHighFreqVocabulary();
        vocabulary->AddKey(index::DictKeyInfo(0));
        vocabulary->AddKey(index::DictKeyInfo(1));
        vocabulary->AddKey(index::DictKeyInfo(2));
        vocabulary->AddKey(index::DictKeyInfo::NULL_TERM);

        indexConfig.SetHighFreqencyTermPostingType(indexlib::index::hp_bitmap);

        ASSERT_TRUE(indexConfig.IsBitmapOnlyTerm(index::DictKeyInfo(0)));
        ASSERT_TRUE(indexConfig.IsBitmapOnlyTerm(index::DictKeyInfo(2)));
        ASSERT_TRUE(indexConfig.IsBitmapOnlyTerm(index::DictKeyInfo(1)));
        ASSERT_TRUE(indexConfig.IsBitmapOnlyTerm(index::DictKeyInfo::NULL_TERM));
        ASSERT_TRUE(!indexConfig.IsBitmapOnlyTerm(index::DictKeyInfo(3)));
    }
}

void IndexConfigTest::TestCaseForReferenceCompress()
{
    IndexPartitionSchemaPtr schema =
        SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "/index_config", "reference_compress.json");
    ASSERT_TRUE(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    ASSERT_EQ((size_t)8, indexSchema->GetIndexCount());

    // normal
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("phrase");
    ASSERT_TRUE(indexConfig);
    ASSERT_FALSE(indexConfig->IsReferenceCompress());

    indexConfig = indexSchema->GetIndexConfig("phrase2");
    ASSERT_TRUE(indexConfig);
    ASSERT_FALSE(indexConfig->IsReferenceCompress());

    // reference
    indexConfig = indexSchema->GetIndexConfig("phrase_reference");
    ASSERT_TRUE(indexConfig);
    ASSERT_TRUE(indexConfig->IsReferenceCompress());

    // truncate
    indexConfig = indexSchema->GetIndexConfig("phrase_reference_desc_product_id");
    ASSERT_TRUE(indexConfig);
    ASSERT_TRUE(indexConfig->IsReferenceCompress());

    indexConfig = indexSchema->GetIndexConfig("phrase_reference_desc_user_name");
    ASSERT_TRUE(indexConfig);
    ASSERT_TRUE(indexConfig->IsReferenceCompress());
}

void IndexConfigTest::TestCaseForReferenceCompressFail()
{
    string path = GET_PRIVATE_TEST_DATA_PATH() + "/index_config";
    ASSERT_THROW(SchemaLoader::LoadSchema(path, "reference_compress_bad.json"), SchemaException);
}

void IndexConfigTest::TestGetIndexNameFromShardingIndexName()
{
    string indexName;
    ASSERT_TRUE(IndexConfig::GetIndexNameFromShardingIndexName("test_name_@_1", indexName));
    ASSERT_EQ(string("test_name"), indexName);

    ASSERT_TRUE(IndexConfig::GetIndexNameFromShardingIndexName("test_name_@_b_@_1", indexName));
    ASSERT_EQ(string("test_name_@_b"), indexName);

    ASSERT_FALSE(IndexConfig::GetIndexNameFromShardingIndexName("test_name_1", indexName));
    ASSERT_FALSE(IndexConfig::GetIndexNameFromShardingIndexName("test_name_1_@_b", indexName));
}

void IndexConfigTest::TestCaseForUpdatableIndex()
{
    {
        string schemaJson = R"( {
            "table_name": "noname",
            "indexs": [
                {
                    "term_frequency_flag": 0,
                    "index_type": "NUMBER",
                    "index_name": "int_index",
                    "index_fields": "int",
                    "doc_payload_flag": 0,
                    "index_updatable": true
                },
                {
                    "index_type": "PRIMARYKEY64",
                    "index_name": "pk",
                    "index_fields": "string"
                }
            ],
            "fields": [
                {
                    "field_type": "INTEGER",
                    "field_name": "int"
                },
                {
                    "field_type": "STRING",
                    "field_name": "string"
                }
            ],
            "attributes": [
                "int",
                "string"
            ]
        } )";

        config::IndexPartitionSchema schema("noname");
        auto check = [](const config::IndexPartitionSchema& schema) {
            auto indexSchema = schema.GetIndexSchema();
            ASSERT_TRUE(indexSchema->AnyIndexUpdatable());
            ASSERT_TRUE(indexSchema->AllIndexUpdatableForField(/*fieldId*/ 0));
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("int_index");
            ASSERT_TRUE(indexConfig);
            ASSERT_TRUE(indexConfig->IsIndexUpdatable());
            ASSERT_FALSE(indexConfig->IsPatchCompressed());
        };

        autil::legacy::FromJsonString(schema, schemaJson);
        check(schema);

        auto str = autil::legacy::ToJsonString(schema, /*isCompact*/ true);
        config::IndexPartitionSchema schema2("noname");
        autil::legacy::FromJsonString(schema2, str);
        check(schema2);
    }
    {
        // has term frequency
        string schemaJson = R"( {
            "table_name": "noname",
            "indexs": [
                {
                    "index_type": "NUMBER",
                    "index_name": "int_index",
                    "index_fields": "int",
                    "doc_payload_flag": 0,
                    "index_updatable": true
                },
                {
                    "index_type": "PRIMARYKEY64",
                    "index_name": "pk",
                    "index_fields": "string"
                }
            ],
            "fields": [
                {
                    "field_type": "INTEGER",
                    "field_name": "int"
                },
                {
                    "field_type": "STRING",
                    "field_name": "string"
                }
            ],
            "attributes": [
                "int",
                "string"
            ]
        } )";

        config::IndexPartitionSchema schema("noname");
        ASSERT_THROW(autil::legacy::FromJsonString(schema, schemaJson), util::SchemaException);
    }
    {
        // disable update
        string schemaJson = R"( {
            "table_name": "noname",
            "indexs": [
                {
                    "term_frequency_flag": 0,
                    "index_type": "NUMBER",
                    "index_name": "int_index",
                    "index_fields": "int",
                    "doc_payload_flag": 0
                },
                {
                    "index_type": "PRIMARYKEY64",
                    "index_name": "pk",
                    "index_fields": "string"
                }
            ],
            "fields": [
                {
                    "field_type": "INTEGER",
                    "field_name": "int"
                },
                {
                    "field_type": "STRING",
                    "field_name": "string"
                }
            ],
            "attributes": [
                "int",
                "string"
            ]
        } )";

        config::IndexPartitionSchema schema("noname");
        auto check = [](const config::IndexPartitionSchema& schema) {
            auto indexSchema = schema.GetIndexSchema();
            ASSERT_FALSE(indexSchema->AnyIndexUpdatable());
            ASSERT_FALSE(indexSchema->AllIndexUpdatableForField(/*fieldId*/ 0));
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("int_index");
            ASSERT_TRUE(indexConfig);
            ASSERT_FALSE(indexConfig->IsIndexUpdatable());
            ASSERT_FALSE(indexConfig->IsPatchCompressed());
        };

        autil::legacy::FromJsonString(schema, schemaJson);
        check(schema);

        auto str = autil::legacy::ToJsonString(schema, /*isCompact*/ true);
        config::IndexPartitionSchema schema2("noname");
        autil::legacy::FromJsonString(schema2, str);
        check(schema2);
    }
}

}} // namespace indexlib::config
