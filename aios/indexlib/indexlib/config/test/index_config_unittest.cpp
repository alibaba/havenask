#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_config.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/test/index_config_unittest.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/high_freq_vocabulary_creator.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(config);

class IndexConfigMock : public IndexConfig
{
public:
    IndexConfigMock()
    {
        IndexConfig::ResetImpl(new SingleFieldIndexConfigImpl("", it_number_int8));
    }
    ~IndexConfigMock() {}
public:
    uint32_t GetFieldCount() const override  { return 0; }
    Iterator CreateIterator() const override
    { return Iterator(FieldConfigPtr()); }

    bool IsInIndex(fieldid_t fieldId) const override { return true; }
    void AssertCompatible(const IndexConfig& other) const override {}
    IndexConfig* Clone() const override { return NULL; }
    bool CheckFieldType(FieldType ft) const override { return true; }
private:
    IE_LOG_DECLARE();
};

void IndexConfigTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void IndexConfigTest::CaseTearDown()
{
}

void IndexConfigTest::GenerateTruncateMetaFile(
        uint32_t lineNum, string fileName,
        uint32_t repeatNum)
{
    ArchiveFolderPtr folder(new ArchiveFolder(true));
    folder->Open(GET_TEST_DATA_PATH());
    string content;
    for (uint32_t k = 0; k < repeatNum; k++)
    {
        for (uint32_t i = 0; i < lineNum; i++)
        {
            dictkey_t key = i;
            int64_t value = rand();

            string keyStr = StringUtil::toString(key);
            string valueStr = StringUtil::toString(value);
            string line = keyStr + "\t" + valueStr + "\n";
            content += line;
        }
    }
    FileWrapperPtr fileWrapper = folder->GetInnerFile(fileName, fslib::WRITE);
    fileWrapper->Write(content.c_str(), content.size());
    fileWrapper->Close();
    folder->Close();
}

void IndexConfigTest::TestCaseForIsTruncateTerm()
{
    {
        IndexConfigMock indexConfig;
        INDEXLIB_TEST_TRUE(!indexConfig.IsTruncateTerm(1));
    }
        
    {
        IndexConfigMock indexConfig;
        string truncateMetaFile = "truncate_meta";
        string fileName = FileSystemWrapper::JoinPath(mRootDir, "truncate_meta");
        GenerateTruncateMetaFile(3, "truncate_meta");
        vector<string> truncIndexNames;
        truncIndexNames.push_back("truncate_meta");
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        folder->Open(GET_TEST_DATA_PATH());
        indexConfig.LoadTruncateTermVocabulary(folder, truncIndexNames);
        folder->Close();
        INDEXLIB_TEST_TRUE(indexConfig.IsTruncateTerm(1));
        INDEXLIB_TEST_TRUE(indexConfig.IsTruncateTerm(2));
        INDEXLIB_TEST_TRUE(indexConfig.IsTruncateTerm(0));
        INDEXLIB_TEST_TRUE(!indexConfig.IsTruncateTerm(3));
    }
}

void IndexConfigTest::TestCaseForGetTruncatePostingCount()
{
    int32_t count;
    {
        IndexConfigMock indexConfig;
        INDEXLIB_TEST_TRUE(!indexConfig.GetTruncatePostingCount(1, count));
        INDEXLIB_TEST_EQUAL(0, count);
    }
        
    {
        IndexConfigMock indexConfig;
        GenerateTruncateMetaFile(3, "truncate_meta", 2);
        vector<string> truncIndexNames;
        truncIndexNames.push_back("truncate_meta");
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        folder->Open(GET_TEST_DATA_PATH());
        indexConfig.LoadTruncateTermVocabulary(folder, truncIndexNames);
        folder->Close();
        INDEXLIB_TEST_TRUE(indexConfig.GetTruncatePostingCount(1, count));
        INDEXLIB_TEST_EQUAL(2, count);
        INDEXLIB_TEST_TRUE(indexConfig.GetTruncatePostingCount(2, count));
        INDEXLIB_TEST_EQUAL(2, count);
        INDEXLIB_TEST_TRUE(indexConfig.GetTruncatePostingCount(0, count));
        INDEXLIB_TEST_EQUAL(2, count);
        INDEXLIB_TEST_TRUE(!indexConfig.GetTruncatePostingCount(3, count));
        INDEXLIB_TEST_EQUAL(0, count);
    }
}

void IndexConfigTest::TestCaseForIsBitmapOnlyTerm()
{
    {
        IndexConfigMock indexConfig;
        INDEXLIB_TEST_TRUE(!indexConfig.IsBitmapOnlyTerm(1));
    }

    {
        IndexConfigMock indexConfig;
        DictionaryConfigPtr dictConfig(
                new DictionaryConfig(
                        /*dictName =*/"",
                        /*content =*/""));
        indexConfig.SetDictConfig(dictConfig);
        indexConfig.SetHighFreqencyTermPostingType(hp_both);
        INDEXLIB_TEST_TRUE(!indexConfig.IsBitmapOnlyTerm(1));
    }

    {
        DictionaryConfigPtr dictConfig(
                new DictionaryConfig(
                        /*dictName =*/"",
                        /*content =*/""));
        IndexConfigMock indexConfig;
        indexConfig.SetDictConfig(dictConfig);
        HighFrequencyVocabularyPtr vocabulary = 
            indexConfig.GetHighFreqVocabulary();
        vocabulary->AddKey(0);
        vocabulary->AddKey(1);
        vocabulary->AddKey(2);
            

        indexConfig.SetHighFreqencyTermPostingType(hp_bitmap);

        INDEXLIB_TEST_TRUE(indexConfig.IsBitmapOnlyTerm(0));
        INDEXLIB_TEST_TRUE(indexConfig.IsBitmapOnlyTerm(2));
        INDEXLIB_TEST_TRUE(indexConfig.IsBitmapOnlyTerm(1));
        INDEXLIB_TEST_TRUE(!indexConfig.IsBitmapOnlyTerm(3));
    }
}

void IndexConfigTest::TestCaseForReferenceCompress()
{
    IndexPartitionSchemaPtr schema = SchemaLoader::LoadSchema(
            string(TEST_DATA_PATH) + "/index_config", "reference_compress.json");
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
    string path = string(TEST_DATA_PATH) + "/index_config";
    ASSERT_THROW(SchemaLoader::LoadSchema(path, "reference_compress_bad.json"),
                 SchemaException);
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

IE_NAMESPACE_END(config);
