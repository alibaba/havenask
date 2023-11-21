#include "autil/Log.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/config/truncate_index_name_mapper.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace config {

class TruncateIndexNameMapperTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TruncateIndexNameMapperTest);
    void CaseSetUp() override
    {
        mFs = file_system::FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow();
        mRootDir = file_system::IDirectory::ToLegacyDirectory(file_system::IDirectory::Get(mFs));
    }

    void CaseTearDown() override {}

    void TestCaseForLoad()
    {
        std::string truncateDir = "truncate_index_mapper";
        ASSERT_EQ(file_system::FSEC_OK, mFs->MountDir(GET_PRIVATE_TEST_DATA_PATH(), truncateDir, truncateDir,
                                                      file_system::FSMT_READ_ONLY, true));
        auto dir = mRootDir->GetDirectory(truncateDir, false);
        ASSERT_NE(dir, nullptr);
        TruncateIndexNameMapper truncMapper(dir);
        truncMapper.Load();
        {
            vector<string> truncNames;
            ASSERT_TRUE(truncMapper.Lookup("index1", truncNames));
            ASSERT_EQ((size_t)2, truncNames.size());
            ASSERT_EQ(string("index1_trunc1"), truncNames[0]);
            ASSERT_EQ(string("index1_trunc2"), truncNames[1]);
        }

        {
            vector<string> truncNames;
            ASSERT_TRUE(truncMapper.Lookup("index2", truncNames));
            ASSERT_EQ((size_t)3, truncNames.size());
            ASSERT_EQ(string("index2_trunc1"), truncNames[0]);
            ASSERT_EQ(string("index2_trunc2"), truncNames[1]);
            ASSERT_EQ(string("index2_trunc3"), truncNames[2]);
        }

        {
            vector<string> truncNames;
            ASSERT_TRUE(truncMapper.Lookup("index3", truncNames));
            ASSERT_EQ((size_t)1, truncNames.size());
            ASSERT_EQ(string("index3_trunc1"), truncNames[0]);
        }

        {
            vector<string> truncNames;
            ASSERT_TRUE(!truncMapper.Lookup("index_nonexist", truncNames));
        }
    }

    void TestCaseForDump()
    {
        IndexPartitionSchemaPtr indexPartitionSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(indexPartitionSchema,
                                              // field schema
                                              "title:text;ends:uint32;userid:uint32;nid:uint32;url:string",
                                              // index schema
                                              "title:text:title;pk:primarykey64:url",
                                              // attribute schema
                                              "ends;userid;nid",
                                              // summary schema
                                              "");

        IndexSchemaPtr indexSchema = indexPartitionSchema->GetIndexSchema();
        AddTruncateIndexConfig(indexSchema, "title", "title_trunc1");
        AddTruncateIndexConfig(indexSchema, "title", "title_trunc2");

        TruncateIndexNameMapper truncMapper(mRootDir);
        truncMapper.Dump(indexSchema);

        TruncateIndexNameMapper loadTruncMapper(mRootDir);
        loadTruncMapper.Load();

        vector<string> truncNames;
        ASSERT_TRUE(loadTruncMapper.Lookup("title", truncNames));
        ASSERT_EQ((size_t)2, truncNames.size());
        ASSERT_EQ(string("title_trunc1"), truncNames[0]);
        ASSERT_EQ(string("title_trunc2"), truncNames[1]);
    }

    void TestCaseForDumpShardingIndex()
    {
        IndexPartitionSchemaPtr indexPartitionSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "simple_sharding_example.json");
        IndexSchemaPtr indexSchema = indexPartitionSchema->GetIndexSchema();
        TruncateIndexNameMapper truncMapper(mRootDir);
        truncMapper.Dump(indexSchema);

        TruncateIndexNameMapper loadTruncMapper(mRootDir);
        loadTruncMapper.Load();
        vector<string> truncNames;
        for (size_t i = 0; i < 4; i++) {
            ASSERT_TRUE(loadTruncMapper.Lookup("phrase_@_" + StringUtil::toString(i), truncNames));
            ASSERT_EQ((size_t)1, truncNames.size());
            ASSERT_TRUE(loadTruncMapper.Lookup("expack_@_" + StringUtil::toString(i), truncNames));
            ASSERT_EQ((size_t)2, truncNames.size());
        }
        ASSERT_FALSE(loadTruncMapper.Lookup("phrase", truncNames));
        ASSERT_FALSE(loadTruncMapper.Lookup("expack", truncNames));
        ASSERT_FALSE(loadTruncMapper.Lookup("title", truncNames));
        ASSERT_TRUE(loadTruncMapper.Lookup("title_@_0", truncNames));
        ASSERT_EQ((size_t)2, truncNames.size());
    }

    void AddTruncateIndexConfig(const IndexSchemaPtr& indexSchema, const string& indexName,
                                const string& truncIndexName)
    {
        IndexConfigPtr parentIndexConfig = indexSchema->GetIndexConfig(indexName);
        assert(parentIndexConfig);

        IndexConfigPtr indexConfig(parentIndexConfig->Clone());
        indexConfig->SetIndexName(truncIndexName);
        indexConfig->SetVirtual(true);
        indexConfig->SetNonTruncateIndexName(indexName);
        indexSchema->AddIndexConfig(indexConfig);
    }

private:
    std::shared_ptr<file_system::Directory> mRootDir;
    std::shared_ptr<file_system::IFileSystem> mFs;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(index, TruncateIndexNameMapperTest);

INDEXLIB_UNIT_TEST_CASE(TruncateIndexNameMapperTest, TestCaseForLoad);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexNameMapperTest, TestCaseForDump);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexNameMapperTest, TestCaseForDumpShardingIndex);
}} // namespace indexlib::config
