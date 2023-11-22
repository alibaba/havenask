#include "indexlib/legacy/index_base/SchemaAdapter.h"

#include "indexlib/base/Constant.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/test/PackageFileUtil.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "unittest/unittest.h"

namespace indexlib::legacy::index_base {

class SchemaAdapterTest : public TESTBASE
{
public:
    SchemaAdapterTest() = default;
    ~SchemaAdapterTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void SchemaAdapterTest::setUp() {}

void SchemaAdapterTest::tearDown() {}

TEST_F(SchemaAdapterTest, LoadAdaptiveBitmapTerms)
{
    auto schema = test::SchemaMaker::MakeSchema(
        "pkstr:string;long1:uint32;long2:int32;long3:int64", "pk:primarykey64:pkstr;idx2:number:long1",
        "long1;pack_attr1:long2,long3", "pkstr;long1;long2", "", "pkstr:long1#long2:long3");
    ASSERT_TRUE(schema);
    auto indexSchema = schema->GetIndexSchema();
    ASSERT_TRUE(indexSchema);
    auto oldIndexConfig = indexSchema->GetIndexConfig("idx2");
    ASSERT_TRUE(oldIndexConfig);
    oldIndexConfig->SetAdaptiveDictConfig(std::make_shared<config::AdaptiveDictionaryConfig>());

    auto fileSystem = file_system::FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow();
    auto directory = file_system::Directory::Get(fileSystem);
    auto adaptiveDir = directory->MakeDirectory(ADAPTIVE_DICT_DIR_NAME);
    auto fileWriter = adaptiveDir->CreateFileWriter("idx2");
    ASSERT_EQ(file_system::FSEC_OK, fileWriter->Close());
    auto phyDir = file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH());
    SchemaAdapter::LoadAdaptiveBitmapTerms(phyDir, schema.get());

    auto indexConfig = indexSchema->GetIndexConfig("idx2");
    ASSERT_TRUE(indexConfig);
    ASSERT_TRUE(indexConfig->GetHighFreqVocabulary());
}

} // namespace indexlib::legacy::index_base
