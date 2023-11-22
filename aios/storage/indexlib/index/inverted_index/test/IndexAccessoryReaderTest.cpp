#include "indexlib/index/inverted_index/IndexAccessoryReader.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletDataSchemaGroup.h"
#include "indexlib/framework/Version.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "unittest/unittest.h"

namespace indexlib::index {

namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
using indexlibv2::index::IndexReaderParameter;

const static std::string identifier = "ut_test";
}; // namespace

class IndexAccessoryReaderTest : public TESTBASE
{
public:
    IndexAccessoryReaderTest() = default;
    ~IndexAccessoryReaderTest() = default;

private:
    std::pair<Status, std::shared_ptr<indexlibv2::framework::TabletData>> CreateEmptyTabletData();
    std::vector<std::shared_ptr<IIndexConfig>> CreateDefaultConfigs();
    std::shared_ptr<IIndexConfig>
    CreateTruncIndexConfig(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                           const std::string& indexName, const std::string& truncIndexName);

public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, IndexAccessoryReaderTest);

void IndexAccessoryReaderTest::setUp() {}

void IndexAccessoryReaderTest::tearDown() {}

std::pair<Status, std::shared_ptr<indexlibv2::framework::TabletData>> IndexAccessoryReaderTest::CreateEmptyTabletData()
{
    auto tabletData = std::make_shared<indexlibv2::framework::TabletData>(identifier);
    std::shared_ptr<indexlibv2::framework::ResourceMap> map(new indexlibv2::framework::ResourceMap());
    auto schemaGroup = std::make_shared<indexlibv2::framework::TabletDataSchemaGroup>();
    std::shared_ptr<indexlibv2::config::ITabletSchema> schema(new indexlibv2::config::TabletSchema());
    schemaGroup->writeSchema = schema;
    schemaGroup->onDiskWriteSchema = schema;
    schemaGroup->onDiskReadSchema = schema;
    auto status = map->AddVersionResource(indexlibv2::framework::TabletDataSchemaGroup::NAME, schemaGroup);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "add schema to resource std::map failed");

    status = tabletData->Init(/*invalid version*/ indexlibv2::framework::Version(), {}, map);
    if (!status.IsOK()) {
        return std::make_pair(status, nullptr);
    }
    return std::make_pair(Status::OK(), tabletData);
}

std::vector<std::shared_ptr<IIndexConfig>> IndexAccessoryReaderTest::CreateDefaultConfigs()
{
    auto schema = indexlibv2::table::NormalTabletSchemaMaker::Make(
        "text1:text;text2:text;string1:string;long1:long", // field schema
        "pack1:pack:text1,text2;pk:primarykey64:string1;", // index schema
        "string1;long1",                                   // attribute schema
        "string1");                                        // summary schema
    auto indexConfigs = schema->GetIndexConfigs(INVERTED_INDEX_TYPE_STR);
    auto truncIndexConfig = CreateTruncIndexConfig(schema, "pack1", "truncate_pack1");
    indexConfigs.emplace_back(truncIndexConfig);
    return indexConfigs;
}

std::shared_ptr<IIndexConfig>
IndexAccessoryReaderTest::CreateTruncIndexConfig(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                                 const std::string& indexName, const std::string& truncIndexName)
{
    auto indexConfig =
        std::dynamic_pointer_cast<InvertedIndexConfig>(schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, indexName));
    assert(indexConfig);

    auto truncIndexConfig = std::shared_ptr<InvertedIndexConfig>(indexConfig->Clone());
    assert(truncIndexConfig != NULL);

    truncIndexConfig->SetIndexName(truncIndexName);
    truncIndexConfig->SetVirtual(true);
    truncIndexConfig->SetNonTruncateIndexName(indexName);
    return truncIndexConfig;
}

TEST_F(IndexAccessoryReaderTest, TestCaseForOpen)
{
    auto [status, tabletData] = CreateEmptyTabletData();
    ASSERT_TRUE(status.IsOK());
    auto indexConfigs = CreateDefaultConfigs();

    auto accessoryReader = std::make_shared<IndexAccessoryReader>(indexConfigs, IndexReaderParameter {});
    ASSERT_TRUE(accessoryReader->Open(tabletData.get()).IsOK());
    ASSERT_EQ(accessoryReader->_sectionReaderMap.size(), (size_t)2);
    ASSERT_EQ(accessoryReader->_sectionReaderVec.size(), (size_t)1);

    // no valid index accessory
    auto newSchema = indexlibv2::table::NormalTabletSchemaMaker::Make(
        "text1:text;text2:text;string1:string;long1:long", // above is field schema
        "pack1:pack:text1,text2;pk:primarykey64:string1;", // above is index schema
        "string1;long1",                                   // above is attribute schema
        "string1");                                        // above is summary schema

    auto packIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(
        newSchema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, "pack1"));
    assert(packIndexConfig != NULL);
    packIndexConfig->SetHasSectionAttributeFlag(false);
    auto newIndexConfigs = newSchema->GetIndexConfigs(INVERTED_INDEX_TYPE_STR);

    newIndexConfigs.emplace_back(CreateTruncIndexConfig(newSchema, "pack1", "truncate_pack1"));
    accessoryReader.reset(new IndexAccessoryReader(newIndexConfigs, IndexReaderParameter {}));
    // TODO: now we return true
    ASSERT_TRUE(accessoryReader->Open(tabletData.get()).IsOK());
    ASSERT_EQ(accessoryReader->_sectionReaderMap.size(), (size_t)0);
    ASSERT_EQ(accessoryReader->_sectionReaderVec.size(), (size_t)0);
}

TEST_F(IndexAccessoryReaderTest, TestCaseForClone)
{
    auto [status, tabletData] = CreateEmptyTabletData();
    ASSERT_TRUE(status.IsOK());
    auto indexConfigs = CreateDefaultConfigs();

    auto accessoryReader = std::make_shared<IndexAccessoryReader>(indexConfigs, IndexReaderParameter {});
    ASSERT_TRUE(accessoryReader->Open(tabletData.get()).IsOK());

    auto cloneReader = std::shared_ptr<IndexAccessoryReader>(accessoryReader->Clone());
    ASSERT_TRUE(cloneReader != NULL);
    ASSERT_TRUE(accessoryReader->_sectionReaderVec != cloneReader->_sectionReaderVec);
    ASSERT_TRUE(accessoryReader->_sectionReaderMap == cloneReader->_sectionReaderMap);
}

TEST_F(IndexAccessoryReaderTest, TestCaseForGetSectionReader)
{
    auto [status, tabletData] = CreateEmptyTabletData();
    ASSERT_TRUE(status.IsOK());
    auto indexConfigs = CreateDefaultConfigs();

    auto accessoryReader = std::make_shared<IndexAccessoryReader>(indexConfigs, IndexReaderParameter {});
    ASSERT_TRUE(accessoryReader->Open(tabletData.get()).IsOK());

    ASSERT_TRUE(accessoryReader->GetSectionReader("pack1") != NULL);
    ASSERT_TRUE(accessoryReader->GetSectionReader("pk") == NULL);
    ASSERT_EQ(accessoryReader->GetSectionReader("pack1"), accessoryReader->GetSectionReader("truncate_pack1"));
}

} // namespace indexlib::index
