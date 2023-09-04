#include "indexlib/framework/TabletSchemaLoader.h"

#include <string>

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::framework {

class TabletSchemaLoaderTest : public TESTBASE
{
public:
    TabletSchemaLoaderTest() {}
    ~TabletSchemaLoaderTest() {}

    void setUp() override
    {
        indexlib::file_system::FileSystemOptions fsOptions;
        std::string rootPath = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("online", rootPath, fsOptions).GetOrThrow();
        _rootDir = indexlib::file_system::IDirectory::Get(fs);
    }
    void tearDown() override {}

private:
    void CreateFile(const std::string& fileName, const std::string& content = std::string(""));

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
};

void TabletSchemaLoaderTest::CreateFile(const std::string& fileName, const std::string& content)
{
    auto ret = _rootDir->Store(fileName, content, indexlib::file_system::WriterOption::BufferAtomicDump());
    ASSERT_TRUE(ret.Status().IsOK());
}

TEST_F(TabletSchemaLoaderTest, TestListSchema)
{
    CreateFile("schema.json");
    CreateFile("schema.json.");
    CreateFile("schema.json.10");
    CreateFile("schema.json_15");
    CreateFile("schema.json.20");
    CreateFile("xschema.json.20");

    fslib::FileList fileList;
    ASSERT_TRUE(TabletSchemaLoader::ListSchema(_rootDir, &fileList).IsOK());
    ASSERT_EQ(3, fileList.size());
    ASSERT_EQ(std::string("schema.json"), fileList[0]);
    ASSERT_EQ(std::string("schema.json.10"), fileList[1]);
    ASSERT_EQ(std::string("schema.json.20"), fileList[2]);

    ASSERT_EQ(10, TabletSchemaLoader::GetSchemaId("schema.json.10"));
    ASSERT_EQ(DEFAULT_SCHEMAID, TabletSchemaLoader::GetSchemaId("schema.json"));
    ASSERT_EQ(std::numeric_limits<uint32_t>::max(), TabletSchemaLoader::GetSchemaId("schema.json~"));
}

TEST_F(TabletSchemaLoaderTest, TestGetSchema)
{
    _rootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(GET_PRIVATE_TEST_DATA_PATH());
    ASSERT_FALSE(TabletSchemaLoader::GetSchema(_rootDir, DEFAULT_SCHEMAID)); // file not exist
    ASSERT_FALSE(TabletSchemaLoader::GetSchema(_rootDir, 1));                // file content is invalid

    auto schema = TabletSchemaLoader::GetSchema(_rootDir, 2); // should be correct, table type is mock
    ASSERT_TRUE(schema);
    ASSERT_EQ(2, schema->GetSchemaId());
}

} // namespace indexlibv2::framework
