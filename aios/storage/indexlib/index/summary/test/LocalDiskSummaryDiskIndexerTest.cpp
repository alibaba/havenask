#include "indexlib/index/summary/LocalDiskSummaryDiskIndexer.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/common/data_structure/VarLenDataReader.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/index/summary/test/SummaryMaker.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace indexlibv2::config;

namespace indexlibv2::index {

class LocalDiskSummaryDiskIndexerTest : public TESTBASE
{
public:
    LocalDiskSummaryDiskIndexerTest() = default;
    ~LocalDiskSummaryDiskIndexerTest() = default;

public:
    void setUp() override;
    void tearDown() override;

    void TestLeafDiskIndexer(bool compress, uint32_t docCount, bool isMmap);
    void CheckReadSummaryDocument(const std::shared_ptr<LocalDiskSummaryDiskIndexer>& leafDiskIndexer,
                                  const SummaryMaker::DocumentArray& answerDocArray);
    std::shared_ptr<indexlib::file_system::IDirectory> GetSummaryDirectory(bool isMmap);
    std::shared_ptr<SummaryIndexConfig> GetSummaryIndexConfig(const std::shared_ptr<ITabletSchema>& schema);

private:
    std::string _rootPath;
    std::shared_ptr<config::ITabletSchema> _schema;
    autil::mem_pool::Pool _pool;
};

void LocalDiskSummaryDiskIndexerTest::setUp()
{
    _schema = table::NormalTabletSchemaMaker::Make(
        /* field */ "title:text;user_name:string;user_id:integer;price:float",
        /* index */ "pk:primarykey64:user_id", /* attribute*/ "", /* summary */ "title;user_name;user_id;price");
    _rootPath = GET_TEMP_DATA_PATH() + "/";
}

void LocalDiskSummaryDiskIndexerTest::tearDown() {}

std::shared_ptr<indexlib::file_system::IDirectory> LocalDiskSummaryDiskIndexerTest::GetSummaryDirectory(bool isMmap)
{
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.needFlush = true;

    indexlib::file_system::LoadConfigList loadConfigList;
    if (!isMmap) {
        indexlib::file_system::LoadConfig loadConfig =
            indexlib::file_system::LoadConfigListCreator::MakeBlockLoadConfig("lru", 10, 1, 4);
        loadConfigList.PushBack(loadConfig);
    } else {
        auto loadStrategy =
            std::make_shared<indexlib::file_system::MmapLoadStrategy>(/*lock*/ true, false, 4 * 1024 * 1024, 0);
        indexlib::file_system::LoadConfig::FilePatternStringVector pattern;
        pattern.push_back(".*");

        indexlib::file_system::LoadConfig loadConfig;
        loadConfig.SetLoadStrategyPtr(loadStrategy);
        loadConfig.SetFilePatternString(pattern);
        loadConfig.SetLoadStrategyPtr(loadStrategy);
        loadConfig.SetName("__OFFLINE_SUMMARY__");
        loadConfigList.PushFront(loadConfig);
    }
    fsOptions.loadConfigList = loadConfigList;

    auto fs = indexlib::file_system::FileSystemCreator::Create("online", _rootPath, fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fs);
    assert(rootDir != nullptr);
    auto rootIDir = rootDir->GetIDirectory();
    assert(rootIDir != nullptr);
    return rootIDir;
}

void LocalDiskSummaryDiskIndexerTest::TestLeafDiskIndexer(bool compress, uint32_t docCount, bool isMmap)
{
    tearDown();
    setUp();
    auto summaryGroupConfig = GetSummaryIndexConfig(_schema)->GetSummaryGroupConfig(0);
    summaryGroupConfig->SetCompress(compress, "");
    SummaryMaker::DocumentArray answerDocArray;
    autil::mem_pool::Pool pool;
    auto rootDir = GetSummaryDirectory(isMmap);
    auto status = SummaryMaker::BuildOneSegment(rootDir, 0, _schema, docCount, &pool, answerDocArray);
    ASSERT_TRUE(status.IsOK());

    DiskIndexerParameter indexerParam;
    indexerParam.docCount = docCount;
    auto summaryLeafDiskIndexer = std::make_shared<LocalDiskSummaryDiskIndexer>(indexerParam);

    auto [dirStatus, summaryDir] = rootDir->GetDirectory(SUMMARY_INDEX_PATH).StatusWith();
    assert(dirStatus.IsOK());
    ASSERT_TRUE(summaryLeafDiskIndexer->Open(summaryGroupConfig, summaryDir).IsOK());
    auto dataFileNode = summaryLeafDiskIndexer->_dataReader->GetDataFileReader()->GetFileNode();
    auto offsetFileNode = summaryLeafDiskIndexer->_dataReader->GetOffsetFileReader()->GetFileNode();

    if (isMmap) {
        ASSERT_EQ(indexlib::file_system::FSFT_MMAP_LOCK, dataFileNode->GetType());
        ASSERT_EQ(indexlib::file_system::FSFT_MMAP_LOCK, offsetFileNode->GetType());
        ASSERT_EQ(indexlib::file_system::FSOT_LOAD_CONFIG,
                  summaryLeafDiskIndexer->_dataReader->GetDataFileReader()->GetOpenType());
        ASSERT_EQ(indexlib::file_system::FSOT_LOAD_CONFIG,
                  summaryLeafDiskIndexer->_dataReader->GetOffsetFileReader()->GetOpenType());
    } else {
        ASSERT_EQ(indexlib::file_system::FSFT_BLOCK, dataFileNode->GetType());
        ASSERT_EQ(indexlib::file_system::FSFT_BLOCK, offsetFileNode->GetType());
        ASSERT_EQ(indexlib::file_system::FSOT_LOAD_CONFIG,
                  summaryLeafDiskIndexer->_dataReader->GetDataFileReader()->GetOpenType());
        ASSERT_EQ(indexlib::file_system::FSOT_LOAD_CONFIG,
                  summaryLeafDiskIndexer->_dataReader->GetOffsetFileReader()->GetOpenType());
    }
    CheckReadSummaryDocument(summaryLeafDiskIndexer, answerDocArray);
}

std::shared_ptr<SummaryIndexConfig>
LocalDiskSummaryDiskIndexerTest::GetSummaryIndexConfig(const std::shared_ptr<ITabletSchema>& schema)
{
    auto summaryConf = std::dynamic_pointer_cast<SummaryIndexConfig>(
        schema->GetIndexConfig(SUMMARY_INDEX_TYPE_STR, SUMMARY_INDEX_NAME));
    assert(summaryConf);
    return summaryConf;
}

void LocalDiskSummaryDiskIndexerTest::CheckReadSummaryDocument(
    const std::shared_ptr<LocalDiskSummaryDiskIndexer>& leafDiskIndexer,
    const SummaryMaker::DocumentArray& answerDocArray)
{
    for (size_t i = 0; i < answerDocArray.size(); ++i) {
        auto answerDoc = answerDocArray[i];
        const auto summaryConf = GetSummaryIndexConfig(_schema);
        auto gotDoc =
            std::make_shared<indexlib::document::SearchSummaryDocument>(nullptr, summaryConf->GetSummaryCount());
        leafDiskIndexer->GetDocument(i, gotDoc.get());

        ASSERT_EQ(answerDoc->GetNotEmptyFieldCount(), gotDoc->GetFieldCount());

        for (uint32_t j = 0; j < answerDoc->GetNotEmptyFieldCount(); ++j) {
            const autil::StringView& expectField = answerDoc->GetField((fieldid_t)j);
            auto summaryFieldId = summaryConf->GetSummaryFieldId((fieldid_t)j);
            const autil::StringView* str = gotDoc->GetFieldValue(summaryFieldId);
            ASSERT_TRUE(str != NULL);
            ASSERT_EQ(expectField, *str);
        }
    }
}

TEST_F(LocalDiskSummaryDiskIndexerTest, TestCaseForRead)
{
    // no compress
    TestLeafDiskIndexer(false, 2000, false);
    TestLeafDiskIndexer(false, 2000, true);
    // using compress
    TestLeafDiskIndexer(true, 2000, false);
    TestLeafDiskIndexer(true, 2000, true);
}

TEST_F(LocalDiskSummaryDiskIndexerTest, TestGetSummaryVarLenDataReader)
{
    bool compress = false;
    uint32_t docCount = 100;
    bool isMmap = false;
    auto summaryGroupConfig = GetSummaryIndexConfig(_schema)->GetSummaryGroupConfig(0);
    summaryGroupConfig->SetCompress(compress, "");
    SummaryMaker::DocumentArray answerDocArray;
    autil::mem_pool::Pool pool;
    auto rootDir = GetSummaryDirectory(isMmap);
    auto status = SummaryMaker::BuildOneSegment(rootDir, 0, _schema, docCount, &pool, answerDocArray);
    ASSERT_TRUE(status.IsOK());

    DiskIndexerParameter indexerParam;
    indexerParam.docCount = docCount;
    auto summaryLeafDiskIndexer = std::make_shared<LocalDiskSummaryDiskIndexer>(indexerParam);

    auto [dirStatus, summaryDir] = rootDir->GetDirectory(SUMMARY_INDEX_PATH).StatusWith();
    assert(dirStatus.IsOK());

    ASSERT_TRUE(summaryLeafDiskIndexer->Open(summaryGroupConfig, summaryDir).IsOK());
    ASSERT_TRUE(summaryLeafDiskIndexer->_dataReader->_isOnline);

    auto [st, reader] =
        LocalDiskSummaryDiskIndexer::GetSummaryVarLenDataReader(docCount, summaryGroupConfig, summaryDir, false);
    ASSERT_TRUE(st.IsOK());
    ASSERT_FALSE(reader->_isOnline);
}
} // namespace indexlibv2::index
