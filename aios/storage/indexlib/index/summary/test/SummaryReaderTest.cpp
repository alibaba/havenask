#include "indexlib/index/summary/SummaryReader.h"

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/document/normal/SerializedSummaryDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/attribute/SingleValueAttributeReader.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/LocalDiskSummaryDiskIndexer.h"
#include "indexlib/index/summary/SummaryMemIndexer.h"
#include "indexlib/index/summary/SummaryMemReaderContainer.h"
#include "indexlib/index/summary/SummaryReader.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/index/summary/test/SummaryMaker.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace indexlibv2::config;

namespace indexlibv2::index {

class FakeInt32AttributeReader : public SingleValueAttributeReader<int32_t>
{
public:
    FakeInt32AttributeReader(indexlibv2::config::SortPattern sortType) : SingleValueAttributeReader<int32_t>(sortType)
    {
    }
    ~FakeInt32AttributeReader() = default;

public:
    bool Read(docid_t docId, std::string& value, autil::mem_pool::Pool* pool) const override
    {
        std::stringstream ss;
        ss << docId;
        value = ss.str();
        return true;
    }
};

class FakeFloatAttributeReader : public SingleValueAttributeReader<float>
{
public:
    FakeFloatAttributeReader(indexlibv2::config::SortPattern sortType) : SingleValueAttributeReader<float>(sortType) {}
    ~FakeFloatAttributeReader() = default;

public:
    bool Read(docid_t docId, std::string& value, autil::mem_pool::Pool* pool) const override
    {
        std::stringstream ss;
        ss << docId << ".0";
        value = ss.str();
        return true;
    }
};

class SummaryReaderTest : public TESTBASE
{
public:
    SummaryReaderTest() = default;
    ~SummaryReaderTest() = default;

    using DocumentArray = std::vector<std::shared_ptr<indexlib::document::SummaryDocument>>;
    using AttrReaderMap = std::map<fieldid_t, std::shared_ptr<AttributeReader>>;

public:
    void setUp() override;
    void tearDown() override;

private:
    void CheckSearchDocument(std::shared_ptr<indexlib::document::SearchSummaryDocument> gotDoc,
                             std::shared_ptr<indexlib::document::SummaryDocument> expectDoc);
    void FullBuild(const std::vector<uint32_t>& docCountInSegs, autil::mem_pool::Pool* pool,
                   DocumentArray& answerDocArray);
    void CheckSummaryReader(const std::shared_ptr<SummaryReader>& summaryReader, const DocumentArray& answerDocArray,
                            std::shared_ptr<AttrReaderMap> attrReaderMap);
    bool FloatEqual(float a, float b);
    void TestOpen(const std::vector<uint32_t>& fullBuildDocCounts, std::shared_ptr<AttrReaderMap> attrReaderMap);
    void TestForOneSegment(bool compress);
    void TestForMultiSegments(bool compress);
    std::shared_ptr<SummaryReader> CreateSummaryReader(const std::vector<uint32_t>& fullBuildDocCounts,
                                                       std::shared_ptr<AttrReaderMap>& attrReaderMap);
    std::shared_ptr<AttrReaderMap> MakeAttrReaderMap();
    void AddAttrReadersToSummaryReader(std::shared_ptr<AttrReaderMap> attrReaderMap,
                                       std::shared_ptr<SummaryReader>& summaryReader);
    std::shared_ptr<indexlib::file_system::IDirectory> GetSummaryDirectory(segmentid_t segId);
    std::shared_ptr<indexlib::file_system::IDirectory> MakeSegmentDirectory(segmentid_t segId);
    void ResetRootDirectory(std::string loadStrategyName);
    std::shared_ptr<SummaryIndexConfig> GetSummaryIndexConfig();

private:
    std::shared_ptr<config::ITabletSchema> _schema;
    fieldid_t _updatableIntFieldId;
    fieldid_t _updatableFloatFieldId;
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
};

void SummaryReaderTest::setUp()
{
    _schema = table::NormalTabletSchemaMaker::Make(
        /* field */ "title:text;user_name:string;user_id:integer;price:float", /* index */ "pk:primarykey64:user_id",
        /* attribute */ "",
        /* summary */ "title;user_name;user_id;price");

    _updatableIntFieldId = 2;   // user_id
    _updatableFloatFieldId = 3; // price
    _rootPath = GET_TEMP_DATA_PATH() + "/";
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.needFlush = true;

    indexlib::file_system::LoadConfigList loadConfigList;
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
    fsOptions.loadConfigList = loadConfigList;

    auto fs = indexlib::file_system::FileSystemCreator::Create("online", _rootPath, fsOptions).GetOrThrow();
    auto directory = indexlib::file_system::Directory::Get(fs);
    assert(directory);
    _rootDir = directory->GetIDirectory();
}

void SummaryReaderTest::tearDown() {}

bool SummaryReaderTest::FloatEqual(float a, float b) { return fabs(a - b) < 1E-6; }

void SummaryReaderTest::ResetRootDirectory(std::string loadStrategyName)
{
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.needFlush = true;

    indexlib::file_system::LoadConfigList loadConfigList;
    if (loadStrategyName == indexlib::file_system::READ_MODE_CACHE) {
        auto loadConfig = indexlib::file_system::LoadConfigListCreator::MakeBlockLoadConfig(
            "lru", 1000, 100, indexlib::file_system::CacheLoadStrategy::DEFAULT_CACHE_IO_BATCH_SIZE);
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
    auto directory = indexlib::file_system::Directory::Get(fs);
    assert(directory);
    _rootDir = directory->GetIDirectory();
}

std::shared_ptr<indexlib::file_system::IDirectory> SummaryReaderTest::MakeSegmentDirectory(segmentid_t segId)
{
    auto [status, directory] =
        _rootDir
            ->MakeDirectory(std::string("segment_") + autil::StringUtil::toString(segId) + "_level_0",
                            indexlib::file_system::DirectoryOption::Mem())
            .StatusWith();
    if (status.IsOK()) {
        return directory;
    }
    return nullptr;
}

std::shared_ptr<indexlib::file_system::IDirectory> SummaryReaderTest::GetSummaryDirectory(segmentid_t segId)
{
    auto [status, directory] =
        _rootDir->GetDirectory(std::string("segment_") + autil::StringUtil::toString(segId) + "_level_0").StatusWith();
    if (status.IsOK()) {
        auto [subStatus, summaryDir] = directory->GetDirectory(SUMMARY_INDEX_PATH).StatusWith();
        if (subStatus.IsOK()) {
            return summaryDir;
        }
    }
    return nullptr;
}

void SummaryReaderTest::FullBuild(const std::vector<uint32_t>& docCountInSegs, autil::mem_pool::Pool* pool,
                                  SummaryReaderTest::DocumentArray& answerDocArray)
{
    for (size_t i = 0; i < docCountInSegs.size(); ++i) {
        auto segDirectory = MakeSegmentDirectory(i);
        ASSERT_TRUE(segDirectory != nullptr);
        [[maybe_unused]] auto status =
            SummaryMaker::BuildOneSegment(segDirectory, i, _schema, docCountInSegs[i], pool, answerDocArray);
        assert(status.IsOK());
    }
}
std::shared_ptr<SummaryIndexConfig> SummaryReaderTest::GetSummaryIndexConfig()
{
    auto summaryConf = std::dynamic_pointer_cast<config::SummaryIndexConfig>(
        _schema->GetIndexConfig(SUMMARY_INDEX_TYPE_STR, SUMMARY_INDEX_NAME));
    assert(summaryConf);
    return summaryConf;
}

std::shared_ptr<SummaryReader>
SummaryReaderTest::CreateSummaryReader(const std::vector<uint32_t>& fullBuildDocCounts,
                                       std::shared_ptr<SummaryReaderTest::AttrReaderMap>& attrReaderMap)
{
    std::shared_ptr<SummaryReader> summaryReader = std::make_shared<SummaryReader>();
    auto summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(
        _schema->GetIndexConfig(SUMMARY_INDEX_TYPE_STR, SUMMARY_INDEX_NAME));
    assert(summaryIndexConfig);

    std::vector<SummaryReader::IndexerInfo> indexerInfo;
    for (size_t i = 0; i < fullBuildDocCounts.size(); i++) {
        DiskIndexerParameter indexerParam;
        indexerParam.docCount = fullBuildDocCounts[i];
        auto summaryDir = GetSummaryDirectory(i);
        assert(summaryDir != nullptr);
        auto diskIndexer = std::make_shared<SummaryDiskIndexer>(indexerParam);
        assert(diskIndexer != nullptr);
        auto status = diskIndexer->Open(summaryIndexConfig, summaryDir);
        if (status.IsOK()) {
            indexerInfo.emplace_back(diskIndexer, (segmentid_t)i, framework::Segment::SegmentStatus::ST_BUILT,
                                     fullBuildDocCounts[i]);
        } else {
            return nullptr;
        }
    }
    auto status = summaryReader->DoOpen(summaryIndexConfig, indexerInfo);
    if (!status.IsOK()) {
        return nullptr;
    }
    AddAttrReadersToSummaryReader(attrReaderMap, summaryReader);
    return summaryReader;
}

std::shared_ptr<SummaryReaderTest::AttrReaderMap> SummaryReaderTest::MakeAttrReaderMap()
{
    auto attrReaderMap = std::make_shared<SummaryReaderTest::AttrReaderMap>();

    auto attrReader = std::make_shared<FakeInt32AttributeReader>(config::sp_nosort);
    attrReaderMap->insert(make_pair(_updatableIntFieldId, attrReader));

    auto floatAttrReader = std::make_shared<FakeFloatAttributeReader>(config::sp_nosort);
    attrReaderMap->insert(make_pair(_updatableFloatFieldId, floatAttrReader));

    return attrReaderMap;
}

void SummaryReaderTest::AddAttrReadersToSummaryReader(std::shared_ptr<SummaryReaderTest::AttrReaderMap> attrReaderMap,
                                                      std::shared_ptr<SummaryReader>& summaryReader)
{
    if (attrReaderMap) {
        SummaryReaderTest::AttrReaderMap::const_iterator it;
        for (it = attrReaderMap->begin(); it != attrReaderMap->end(); ++it) {
            summaryReader->AddAttrReader(it->first, it->second.get());
        }
    }
}

void SummaryReaderTest::CheckSearchDocument(std::shared_ptr<indexlib::document::SearchSummaryDocument> gotDoc,
                                            std::shared_ptr<indexlib::document::SummaryDocument> expectDoc)
{
    ASSERT_EQ(expectDoc->GetNotEmptyFieldCount(), gotDoc->GetFieldCount());

    for (uint32_t j = 0; j < gotDoc->GetFieldCount(); ++j) {
        autil::StringView constStr = expectDoc->GetField((fieldid_t)j);
        std::string expectField(constStr.data(), constStr.size());

        const autil::StringView* str = gotDoc->GetFieldValue((summaryfieldid_t)j);
        std::string field(str->data(), str->size());
        ASSERT_EQ(expectField, field);
    }
}

void SummaryReaderTest::CheckSummaryReader(const std::shared_ptr<SummaryReader>& summaryReader,
                                           const SummaryReaderTest::DocumentArray& answerDocArray,
                                           std::shared_ptr<SummaryReaderTest::AttrReaderMap> attrReaderMap)
{
    for (size_t i = 0; i < answerDocArray.size(); ++i) {
        std::shared_ptr<indexlib::document::SummaryDocument> answerDoc = answerDocArray[i];

        if (answerDoc.get() == NULL) {
            std::shared_ptr<indexlib::document::SearchSummaryDocument> nullDoc(
                new indexlib::document::SearchSummaryDocument(NULL, GetSummaryIndexConfig()->GetSummaryCount()));
            auto [status, ret] = summaryReader->GetDocument(i, nullDoc.get());
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(!ret);
            continue;
        }

        std::shared_ptr<indexlib::document::SearchSummaryDocument> gotDoc(
            new indexlib::document::SearchSummaryDocument(NULL, GetSummaryIndexConfig()->GetSummaryCount()));
        auto [status, ret] = summaryReader->GetDocument(i, gotDoc.get());
        ASSERT_TRUE(status.IsOK());

        ASSERT_EQ(answerDoc->GetNotEmptyFieldCount(), gotDoc->GetFieldCount());

        for (uint32_t j = 0; j < gotDoc->GetFieldCount(); ++j) {
            auto constStr = answerDoc->GetField((fieldid_t)j);
            std::string expectField(constStr.data(), constStr.size());

            if (attrReaderMap) {
                AttrReaderMap::const_iterator it = attrReaderMap->find((fieldid_t)j);
                if (it != attrReaderMap->end()) {
                    it->second->Read(answerDoc->GetDocId(), expectField, nullptr);
                }
            }

            const autil::StringView* str = gotDoc->GetFieldValue((summaryfieldid_t)j);
            std::string field(str->data(), str->size());
            ASSERT_EQ(expectField, field);
        }
    }
}

void SummaryReaderTest::TestOpen(const std::vector<uint32_t>& fullBuildDocCounts,
                                 std::shared_ptr<SummaryReaderTest::AttrReaderMap> attrReaderMap)
{
    tearDown();
    setUp();
    autil::mem_pool::Pool pool;
    SummaryReaderTest::DocumentArray answerDocArray;
    FullBuild(fullBuildDocCounts, &pool, answerDocArray);
    auto summaryReader = CreateSummaryReader(fullBuildDocCounts, attrReaderMap);
    CheckSummaryReader(summaryReader, answerDocArray, attrReaderMap);
}

void SummaryReaderTest::TestForOneSegment(bool compress)
{
    std::vector<uint32_t> docCountInSegs;
    docCountInSegs.push_back(10);
    TestOpen(docCountInSegs, nullptr);

    auto attrReaderMap = MakeAttrReaderMap();
    TestOpen(docCountInSegs, attrReaderMap);
}

void SummaryReaderTest::TestForMultiSegments(bool compress)
{
    std::vector<uint32_t> docCountInSegs;
    docCountInSegs.push_back(10);
    docCountInSegs.push_back(5);
    docCountInSegs.push_back(15);
    docCountInSegs.push_back(27);
    TestOpen(docCountInSegs, nullptr);

    auto attrReaderMap = MakeAttrReaderMap();
    TestOpen(docCountInSegs, attrReaderMap);
}

TEST_F(SummaryReaderTest, TestCaseForOneSegment)
{
    // no compress
    TestForOneSegment(false);
    // using compress
    TestForOneSegment(true);
}

TEST_F(SummaryReaderTest, TestCaseForMultiSegments)
{
    // no compress
    TestForMultiSegments(false);
    // using compress
    TestForMultiSegments(true);
}

TEST_F(SummaryReaderTest, TestCaseForOpen)
{
    std::vector<uint32_t> fullBuildDocCounts;
    fullBuildDocCounts.push_back(10);
    autil::mem_pool::Pool pool;
    SummaryReaderTest::DocumentArray answerDocArray;
    FullBuild(fullBuildDocCounts, &pool, answerDocArray);
    std::shared_ptr<SummaryReaderTest::AttrReaderMap> readerMap;
    {
        // test case for need store summary true
        auto summaryReader = CreateSummaryReader(fullBuildDocCounts, readerMap);
        auto diskIndexer = summaryReader->_diskIndexers[0];
        ASSERT_EQ((size_t)1, diskIndexer->_summaryGroups.size());
        ASSERT_TRUE(diskIndexer->_summaryGroups[0]->_dataReader);
    }
    {
        // test case for need store summary false
        auto summaryConf = GetSummaryIndexConfig();
        summaryConf->SetNeedStoreSummary(false);
        auto summaryReader = CreateSummaryReader(fullBuildDocCounts, readerMap);
        auto diskIndexer = summaryReader->_diskIndexers[0];
        ASSERT_EQ((size_t)0, diskIndexer->_summaryGroups.size());
        summaryConf->SetNeedStoreSummary(true);
    }
}

TEST_F(SummaryReaderTest, TestCaseForInvalidDocId)
{
    std::vector<uint32_t> fullBuildDocCounts;
    fullBuildDocCounts.push_back(10);

    SummaryReaderTest::DocumentArray answerDocArray;
    autil::mem_pool::Pool pool;
    FullBuild(fullBuildDocCounts, &pool, answerDocArray);

    auto attrReaderMap = MakeAttrReaderMap();

    indexlib::document::SearchSummaryDocument document(NULL, 100);
    auto summaryReader = CreateSummaryReader(fullBuildDocCounts, attrReaderMap);
    auto [status, ret] = summaryReader->GetDocument(INVALID_DOCID, &document);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(!ret);
}

TEST_F(SummaryReaderTest, TestCaseForInvalidDocIdInCache)
{
    std::vector<uint32_t> fullBuildDocCounts;
    fullBuildDocCounts.push_back(10);

    ResetRootDirectory(indexlib::file_system::READ_MODE_CACHE);
    SummaryReaderTest::DocumentArray answerDocArray;
    autil::mem_pool::Pool pool;
    FullBuild(fullBuildDocCounts, &pool, answerDocArray);

    auto attrReaderMap = MakeAttrReaderMap();

    auto summaryReader = CreateSummaryReader(fullBuildDocCounts, attrReaderMap);

    indexlib::document::SearchSummaryDocument document(NULL, 100);
    auto [status, ret] = summaryReader->GetDocument(INVALID_DOCID, &document);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(!ret);

    std::tie(status, ret) = summaryReader->GetDocument(-100, &document);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(!ret);
}

TEST_F(SummaryReaderTest, TestCaseForGetDocumentFromSummary)
{
    autil::mem_pool::Pool pool;
    SummaryReaderTest::DocumentArray answerDocArray;

    std::vector<uint32_t> fullBuildDocCounts;
    fullBuildDocCounts.push_back(1);
    fullBuildDocCounts.push_back(2);
    FullBuild(fullBuildDocCounts, &pool, answerDocArray);

    std::shared_ptr<SummaryReaderTest::AttrReaderMap> readerMap;
    auto summaryReader = CreateSummaryReader(fullBuildDocCounts, readerMap);
    auto summaryDoc = std::make_shared<indexlib::document::SearchSummaryDocument>(
        nullptr, GetSummaryIndexConfig()->GetSummaryCount());
    {
        // test for docid < 0
        auto [status, ret] = summaryReader->GetDocument((docid_t)-1, summaryDoc.get());
        ASSERT_FALSE(ret);
    }

    {
        // test for needStoreSummary = false
        summaryReader->_summaryIndexConfig->SetNeedStoreSummary(false);
        summaryReader->_needStoreSummary = false;
        auto [status, ret] = summaryReader->GetDocument((docid_t)100, summaryDoc.get());
        ASSERT_TRUE(status.IsOK() && ret);

        summaryReader->_summaryIndexConfig->SetNeedStoreSummary(true);
        summaryReader->_needStoreSummary = true;
    }

    {
        // test for get doc from on disk segment reader
        CheckSummaryReader(summaryReader, answerDocArray, nullptr);
    }

    // TODO: add open unit test
    {
        // test for get doc from in memory segment reader
        SummaryReaderTest::DocumentArray inMemDocArray;
        auto memIndexer = SummaryMaker::BuildOneSegmentWithoutDump((uint32_t)1, _schema, &pool, inMemDocArray);

        auto memReaderContainer = memIndexer->CreateInMemReader();
        summaryReader->_buildingSummaryMemReaders.emplace_back(std::make_pair(3, memReaderContainer));

        auto [status, ret] = summaryReader->GetDocument(0, summaryDoc.get());
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(ret);
        CheckSearchDocument(summaryDoc, answerDocArray[0]);

        // test in mem doc
        std::tie(status, ret) = summaryReader->GetDocument(3, summaryDoc.get());
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(ret);
        CheckSearchDocument(summaryDoc, inMemDocArray[0]);

        std::tie(status, ret) = summaryReader->GetDocument(4, summaryDoc.get());
        ASSERT_FALSE(ret);
        // test for docid too large
        std::tie(status, ret) = summaryReader->GetDocument((docid_t)100, summaryDoc.get());
        ASSERT_FALSE(ret);
    }
}

} // namespace indexlibv2::index
