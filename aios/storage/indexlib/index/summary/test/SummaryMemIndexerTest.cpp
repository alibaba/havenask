#include "indexlib/index/summary/SummaryMemIndexer.h"

#include <random>

#include "autil/StringUtil.h"
#include "autil/ZlibCompressor.h"
#include "autil/mem_pool/Pool.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/SingleFileCompressConfig.h"
#include "indexlib/document/extractor/IDocumentInfoExtractor.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/SerializedSummaryDocument.h"
#include "indexlib/document/normal/SummaryFormatter.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/DocMapDumpParams.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/LocalDiskSummaryMemIndexer.h"
#include "indexlib/index/summary/SummaryDiskIndexer.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "unittest/unittest.h"

using namespace indexlibv2::config;

namespace indexlibv2::index {

class SummaryMemIndexerTest : public TESTBASE
{
public:
    SummaryMemIndexerTest() = default;
    ~SummaryMemIndexerTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    void TestCaseForWrite(bool useCompress);
    void TestCaseForSortDump(bool useCompress);

    std::shared_ptr<config::SummaryIndexConfig>
    GetSummaryIndexConfig(bool useCompress, const std::shared_ptr<config::ITabletSchema>& schema) const;
    std::shared_ptr<SummaryMemIndexer>
    CreateSummaryMemIndexer(const std::shared_ptr<config::SummaryIndexConfig>& summaryIndexConfig);
    void MakeDocuments(uint32_t total, std::shared_ptr<config::ITabletSchema> schema, autil::mem_pool::Pool* pool,
                       bool useCompress, std::vector<std::shared_ptr<indexlib::document::SummaryDocument>>& summaryDocs,
                       std::vector<std::string>& expectedSummaryVector);
    std::string MakeDocKey(docid_t docId);
    std::shared_ptr<indexlib::document::SummaryDocument>
    MakeOneSummaryDocument(docid_t docId, int beginContentId, autil::mem_pool::Pool* pool,
                           const std::shared_ptr<config::ITabletSchema> schema, bool useCompress,
                           std::string& expectedSummaryString);
    std::string Int32ToStr(int32_t value);
    std::shared_ptr<SummaryIndexConfig> GetSummaryIndexConfig(const std::shared_ptr<ITabletSchema>& schema) const;

private:
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
    indexlib::util::SimplePool _pool;
    bool _alreadRegister = false;
    std::unique_ptr<indexlib::util::BuildResourceMetrics> _buildResourceMetrics;
    indexlib::util::BuildResourceMetricsNode* _buildResourceMetricsNode;
    std::shared_ptr<index::BuildingIndexMemoryUseUpdater> _memoryUseUpdater;
    std::shared_ptr<document::extractor::IDocumentInfoExtractorFactory> _docInfoExtractorFactory;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<config::SummaryIndexConfig> _summaryIndexConfig;
};

void SummaryMemIndexerTest::setUp()
{
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.needFlush = true;

    auto loadStrategy =
        std::make_shared<indexlib::file_system::MmapLoadStrategy>(/*lock*/ true, false, 4 * 1024 * 1024, 0);
    indexlib::file_system::LoadConfig::FilePatternStringVector pattern;
    pattern.push_back(".*");

    indexlib::file_system::LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetFilePatternString(pattern);
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetName("__OFFLINE_SUMMARY__");

    indexlib::file_system::LoadConfigList loadConfigList;
    loadConfigList.PushFront(loadConfig);

    fsOptions.loadConfigList = loadConfigList;
    _rootPath = GET_TEMP_DATA_PATH() + "/";
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", _rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(fs);

    if (!_alreadRegister) {
        _buildResourceMetrics = std::make_unique<indexlib::util::BuildResourceMetrics>();
        _buildResourceMetrics->Init();
        _alreadRegister = true;
    }
    _buildResourceMetricsNode = _buildResourceMetrics->AllocateNode();
    _memoryUseUpdater = std::make_shared<index::BuildingIndexMemoryUseUpdater>(_buildResourceMetricsNode);
    _docInfoExtractorFactory = std::make_shared<indexlibv2::plain::DocumentInfoExtractorFactory>();
    _schema = table::NormalTabletSchemaMaker::Make(
        /* field */ "title:text;user_name:string;price:float;user_id:integer",
        /*index*/ "pk:primarykey64:user_id", /*attribute*/ "",
        /*summary*/ "title;user_name;price");
    ASSERT_TRUE(_schema);
    _summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(
        _schema->GetIndexConfig(index::SUMMARY_INDEX_TYPE_STR, SUMMARY_INDEX_NAME));
    ASSERT_TRUE(_summaryIndexConfig);
}

void SummaryMemIndexerTest::tearDown() {}

void SummaryMemIndexerTest::TestCaseForWrite(bool useCompress)
{
    const uint32_t documentCount = 10;

    const auto& summaryIndexConfig = GetSummaryIndexConfig(useCompress, _schema);
    std::shared_ptr<SummaryMemIndexer> summaryMemIndexer = CreateSummaryMemIndexer(summaryIndexConfig);
    std::vector<std::shared_ptr<indexlib::document::SummaryDocument>> summaryDocs;
    autil::mem_pool::Pool pool;
    std::vector<std::string> expectedSummaryStringVec;
    MakeDocuments(documentCount, _schema, &pool, useCompress, summaryDocs, expectedSummaryStringVec);
    for (uint32_t i = 0; i < (uint32_t)documentCount; ++i) {
        std::shared_ptr<indexlib::document::SerializedSummaryDocument> serDoc;
        indexlibv2::document::SummaryFormatter formatter(_summaryIndexConfig);
        ASSERT_TRUE(formatter.SerializeSummaryDoc(summaryDocs[i], serDoc).IsOK());
        auto normalDoc = std::make_shared<indexlibv2::document::NormalDocument>();
        normalDoc->SetSummaryDocument(serDoc);
        ASSERT_TRUE(summaryMemIndexer->AddDocument(normalDoc.get()).IsOK());
    }

    auto directory = _rootDir;
    ASSERT_TRUE(summaryMemIndexer->Dump(&_pool, directory, nullptr).IsOK());

    std::string offsetPath = indexlib::util::PathUtil::JoinPath(_rootPath, SUMMARY_OFFSET_FILE_NAME);
    std::string dataPath = indexlib::util::PathUtil::JoinPath(_rootPath, SUMMARY_DATA_FILE_NAME);

    fslib::FileMeta offsetFileMeta;
    auto err = fslib::fs::FileSystem::getFileMeta(offsetPath, offsetFileMeta);
    ASSERT_EQ(fslib::EC_OK, err);
    ASSERT_EQ(documentCount * sizeof(uint64_t), (uint64_t)offsetFileMeta.fileLength);

    std::unique_ptr<fslib::fs::File> offsetFile(fslib::fs::FileSystem::openFile(offsetPath, fslib::READ));
    uint64_t lastOffset;
    offsetFile->pread((void*)(&lastOffset), sizeof(uint64_t), (documentCount - 1) * sizeof(uint64_t));

    fslib::FileMeta dataFileMeta;
    err = fslib::fs::FileSystem::getFileMeta(dataPath, dataFileMeta);
    ASSERT_EQ(fslib::EC_OK, err);
    ASSERT_TRUE(dataFileMeta.fileLength > (int64_t)lastOffset);
}

void SummaryMemIndexerTest::TestCaseForSortDump(bool useCompress)
{
    const uint32_t documentCount = 5000;

    const auto& summaryIndexConfig = GetSummaryIndexConfig(useCompress, _schema);
    std::shared_ptr<SummaryMemIndexer> summaryMemIndexer = CreateSummaryMemIndexer(summaryIndexConfig);
    std::vector<std::shared_ptr<indexlib::document::SummaryDocument>> summaryDocs;
    autil::mem_pool::Pool pool;
    // create sort dump parameter
    auto params = std::make_shared<DocMapDumpParams>();
    std::vector<std::string> expectedSummaryStringVec;
    MakeDocuments(documentCount, _schema, &pool, useCompress, summaryDocs, expectedSummaryStringVec);
    for (uint32_t i = 0; i < (uint32_t)documentCount; ++i) {
        std::shared_ptr<indexlib::document::SerializedSummaryDocument> serDoc;
        indexlibv2::document::SummaryFormatter formatter(_summaryIndexConfig);
        ASSERT_TRUE(formatter.SerializeSummaryDoc(summaryDocs[i], serDoc).IsOK());
        auto normalDoc = std::make_shared<indexlibv2::document::NormalDocument>();
        normalDoc->SetSummaryDocument(serDoc);
        ASSERT_TRUE(summaryMemIndexer->AddDocument(normalDoc.get()).IsOK());
        params->new2old.push_back(i);
    }

    std::random_device rd;
    std::mt19937 rng(rd());
    std::shuffle(params->new2old.begin(), params->new2old.end(), rng);

    auto directory = _rootDir;
    ASSERT_TRUE(summaryMemIndexer->Dump(&_pool, directory, params).IsOK());

    IndexerParameter indexerParam;
    indexerParam.docCount = documentCount;
    auto diskIndexer = std::make_shared<SummaryDiskIndexer>(indexerParam);
    ASSERT_TRUE(diskIndexer->Open(summaryIndexConfig, directory->GetIDirectory()).IsOK());

    for (size_t i = 0; i < documentCount; ++i) {
        auto summaryConf = GetSummaryIndexConfig(_schema);
        auto gotDoc =
            std::make_shared<indexlib::document::SearchSummaryDocument>(nullptr, summaryConf->GetSummaryCount());
        auto [status, ret] = diskIndexer->GetDocument(i, gotDoc.get());
        ASSERT_TRUE(status.IsOK() && ret);

        docid_t oldDocId = params->new2old[i];
        auto answerDoc = summaryDocs[oldDocId];
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

std::shared_ptr<config::SummaryIndexConfig>
SummaryMemIndexerTest::GetSummaryIndexConfig(bool useCompress,
                                             const std::shared_ptr<config::ITabletSchema>& schema) const
{
    auto summaryIndexConfig = GetSummaryIndexConfig(schema);
    assert(summaryIndexConfig);
    auto groupConf = summaryIndexConfig->GetSummaryGroupConfig(DEFAULT_SUMMARYGROUPID);
    assert(groupConf);
    groupConf->SetCompress(useCompress, "");
    return summaryIndexConfig;
}

std::shared_ptr<SummaryMemIndexer>
SummaryMemIndexerTest::CreateSummaryMemIndexer(const std::shared_ptr<config::SummaryIndexConfig>& summaryIndexConfig)
{
    auto summaryMemIndexer = std::make_shared<SummaryMemIndexer>();
    auto status = summaryMemIndexer->Init(summaryIndexConfig, _docInfoExtractorFactory.get());
    if (!status.IsOK()) {
        return nullptr;
    }
    return summaryMemIndexer;
}

void SummaryMemIndexerTest::MakeDocuments(
    uint32_t total, std::shared_ptr<config::ITabletSchema> schema, autil::mem_pool::Pool* pool, bool useCompress,
    std::vector<std::shared_ptr<indexlib::document::SummaryDocument>>& summaryDocs,
    std::vector<std::string>& expectedSummaryVector)
{
    for (uint32_t i = 0; i < total; ++i) {
        docid_t docId = (docid_t)i;
        int32_t beginContentId = i;
        std::string expectedSummaryString;

        std::shared_ptr<indexlib::document::SummaryDocument> doc(
            MakeOneSummaryDocument(docId, beginContentId, pool, schema, useCompress, expectedSummaryString));
        summaryDocs.push_back(doc);
        expectedSummaryVector.push_back(expectedSummaryString);
    }
}

std::string SummaryMemIndexerTest::MakeDocKey(docid_t docId)
{
    std::string strId;
    autil::StringUtil::serializeUInt64((uint64_t)docId, strId);
    return strId;
}

std::shared_ptr<SummaryIndexConfig>
SummaryMemIndexerTest::GetSummaryIndexConfig(const std::shared_ptr<ITabletSchema>& schema) const
{
    auto summaryConf = std::dynamic_pointer_cast<SummaryIndexConfig>(
        schema->GetIndexConfig(SUMMARY_INDEX_TYPE_STR, SUMMARY_INDEX_NAME));
    assert(summaryConf);
    return summaryConf;
}

std::shared_ptr<indexlib::document::SummaryDocument>
SummaryMemIndexerTest::MakeOneSummaryDocument(docid_t docId, int beginContentId, autil::mem_pool::Pool* pool,
                                              const std::shared_ptr<config::ITabletSchema> schema, bool useCompress,
                                              std::string& expectedSummaryString)
{
    std::shared_ptr<indexlib::document::SummaryDocument> doc(new indexlib::document::SummaryDocument());

    doc->SetDocId(docId);
    expectedSummaryString.clear();
    const auto& fieldConfigs = schema->GetFieldConfigs();
    for (size_t i = 0; i < fieldConfigs.size(); ++i) {
        auto fieldConfig = fieldConfigs[i];
        std::string content = "content_" + Int32ToStr(i + beginContentId);

        if (GetSummaryIndexConfig(schema)->IsInSummary((fieldid_t)i)) {
            doc->SetField(fieldConfig->GetFieldId(), autil::MakeCString(content, pool));

            fieldid_t fieldId = fieldConfig->GetFieldId();
            uint32_t contentSize = (uint32_t)content.size();
            if (contentSize > 0) {
                expectedSummaryString.append((const char*)&fieldId, sizeof(fieldid_t));
                expectedSummaryString.append((const char*)&contentSize, sizeof(contentSize));
                expectedSummaryString.append(content);
            }
        }
    }
    if (useCompress) {
        autil::ZlibCompressor compressor;
        compressor.addDataToBufferIn(expectedSummaryString.c_str(), expectedSummaryString.length());
        compressor.compress();
        expectedSummaryString.clear();

        expectedSummaryString.append(compressor.getBufferOut(), compressor.getBufferOutLen());
    }
    return doc;
}

std::string SummaryMemIndexerTest::Int32ToStr(int32_t value)
{
    std::stringstream stream;
    stream << value;
    return stream.str();
}

TEST_F(SummaryMemIndexerTest, TestCaseForWriteWithOutCompress) { TestCaseForWrite(false); }

TEST_F(SummaryMemIndexerTest, TestCaseForWriteWithCompress) { TestCaseForWrite(true); }

TEST_F(SummaryMemIndexerTest, TestCaseForSortDumpWithOutCompress) { TestCaseForSortDump(false); }

TEST_F(SummaryMemIndexerTest, TestCaseForSortDumpWithCompress) { TestCaseForSortDump(true); }

TEST_F(SummaryMemIndexerTest, TestCaseForUpdateBuildMetrics)
{
    auto schema = table::NormalTabletSchemaMaker::Make(
        /*field*/ "title:text;user_name:string;user_id:integer;price:float", /*index*/ "pk:primarykey64:user_id",
        /*attribute*/ "", /*summary*/ "title;user_name;price");

    auto memIndexer = std::make_shared<LocalDiskSummaryMemIndexer>();
    memIndexer->Init(GetSummaryIndexConfig(schema)->GetSummaryGroupConfig(DEFAULT_SUMMARYGROUPID));

    int64_t allocateSize = memIndexer->_dataWriter->_pool->getUsedBytes();
    memIndexer->UpdateMemUse(_memoryUseUpdater.get());
    int64_t curMemoryUse = _buildResourceMetricsNode->GetValue(indexlib::util::BMT_CURRENT_MEMORY_USE);
    ASSERT_EQ(allocateSize, curMemoryUse);

    // test allocate & update build metrics
    memIndexer->_dataWriter->_pool->allocate(30);
    memIndexer->UpdateMemUse(_memoryUseUpdater.get());
    allocateSize = memIndexer->_dataWriter->_pool->getUsedBytes();
    curMemoryUse = _buildResourceMetricsNode->GetValue(indexlib::util::BMT_CURRENT_MEMORY_USE);
    ASSERT_EQ(allocateSize, curMemoryUse);

    // test allocate & update build metrics over 2G
    memIndexer->_dataWriter->_pool->allocate(1024 * 1024 * 1024);
    memIndexer->_dataWriter->_pool->allocate(1024 * 1024 * 1024);
    memIndexer->UpdateMemUse(_memoryUseUpdater.get());
    allocateSize = memIndexer->_dataWriter->_pool->getUsedBytes();
    curMemoryUse = _buildResourceMetricsNode->GetValue(indexlib::util::BMT_CURRENT_MEMORY_USE);
    ASSERT_EQ(allocateSize, curMemoryUse);
}

TEST_F(SummaryMemIndexerTest, TestFileCompress)
{
    const uint32_t documentCount = 10;

    auto summaryIndexConfig = GetSummaryIndexConfig(false, _schema);
    std::shared_ptr<std::vector<config::SingleFileCompressConfig>> compressConfigs(
        new std::vector<SingleFileCompressConfig>);
    SingleFileCompressConfig simpleCompressConfig("simple", "zstd");

    compressConfigs->push_back(simpleCompressConfig);
    std::shared_ptr<config::FileCompressConfigV2> fileCompressConfig(
        new config::FileCompressConfigV2(compressConfigs, "simple"));
    ASSERT_EQ(1, summaryIndexConfig->GetSummaryGroupConfigCount());
    auto groupConfig = summaryIndexConfig->GetSummaryGroupConfig(0);
    auto currentParam = groupConfig->GetSummaryGroupDataParam();
    currentParam.SetFileCompressConfigV2(fileCompressConfig);
    groupConfig->SetSummaryGroupDataParam(currentParam);
    std::shared_ptr<SummaryMemIndexer> summaryMemIndexer = CreateSummaryMemIndexer(summaryIndexConfig);
    std::vector<std::shared_ptr<indexlib::document::SummaryDocument>> summaryDocs;
    autil::mem_pool::Pool pool;
    std::vector<std::string> expectedSummaryStringVec;
    MakeDocuments(documentCount, _schema, &pool, false, summaryDocs, expectedSummaryStringVec);
    for (uint32_t i = 0; i < (uint32_t)documentCount; ++i) {
        std::shared_ptr<indexlib::document::SerializedSummaryDocument> serDoc;
        indexlibv2::document::SummaryFormatter formatter(_summaryIndexConfig);
        ASSERT_TRUE(formatter.SerializeSummaryDoc(summaryDocs[i], serDoc).IsOK());
        auto normalDoc = std::make_shared<indexlibv2::document::NormalDocument>();
        normalDoc->SetSummaryDocument(serDoc);
        ASSERT_TRUE(summaryMemIndexer->AddDocument(normalDoc.get()).IsOK());
    }

    ASSERT_TRUE(summaryMemIndexer->Dump(&_pool, _rootDir, nullptr).IsOK());
    ASSERT_TRUE(_rootDir->IsExist(SUMMARY_OFFSET_FILE_NAME));
    ASSERT_TRUE(_rootDir->IsExist(SUMMARY_DATA_FILE_NAME));
    ASSERT_TRUE(_rootDir->GetCompressFileInfo(SUMMARY_OFFSET_FILE_NAME));
    ASSERT_TRUE(_rootDir->GetCompressFileInfo(SUMMARY_DATA_FILE_NAME));

    std::string offsetPath = indexlib::util::PathUtil::JoinPath(_rootPath, SUMMARY_OFFSET_FILE_NAME);
    std::string dataPath = indexlib::util::PathUtil::JoinPath(_rootPath, SUMMARY_DATA_FILE_NAME);
}

} // namespace indexlibv2::index
