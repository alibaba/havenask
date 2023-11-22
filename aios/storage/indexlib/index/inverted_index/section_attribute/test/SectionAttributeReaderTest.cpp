#include "indexlib/index/inverted_index/SectionAttributeReader.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/rewriter/SectionAttributeRewriter.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletDataSchemaGroup.h"
#include "indexlib/framework/mock/FakeDiskSegment.h"
#include "indexlib/index/common/field_format/section_attribute/MultiSectionMeta.h"
#include "indexlib/index/common/field_format/section_attribute/SectionAttributeFormatter.h"
#include "indexlib/index/common/field_format/section_attribute/SectionMeta.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/section_attribute/InDocMultiSectionMeta.h"
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeMemIndexer.h"
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeReaderImpl.h"
#include "indexlib/index/inverted_index/section_attribute/test/SectionAttributeTestUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::ITabletSchema;
using indexlibv2::config::sp_nosort;
using indexlibv2::framework::BuildResource;
using indexlibv2::framework::MetricsManager;
using indexlibv2::framework::ResourceMap;
using indexlibv2::framework::Segment;
using indexlibv2::framework::SegmentMeta;
using indexlibv2::framework::TabletData;
using indexlibv2::framework::TabletDataSchemaGroup;
using indexlibv2::framework::Version;
const static std::string identifier = "ut_test";
}; // namespace

#define MAX_SECTION_COUNT_PER_DOC (255 * 32)

class SectionAttributeReaderTest : public TESTBASE
{
public:
    SectionAttributeReaderTest() = default;
    ~SectionAttributeReaderTest() = default;

    using Answer = std::map<docid_t, std::vector<SectionMeta>>;

public:
    void setUp() override;
    void tearDown() override;

private:
    void InnerTestOpen(const std::vector<uint32_t>& fullBuildDocCounts);
    std::pair<Status, std::shared_ptr<TabletData>> FullBuild(const std::shared_ptr<ITabletSchema>& schema,
                                                             const std::vector<uint32_t>& fullBuildDocCounts,
                                                             Answer& answer);
    Status IncBuild(std::shared_ptr<TabletData>& tabletData, const std::shared_ptr<ITabletSchema>& schema,
                    segmentid_t segId, uint32_t docCount, Answer& answer);
    void CheckData(const SectionAttributeReaderImpl& reader, Answer& answer);
    void CheckSectionMetaInOneDoc(const std::vector<SectionMeta>& expectedMeta, const MultiSectionMeta& meta);

private:
    Status BuildMultiSegmentsData(const std::shared_ptr<file_system::Directory>& rootDirectory,
                                  std::shared_ptr<TabletData>& tabletData, const std::shared_ptr<ITabletSchema>& schema,
                                  const std::string& indexName, const std::vector<uint32_t>& docCounts, Answer& answer);

    std::pair<Status, std::shared_ptr<Segment>>
    BuildOneSegmentData(const std::shared_ptr<file_system::Directory>& rootDirectory, segmentid_t segId,
                        const std::shared_ptr<ITabletSchema>& schema, const std::string& indexName, uint32_t docCount,
                        Answer& answer);
    Status AppendSegmentIntoTabletData(std::shared_ptr<TabletData>& tabletData, std::shared_ptr<Segment>& segment,
                                       const std::shared_ptr<ITabletSchema>& schema);

private:
    static const uint32_t MAX_FIELD_COUNT = 32;

    std::shared_ptr<indexlibv2::document::extractor::IDocumentInfoExtractorFactory> _docInfoExtractorFactory;
    std::string _indexName;
    std::string _rootPath;
    std::shared_ptr<file_system::Directory> _rootDir;
    std::shared_ptr<MetricsManager> _metricsManager;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
};

void SectionAttributeReaderTest::setUp()
{
    _docInfoExtractorFactory = std::make_shared<indexlibv2::plain::DocumentInfoExtractorFactory>();
    _indexName = "test_pack";
    _rootPath = GET_TEMP_DATA_PATH() + "/";
    kmonitor::MetricsTags metricsTags;
    _metricsReporter = std::make_shared<kmonitor::MetricsReporter>("", metricsTags, "");
    _metricsManager = std::make_shared<MetricsManager>("", _metricsReporter);

    auto loadStrategy = std::make_shared<file_system::MmapLoadStrategy>();
    file_system::LoadConfig::FilePatternStringVector pattern;
    pattern.push_back(".*");
    file_system::LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetFilePatternString(pattern);
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetName("__OFFLINE_ATTRIBUTE__");
    file_system::LoadConfigList loadConfigList;
    loadConfigList.PushFront(loadConfig);
    file_system::FileSystemOptions fsOptions;
    fsOptions.loadConfigList = loadConfigList;
    auto fs = file_system::FileSystemCreator::Create(identifier, _rootPath, fsOptions).GetOrThrow();
    _rootDir = file_system::Directory::Get(fs);
}

void SectionAttributeReaderTest::tearDown() {}

std::pair<Status, std::shared_ptr<Segment>>
SectionAttributeReaderTest::BuildOneSegmentData(const std::shared_ptr<file_system::Directory>& rootDirectory,
                                                segmentid_t segId, const std::shared_ptr<ITabletSchema>& schema,
                                                const std::string& indexName, uint32_t docCount, Answer& answer)
{
    auto packIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(
        schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, indexName));
    assert(packIndexConfig != nullptr);

    autil::mem_pool::Pool pool(SectionAttributeFormatter::DATA_SLICE_LEN * 16);
    indexlibv2::index::MemIndexerParameter emptyIndexerParam;
    SectionAttributeMemIndexer writer(emptyIndexerParam);
    auto status = writer.Init(packIndexConfig, _docInfoExtractorFactory.get());
    if (!status.IsOK()) {
        return std::make_pair(status, nullptr);
    }

    indexlibv2::document::SectionAttributeRewriter rewriter;
    [[maybe_unused]] bool ret = rewriter.Init(schema);
    assert(ret);

    for (docid_t docId = 0; docId < (docid_t)docCount; ++docId) {
        auto doc = std::make_shared<indexlibv2::document::NormalDocument>();
        autil::mem_pool::Pool* pool = doc->GetPool();
        auto indexDoc = std::make_shared<document::IndexDocument>(pool);
        doc->SetIndexDocument(indexDoc);
        doc->ModifyDocOperateType(ADD_DOC);

        std::vector<SectionMeta> sectionsOfCurrentDoc;
        uint32_t sectionPerDoc = docId % MAX_SECTION_COUNT_PER_DOC + 1;
        uint32_t sectionPerField = sectionPerDoc / MAX_FIELD_COUNT;
        if (sectionPerField == 0) {
            sectionPerField = 1;
        }

        for (uint32_t fieldId = 0; fieldId < MAX_FIELD_COUNT; ++fieldId) {
            document::IndexTokenizeField* field =
                IE_POOL_COMPATIBLE_NEW_CLASS(pool, document::IndexTokenizeField, pool);
            field->SetFieldId((fieldid_t)(fieldId));

            for (uint32_t sectionId = 0; sectionId < sectionPerField; ++sectionId) {
                document::Section* section = IE_POOL_COMPATIBLE_NEW_CLASS(pool, document::Section, 8, pool);
                section->SetLength((section_len_t)((docId + sectionId + 2) % 2047 + 1));
                section->SetWeight((section_weight_t)((docId + sectionId) % 65536));
                field->AddSection(section);

                SectionMeta meta;
                meta.fieldId = field->GetFieldId();
                meta.length = section->GetLength();
                meta.weight = section->GetWeight();

                sectionsOfCurrentDoc.push_back(meta);
            }

            indexDoc->SetField(fieldId, field);
        }

        doc->SetDocId(docId);
        auto status = rewriter.RewriteOneDoc(doc);
        assert(status.IsOK());

        docid_t globalId = answer.size();
        answer[globalId] = sectionsOfCurrentDoc;
        writer.EndDocument(*indexDoc);
    }

    SegmentMeta segMeta(segId);
    segMeta.schema = schema;
    auto segmentName = indexlibv2::PathUtil::NewSegmentDirName(segId, 0);
    auto segDir = _rootDir->MakeDirectory(segmentName);
    assert(segDir);
    segMeta.segmentInfo->docCount = docCount;
    segMeta.segmentDir = segDir;
    auto segment = std::make_shared<indexlibv2::framework::FakeDiskSegment>(segMeta);

    auto invertedIndexDirectory = segDir->MakeDirectory(INVERTED_INDEX_PATH);
    auto packageIndexDirectory = invertedIndexDirectory->MakeDirectory(packIndexConfig->GetIndexName());
    (void)packageIndexDirectory;

    status = writer.Dump(&pool, invertedIndexDirectory, nullptr);
    if (!status.IsOK()) {
        return std::make_pair(status, nullptr);
    }
    invertedIndexDirectory->Sync(true);

    indexlibv2::index::DiskIndexerParameter indexerParam;
    indexerParam.docCount = docCount;
    indexerParam.segmentInfo = segMeta.segmentInfo;
    indexerParam.metricsManager = _metricsManager.get();
    auto invertedDiskIndexer = std::make_shared<InvertedDiskIndexer>(indexerParam);
    status = invertedDiskIndexer->Open(packIndexConfig, invertedIndexDirectory->GetIDirectory());
    if (!status.IsOK()) {
        return std::make_pair(status, nullptr);
    }
    segment->AddIndexer(packIndexConfig->GetIndexType(), packIndexConfig->GetIndexName(), invertedDiskIndexer);
    return std::make_pair(Status::OK(), segment);
}

Status SectionAttributeReaderTest::AppendSegmentIntoTabletData(std::shared_ptr<TabletData>& tabletData,
                                                               std::shared_ptr<Segment>& segment,
                                                               const std::shared_ptr<ITabletSchema>& schema)
{
    std::vector<std::shared_ptr<Segment>> oldSegments;
    auto slice = tabletData->CreateSlice();
    for (const auto& seg : slice) {
        oldSegments.push_back(seg);
    }
    oldSegments.emplace_back(segment);
    std::shared_ptr<ResourceMap> map(new ResourceMap());
    auto schemaGroup = std::make_shared<TabletDataSchemaGroup>();
    schemaGroup->writeSchema = schema;
    schemaGroup->onDiskWriteSchema = schema;
    schemaGroup->onDiskReadSchema = schema;
    auto status = map->AddVersionResource(TabletDataSchemaGroup::NAME, schemaGroup);

    return tabletData->Init(/*invalid version*/ Version(), std::move(oldSegments), map);
}

Status SectionAttributeReaderTest::BuildMultiSegmentsData(const std::shared_ptr<file_system::Directory>& rootDirectory,
                                                          std::shared_ptr<TabletData>& tabletData,
                                                          const std::shared_ptr<ITabletSchema>& schema,
                                                          const std::string& indexName,
                                                          const std::vector<uint32_t>& docCounts, Answer& answer)
{
    for (size_t i = 0; i < docCounts.size(); ++i) {
        auto [status, segment] = BuildOneSegmentData(rootDirectory, i, schema, indexName, docCounts[i], answer);
        if (!status.IsOK()) {
            return status;
        }
        status = AppendSegmentIntoTabletData(tabletData, segment, schema);
        if (!status.IsOK()) {
            return status;
        }
    }
    return Status::OK();
}

std::pair<Status, std::shared_ptr<TabletData>>
SectionAttributeReaderTest::FullBuild(const std::shared_ptr<ITabletSchema>& schema,
                                      const std::vector<uint32_t>& fullBuildDocCounts, Answer& answer)
{
    tearDown();
    setUp();

    auto tabletData = std::make_shared<TabletData>(identifier);
    auto status = BuildMultiSegmentsData(_rootDir, tabletData, schema, _indexName, fullBuildDocCounts, answer);
    return std::make_pair(Status::OK(), tabletData);
}

Status SectionAttributeReaderTest::IncBuild(std::shared_ptr<TabletData>& tabletData,
                                            const std::shared_ptr<ITabletSchema>& schema, segmentid_t segId,
                                            uint32_t docCount, Answer& answer)
{
    auto [status, segment] = BuildOneSegmentData(_rootDir, segId, schema, _indexName, docCount, answer);
    if (!status.IsOK()) {
        return status;
    }
    status = AppendSegmentIntoTabletData(tabletData, segment, schema);
    if (!status.IsOK()) {
        return status;
    }
    return Status::OK();
}

void SectionAttributeReaderTest::InnerTestOpen(const std::vector<uint32_t>& fullBuildDocCounts)
{
    Answer answer;

    auto schema = SectionAttributeTestUtil::MakeSchemaWithPackageIndexConfig(
        /*indexName*/ _indexName, /*maxFieldCount*/ MAX_FIELD_COUNT, /*indexType*/ it_pack,
        /*bigFieldIdBase*/ 0,
        /*hasSectionAttribute*/ true);

    auto packIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(
        schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, _indexName));
    ASSERT_TRUE(packIndexConfig != nullptr);
    auto [status, tabletData] = FullBuild(schema, fullBuildDocCounts, answer);
    ASSERT_TRUE(status.IsOK());

    SectionAttributeReaderImpl reader(sp_nosort);
    ASSERT_TRUE(reader.Open(packIndexConfig, tabletData.get()).IsOK());
    CheckData(reader, answer);
}

void SectionAttributeReaderTest::CheckData(const SectionAttributeReaderImpl& reader, Answer& answer)
{
    size_t sizePerSection = sizeof(section_len_t) + sizeof(section_weight_t) + sizeof(section_fid_t);

    for (size_t i = 0; i < answer.size(); ++i) {
        const std::vector<SectionMeta>& expectedMeta = answer[i];

        uint8_t buf[sizePerSection * MAX_SECTION_COUNT_PER_DOC];
        uint32_t ret = reader.Read((docid_t)i, buf, sizeof(uint8_t) * sizePerSection * MAX_SECTION_COUNT_PER_DOC);
        assert(0 == ret);
        ASSERT_EQ((uint32_t)0, ret);

        MultiSectionMeta meta;
        meta.Init(buf, reader.HasFieldId(), reader.HasSectionWeight());
        CheckSectionMetaInOneDoc(expectedMeta, meta);

        auto inDocSectionMeta = reader.GetSection((docid_t)i);
        auto inDocMultiSectionMeta = std::dynamic_pointer_cast<InDocMultiSectionMeta>(inDocSectionMeta);
        ASSERT_TRUE(inDocMultiSectionMeta != NULL);
        CheckSectionMetaInOneDoc(expectedMeta, *inDocMultiSectionMeta);
    }
}

void SectionAttributeReaderTest::CheckSectionMetaInOneDoc(const std::vector<SectionMeta>& expectedMeta,
                                                          const MultiSectionMeta& meta)
{
    uint32_t sectionCount = meta.GetSectionCount();
    ASSERT_EQ(expectedMeta.size(), sectionCount);

    for (size_t i = 0; i < sectionCount; ++i) {
        section_fid_t fieldId = meta.GetFieldId(i);
        ASSERT_EQ(expectedMeta[i].fieldId, fieldId);

        section_len_t sectionLen = meta.GetSectionLen(i);
        ASSERT_EQ(expectedMeta[i].length, sectionLen);

        section_weight_t sectionWeight = meta.GetSectionWeight(i);
        ASSERT_EQ(expectedMeta[i].weight, sectionWeight);
    }
}

TEST_F(SectionAttributeReaderTest, TestOpenForOneSegment)
{
    // todo: big field id
    std::vector<uint32_t> fullBuildDocCounts;
    fullBuildDocCounts.push_back(10);

    InnerTestOpen(fullBuildDocCounts);
}

TEST_F(SectionAttributeReaderTest, TestOpenForMultiSegments)
{
    // todo: big field id
    std::vector<uint32_t> fullBuildDocCounts;
    fullBuildDocCounts.push_back(30);
    fullBuildDocCounts.push_back(100);
    fullBuildDocCounts.push_back(80);
    fullBuildDocCounts.push_back(0);
    fullBuildDocCounts.push_back(50);

    InnerTestOpen(fullBuildDocCounts);
}

TEST_F(SectionAttributeReaderTest, TestReOpen)
{
    // TODO: reader use open to reopen index
    auto schema = SectionAttributeTestUtil::MakeSchemaWithPackageIndexConfig(
        /*indexName*/ _indexName, /*maxFieldCount*/ MAX_FIELD_COUNT, /*indexType*/ it_pack,
        /*bigFieldIdBase*/ 0,
        /*hasSectionAttribute*/ true);
    auto packIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(
        schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, _indexName));
    ASSERT_TRUE(packIndexConfig != nullptr);

    std::vector<uint32_t> fullBuildDocCounts;
    fullBuildDocCounts.push_back(100);
    fullBuildDocCounts.push_back(230);
    fullBuildDocCounts.push_back(30);

    Answer answer;
    auto [status, tabletData] = FullBuild(schema, fullBuildDocCounts, answer);
    ASSERT_TRUE(status.IsOK());

    std::vector<uint32_t> incBuildDocCounts;
    incBuildDocCounts.push_back(20);
    incBuildDocCounts.push_back(40);

    for (size_t i = 0; i < incBuildDocCounts.size(); ++i) {
        segmentid_t segId = fullBuildDocCounts.size() + i;
        status = IncBuild(tabletData, schema, segId, incBuildDocCounts[i], answer);

        auto reader = std::make_shared<SectionAttributeReaderImpl>(sp_nosort);
        ASSERT_TRUE(reader->Open(packIndexConfig, tabletData.get()).IsOK());
        CheckData(*reader, answer);
    }
}

TEST_F(SectionAttributeReaderTest, TestRead)
{
    Answer answer;
    auto schema = SectionAttributeTestUtil::MakeSchemaWithPackageIndexConfig(
        /*indexName*/ _indexName, /*maxFieldCount*/ MAX_FIELD_COUNT, /*indexType*/ it_pack,
        /*bigFieldIdBase*/ 0,
        /*hasSectionAttribute*/ true);
    auto packIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(
        schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, _indexName));
    ASSERT_TRUE(packIndexConfig != nullptr);

    std::vector<uint32_t> fullBuildDocCounts;
    fullBuildDocCounts.push_back(1);
    auto [status, tabletData] = FullBuild(schema, fullBuildDocCounts, answer);
    ASSERT_TRUE(status.IsOK());

    SectionAttributeReaderImpl reader(sp_nosort);
    ASSERT_TRUE(reader.Open(packIndexConfig, tabletData.get()).IsOK());

    size_t bufLen =
        (sizeof(section_len_t) + sizeof(section_weight_t) + sizeof(section_fid_t)) * MAX_SECTION_COUNT_PER_DOC;
    uint8_t buf[bufLen];
    ASSERT_EQ(0, reader.Read(0, buf, bufLen));
    ASSERT_EQ(-1, reader.Read(1, buf, bufLen));
}

TEST_F(SectionAttributeReaderTest, TestGetSection)
{
    Answer answer;

    auto schema = SectionAttributeTestUtil::MakeSchemaWithPackageIndexConfig(
        /*indexName*/ _indexName, /*maxFieldCount*/ MAX_FIELD_COUNT, /*indexType*/ it_pack,
        /*bigFieldIdBase*/ 0,
        /*hasSectionAttribute*/ true);
    auto packIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(
        schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, _indexName));

    std::vector<uint32_t> fullBuildDocCounts;
    fullBuildDocCounts.push_back(1);
    auto [status, tabletData] = FullBuild(schema, fullBuildDocCounts, answer);

    SectionAttributeReaderImpl reader(sp_nosort);
    ASSERT_TRUE(reader.Open(packIndexConfig, tabletData.get()).IsOK());

    ASSERT_TRUE(reader.GetSection(0) != NULL);
    ASSERT_TRUE(reader.GetSection(1) == NULL);
}

} // namespace indexlib::index
