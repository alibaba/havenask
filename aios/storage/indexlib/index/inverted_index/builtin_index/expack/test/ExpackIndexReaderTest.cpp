#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/IndexAccessoryReader.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/InvertedIndexReaderImpl.h"
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/expack/test/ExpackIndexDocumentMaker.h"
#include "indexlib/index/inverted_index/builtin_index/test_util/InvertedTestUtil.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/test/InvertedIndexConfigCreator.h"
#include "indexlib/index/inverted_index/format/NormalInDocPositionIterator.h"
#include "indexlib/util/NumericUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {
namespace {
using indexlibv2::framework::SegmentInfo;

} // namespace

#define VOL_NAME "reader_vol1"
#define VOL_CONTENT "token0"

class ExpackIndexReaderTest : public TESTBASE
{
public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 10240;

    ExpackIndexReaderTest()
    {
        _vol.reset(new config::HighFrequencyVocabulary);
        _vol->Init("expack", it_expack, VOL_CONTENT, {});
    }
    ~ExpackIndexReaderTest() = default;

public:
    typedef InvertedTestHelper::Answer Answer;
    typedef InvertedTestHelper::PostingAnswerMap PostingAnswerMap;
    typedef InvertedTestHelper::SectionAnswerMap SectionAnswerMap;
    typedef InvertedTestHelper::KeyAnswer KeyAnswer;
    typedef InvertedTestHelper::KeyAnswerInDoc KeyAnswerInDoc;

    void setUp() override
    {
        std::string dir = GET_TEMP_DATA_PATH();
        _testUtil = std::make_shared<InvertedTestUtil>(dir, _vol);
        _testUtil->SetUp();
    }
    void tearDown() override
    {
        if (_testUtil) {
            _testUtil->TearDown();
        }
    }

private:
    void InitSchemaWithOnePackageConfig(uint32_t fieldNum, InvertedIndexType indexType, const std::string& indexName);
    void InnerTestDumpEmptySegment(optionflag_t optionFlag);
    void InnerTestLookUpWithManyDoc(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding);
    void InnerTestLookUpWithOneDoc(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding = false);
    void InnerTestLookUpWithMultiSegment(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding);
    void InnerTestGenFieldMapMask(const std::vector<std::string>& termFieldNames, fieldmap_t targetFieldMap,
                                  bool isSuccess = true);

    void CheckIndexReaderLookup(const std::vector<uint32_t>& docNums,
                                const std::shared_ptr<InvertedIndexReaderImpl>& indexReader,
                                const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                Answer& answer, bool usePool = false);
    void CheckSection(docid_t docId, const std::shared_ptr<InDocPositionIterator>& inDocPosIter,
                      SectionAnswerMap& sectionAnswerMap);

    void CreateOneSegmentData(segmentid_t segId, uint32_t docCount, docid_t baseDocId,
                              const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                              Answer& answer);
    void CreateMultiSegmentsData(const std::vector<uint32_t>& docNums,
                                 const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                 Answer& answer);
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> CreateIndexConfig(optionflag_t optionFlag,
                                                                               bool hasHighFreq, bool multiSharding);

private:
    std::shared_ptr<InvertedTestUtil> _testUtil;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    std::shared_ptr<config::HighFrequencyVocabulary> _vol;
};

void ExpackIndexReaderTest::InnerTestDumpEmptySegment(optionflag_t optionFlag)
{
    std::vector<uint32_t> docNums;
    docNums.push_back(100);
    docNums.push_back(23);

    std::vector<SegmentInfo> segmentInfos;
    std::shared_ptr<config::HighFrequencyVocabulary> vol;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig = CreateIndexConfig(optionFlag, false, false);

    docid_t currBeginDocId = 0;
    SegmentInfo segmentInfo;
    segmentInfo.docCount = 0;

    std::vector<std::shared_ptr<document::IndexDocument>> indexDocs;
    indexDocs.push_back(
        std::shared_ptr<document::IndexDocument>(new document::IndexDocument(_testUtil->_byteSlicePool)));
    InvertedTestHelper::RewriteSectionAttributeInIndexDocuments(indexDocs, _schema);

    for (size_t x = 0; x < docNums.size(); ++x) {
        auto writer = _testUtil->CreateIndexWriter(indexConfig, /*resetHighFreqVol=*/true);
        document::IndexDocument& emptyIndexDoc = *indexDocs[0];
        for (uint32_t i = 0; i < docNums[x]; i++) {
            emptyIndexDoc.SetDocId(i);
            writer->EndDocument(emptyIndexDoc);
        }
        segmentInfo.docCount = docNums[x];
        writer->Seal();
        _testUtil->DumpSegment(/*segmentId=*/x, writer, segmentInfo);
        currBeginDocId += segmentInfo.docCount;
    }

    auto indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
    assert(indexReader);
}

void ExpackIndexReaderTest::InnerTestLookUpWithManyDoc(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding)
{
    setMultiSharding = false;
    std::vector<uint32_t> docNums;
    docNums.push_back(6);

    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig =
        CreateIndexConfig(optionFlag, hasHighFreq, setMultiSharding);
    Answer answer;
    CreateMultiSegmentsData(docNums, indexConfig, answer);

    auto indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
    CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer, true);
}

void ExpackIndexReaderTest::InnerTestLookUpWithOneDoc(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding)
{
    setMultiSharding = false;
    std::vector<uint32_t> docNums;
    docNums.push_back(1);

    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig =
        CreateIndexConfig(optionFlag, hasHighFreq, setMultiSharding);
    Answer answer;
    CreateMultiSegmentsData(docNums, indexConfig, answer);
    auto indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
    CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
}

void ExpackIndexReaderTest::InnerTestLookUpWithMultiSegment(bool hasHighFreq, optionflag_t optionFlag,
                                                            bool setMultiSharding)
{
    setMultiSharding = false;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig =
        CreateIndexConfig(optionFlag, hasHighFreq, setMultiSharding);
    const int32_t segmentCounts[] = {1, 4, 9};
    for (size_t j = 0; j < sizeof(segmentCounts) / sizeof(int32_t); ++j) {
        std::vector<uint32_t> docNums;
        for (int32_t i = 0; i < segmentCounts[j]; ++i) {
            docNums.push_back(rand() % 37 + 7);
        }

        Answer answer;
        CreateMultiSegmentsData(docNums, indexConfig, answer);
        auto indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    }
}

void ExpackIndexReaderTest::InnerTestGenFieldMapMask(const std::vector<std::string>& termFieldNames,
                                                     fieldmap_t targetFieldMap, bool isSuccess)
{
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig =
        CreateIndexConfig(EXPACK_OPTION_FLAG_ALL, false, false);
    ASSERT_TRUE(indexConfig);
    InvertedIndexReaderImpl indexReader(nullptr);
    indexReader._indexConfig = indexConfig;
    fieldmap_t fieldMap = 0;
    ASSERT_EQ(isSuccess, indexReader.GenFieldMapMask("", termFieldNames, fieldMap));
    ASSERT_EQ(targetFieldMap, fieldMap);
}

void ExpackIndexReaderTest::CheckIndexReaderLookup(
    const std::vector<uint32_t>& docNums, const std::shared_ptr<InvertedIndexReaderImpl>& indexReader,
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig, Answer& answer, bool usePool)
{
    optionflag_t optionFlag = indexConfig->GetOptionFlag();

    ASSERT_EQ(indexReader->HasTermPayload(), (optionFlag & of_term_payload) == of_term_payload);
    ASSERT_EQ(indexReader->HasDocPayload(), (optionFlag & of_doc_payload) == of_doc_payload);
    ASSERT_EQ(indexReader->HasPositionPayload(), (optionFlag & of_position_payload) == of_position_payload);

    const PostingAnswerMap& postingAnswerMap = answer.postingAnswerMap;
    SectionAnswerMap& sectionAnswerMap = answer.sectionAnswerMap;

    bool hasHighFreq = (indexConfig->GetDictConfig() != nullptr);
    PostingAnswerMap::const_iterator keyIter = postingAnswerMap.begin();
    for (; keyIter != postingAnswerMap.end(); ++keyIter) {
        std::string key = keyIter->first;

        index::Term term(key, "");
        autil::mem_pool::Pool pool;
        autil::mem_pool::Pool* pPool = usePool ? &pool : nullptr;

        PostingIterator* iter = indexReader->Lookup(term, 1000, pt_default, pPool).ValueOrThrow();
        ASSERT_TRUE(iter != nullptr);

        bool hasPos = iter->HasPosition();
        if (hasHighFreq && _vol->Lookup(term.GetWord())) {
            PostingIterator* bitmapIt = indexReader->Lookup(term, 1000, pt_bitmap, pPool).ValueOrThrow();

            ASSERT_EQ(bitmapIt->GetType(), pi_bitmap);
            BitmapPostingIterator* bmIt = dynamic_cast<BitmapPostingIterator*>(bitmapIt);
            ASSERT_TRUE(bmIt != nullptr);
            ASSERT_EQ(bmIt->HasPosition(), false);
            IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, bitmapIt);
        } else {
            ASSERT_EQ(iter->HasPosition(), (optionFlag & of_position_list) != 0);
        }

        const KeyAnswer& keyAnswer = keyIter->second;
        uint32_t docCount = keyAnswer.docIdList.size();
        docid_t docId = 0;

        TermMeta* tm = iter->GetTermMeta();
        if (optionFlag & of_term_payload) {
            ASSERT_EQ(keyAnswer.termPayload, tm->GetPayload());
        }

        if (optionFlag & of_term_frequency) {
            ASSERT_EQ(keyAnswer.totalTF, tm->GetTotalTermFreq());
        }

        ASSERT_EQ((df_t)keyAnswer.docIdList.size(), tm->GetDocFreq());

        for (uint32_t i = 0; i < docCount; i++) {
            docId = iter->SeekDoc(docId);
            ASSERT_EQ(docId, keyAnswer.docIdList[i]);

            TermMatchData tmd;
            if (optionFlag & of_doc_payload) {
                tmd.SetHasDocPayload(true);
            }
            iter->Unpack(tmd);
            const KeyAnswerInDoc& answerInDoc = keyAnswer.inDocAnswers.find(docId)->second;

            if (key == VOL_CONTENT && hasHighFreq) {
                ASSERT_EQ(1, tmd.GetTermFreq());
                if (optionFlag & of_doc_payload) {
                    ASSERT_EQ(0, tmd.GetDocPayload());
                }
            } else {
                if (optionFlag & of_term_frequency) {
                    ASSERT_EQ(keyAnswer.tfList[i], tmd.GetTermFreq());
                } else {
                    ASSERT_EQ(1, tmd.GetTermFreq());
                }

                if (optionFlag & of_doc_payload) {
                    ASSERT_EQ(answerInDoc.docPayload, tmd.GetDocPayload());
                }
                ASSERT_EQ(answerInDoc.fieldMap, tmd.GetFieldMap());
            }

            if (!hasPos) {
                continue;
            }

            std::shared_ptr<InDocPositionIterator> inDocPosIter = tmd.GetInDocPositionIterator();
            pos_t pos = 0;
            uint32_t posCount = answerInDoc.positionList.size();
            for (uint32_t j = 0; j < posCount; j++) {
                pos = inDocPosIter->SeekPosition(pos);
                ASSERT_EQ(answerInDoc.positionList[j], pos);

                if (optionFlag & of_position_payload) {
                    pospayload_t posPayload = inDocPosIter->GetPosPayload();
                    ASSERT_EQ(answerInDoc.posPayloadList[j], posPayload);
                }

                CheckSection(docId, inDocPosIter, sectionAnswerMap);
            }
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, iter);
    }
}

void ExpackIndexReaderTest::CheckSection(docid_t docId, const std::shared_ptr<InDocPositionIterator>& inDocPosIter,
                                         SectionAnswerMap& sectionAnswerMap)
{
    return;
}

void ExpackIndexReaderTest::CreateOneSegmentData(
    segmentid_t segId, uint32_t docCount, docid_t baseDocId,
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig, Answer& answer)
{
    SegmentInfo segmentInfo;
    segmentInfo.docCount = 0;
    auto writer = _testUtil->CreateIndexWriter(indexConfig,
                                               /*resetHighFreqVol=*/false);
    std::vector<std::shared_ptr<document::IndexDocument>> indexDocs;
    autil::mem_pool::Pool pool;
    auto packageIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(indexConfig);
    ASSERT_TRUE(packageIndexConfig);
    ExpackIndexDocumentMaker::MakeIndexDocuments(&pool, indexDocs, docCount, baseDocId, &answer, packageIndexConfig);
    InvertedTestHelper::RewriteSectionAttributeInIndexDocuments(indexDocs, _schema);

    for (size_t idx = 0; idx < indexDocs.size(); ++idx) {
        document::IndexDocument::Iterator iter = indexDocs[idx]->CreateIterator();
        while (iter.HasNext()) {
            auto s = writer->AddField(iter.Next());
            ASSERT_TRUE(s.IsOK());
        }
        writer->EndDocument(*(indexDocs[idx]));
    }

    segmentInfo.docCount = docCount;
    writer->Seal();
    _testUtil->DumpSegment(segId, writer, segmentInfo);
    indexDocs.clear();
}

void ExpackIndexReaderTest::CreateMultiSegmentsData(
    const std::vector<uint32_t>& docNums, const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
    Answer& answer)
{
    tearDown();
    setUp();

    docid_t baseDocId = 0;
    for (size_t i = 0; i < docNums.size(); ++i) {
        CreateOneSegmentData(i, docNums[i], baseDocId, indexConfig, answer);
        baseDocId += docNums[i];
    }
}

void ExpackIndexReaderTest::InitSchemaWithOnePackageConfig(uint32_t fieldNum, InvertedIndexType indexType,
                                                           const std::string& indexName)
{
    auto schema = std::make_shared<indexlibv2::config::TabletSchema>();
    for (uint32_t i = 0; i < fieldNum; i++) {
        std::string fieldName = "field" + std::to_string(i);
        auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>(fieldName, ft_text, false);
        fieldConfig->SetAnalyzerName("taobao_analyzer");
        ASSERT_TRUE(schema->TEST_GetImpl()->AddFieldConfig(fieldConfig).IsOK());
    }
    auto indexConf = std::make_shared<indexlibv2::config::PackageIndexConfig>(indexName, indexType);
    indexConf->SetIndexId(0);
    for (uint32_t i = 0; i < fieldNum; i++) {
        std::stringstream ss;
        ss << "field" << i;
        auto status = indexConf->AddFieldConfig(schema->GetFieldConfig(ss.str()), 10 - i + 1);
        ASSERT_TRUE(status.IsOK());
    }
    ASSERT_TRUE(schema->TEST_GetImpl()->AddIndexConfig(indexConf).IsOK());
    _schema = schema;
}

std::shared_ptr<indexlibv2::config::InvertedIndexConfig>
ExpackIndexReaderTest::CreateIndexConfig(optionflag_t optionFlag, bool hasHighFreq, bool multiSharding)
{
    InitSchemaWithOnePackageConfig(8, it_expack, "ExpackIndex");
    auto config = _schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, "ExpackIndex");
    assert(config);
    auto indexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(config);
    assert(indexConfig);
    if (hasHighFreq) {
        auto dictConfig = std::make_shared<config::DictionaryConfig>(VOL_NAME, VOL_CONTENT);
        indexConfig->SetDictConfig(dictConfig);
        indexConfig->SetHighFreqencyTermPostingType(indexlib::index::hp_both);
    }
    indexConfig->SetOptionFlag(optionFlag);
    if (multiSharding) {
        // set sharding count to 3
        indexlib::index::InvertedIndexConfigCreator::CreateShardingIndexConfigs(indexConfig, 3);
    }
    return indexConfig;
}

TEST_F(ExpackIndexReaderTest, TestCaseForGenFieldMapMask)
{
    {
        std::vector<std::string> termFieldNames;
        termFieldNames.push_back("field5");
        fieldmap_t targetFieldMap = 32;
        InnerTestGenFieldMapMask(termFieldNames, targetFieldMap);
    }
    {
        std::vector<std::string> termFieldNames;
        termFieldNames.push_back("field1");
        termFieldNames.push_back("field2");
        fieldmap_t targetFieldMap = 6;
        InnerTestGenFieldMapMask(termFieldNames, targetFieldMap);
    }

    {
        std::vector<std::string> termFieldNames;
        termFieldNames.push_back("field0");
        termFieldNames.push_back("field1");
        termFieldNames.push_back("field2");
        termFieldNames.push_back("field3");
        termFieldNames.push_back("field4");
        fieldmap_t targetFieldMap = 31;
        InnerTestGenFieldMapMask(termFieldNames, targetFieldMap);
    }
    {
        std::vector<std::string> termFieldNames;
        termFieldNames.push_back("field0");
        termFieldNames.push_back("field1");
        termFieldNames.push_back("field2");
        termFieldNames.push_back("field3");
        termFieldNames.push_back("field4");
        termFieldNames.push_back("field5");
        termFieldNames.push_back("field6");
        termFieldNames.push_back("field7");
        fieldmap_t targetFieldMap = 255;
        InnerTestGenFieldMapMask(termFieldNames, targetFieldMap);
    }
    {
        std::vector<std::string> termFieldNames;
        termFieldNames.push_back("not_exist");
        fieldmap_t targetFieldMap = 0;
        InnerTestGenFieldMapMask(termFieldNames, targetFieldMap, false);
    }
}

TEST_F(ExpackIndexReaderTest, TestCaseForLookUpWithOneDoc)
{
    InnerTestLookUpWithOneDoc(false, EXPACK_OPTION_FLAG_ALL);
    InnerTestLookUpWithOneDoc(false, EXPACK_OPTION_FLAG_ALL, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForLookUpWithOneDocWithHighFreq)
{
    InnerTestLookUpWithOneDoc(true, EXPACK_OPTION_FLAG_ALL);
    InnerTestLookUpWithOneDoc(true, EXPACK_OPTION_FLAG_ALL, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithOneDoc)
{
    InnerTestLookUpWithOneDoc(false, EXPACK_NO_PAYLOAD);
    InnerTestLookUpWithOneDoc(false, EXPACK_NO_PAYLOAD, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithOneDocWithHighFreq)
{
    InnerTestLookUpWithOneDoc(true, EXPACK_NO_PAYLOAD);
    InnerTestLookUpWithOneDoc(true, EXPACK_NO_PAYLOAD, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithOneDoc)
{
    InnerTestLookUpWithOneDoc(false, EXPACK_NO_POSITION_LIST);
    InnerTestLookUpWithOneDoc(false, EXPACK_NO_POSITION_LIST, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithOneDocWithHighFreq)
{
    InnerTestLookUpWithOneDoc(true, EXPACK_NO_POSITION_LIST);
    InnerTestLookUpWithOneDoc(true, EXPACK_NO_POSITION_LIST, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithOneDoc)
{
    InnerTestLookUpWithOneDoc(false, EXPACK_NO_TERM_FREQUENCY);
    InnerTestLookUpWithOneDoc(false, EXPACK_NO_TERM_FREQUENCY, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithOneDocWithHighFreq)
{
    InnerTestLookUpWithOneDoc(true, EXPACK_NO_TERM_FREQUENCY);
    InnerTestLookUpWithOneDoc(true, EXPACK_NO_TERM_FREQUENCY, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForLookUpWithManyDoc)
{
    InnerTestLookUpWithManyDoc(false, EXPACK_OPTION_FLAG_ALL, false);
    InnerTestLookUpWithManyDoc(false, EXPACK_OPTION_FLAG_ALL, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForLookUpWithManyDocWithHighFreq)
{
    InnerTestLookUpWithManyDoc(true, EXPACK_OPTION_FLAG_ALL, false);
    InnerTestLookUpWithManyDoc(true, EXPACK_OPTION_FLAG_ALL, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithManyDoc)
{
    InnerTestLookUpWithManyDoc(false, EXPACK_NO_PAYLOAD, false);
    InnerTestLookUpWithManyDoc(false, EXPACK_NO_PAYLOAD, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithManyDocWithHighFreq)
{
    InnerTestLookUpWithManyDoc(true, EXPACK_NO_PAYLOAD, false);
    InnerTestLookUpWithManyDoc(true, EXPACK_NO_PAYLOAD, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithManyDoc)
{
    InnerTestLookUpWithManyDoc(false, EXPACK_NO_POSITION_LIST, false);
    InnerTestLookUpWithManyDoc(false, EXPACK_NO_POSITION_LIST, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithManyDocWithHighFreq)
{
    InnerTestLookUpWithManyDoc(true, EXPACK_NO_POSITION_LIST, false);
    InnerTestLookUpWithManyDoc(true, EXPACK_NO_POSITION_LIST, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithManyDoc)
{
    InnerTestLookUpWithManyDoc(false, EXPACK_NO_TERM_FREQUENCY, false);
    InnerTestLookUpWithManyDoc(false, EXPACK_NO_TERM_FREQUENCY, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithManyDocWithHighFreq)
{
    InnerTestLookUpWithManyDoc(true, EXPACK_NO_TERM_FREQUENCY, false);
    InnerTestLookUpWithManyDoc(true, EXPACK_NO_TERM_FREQUENCY, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForLookUpWithMultiSegment)
{
    InnerTestLookUpWithMultiSegment(false, EXPACK_OPTION_FLAG_ALL, false);
    InnerTestLookUpWithMultiSegment(false, EXPACK_OPTION_FLAG_ALL, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForLookUpWithMultiSegmentWithHighFreq)
{
    InnerTestLookUpWithMultiSegment(true, EXPACK_OPTION_FLAG_ALL, false);
    InnerTestLookUpWithMultiSegment(true, EXPACK_OPTION_FLAG_ALL, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithMultiSegment)
{
    InnerTestLookUpWithMultiSegment(false, EXPACK_NO_PAYLOAD, false);
    InnerTestLookUpWithMultiSegment(false, EXPACK_NO_PAYLOAD, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithMultiSegmentWithHighFreq)
{
    InnerTestLookUpWithMultiSegment(true, EXPACK_NO_PAYLOAD, false);
    InnerTestLookUpWithMultiSegment(true, EXPACK_NO_PAYLOAD, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithMultiSegment)
{
    InnerTestLookUpWithMultiSegment(false, EXPACK_NO_POSITION_LIST, false);
    InnerTestLookUpWithMultiSegment(false, EXPACK_NO_POSITION_LIST, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithMultiSegmentWithHighFreq)
{
    InnerTestLookUpWithMultiSegment(true, EXPACK_NO_POSITION_LIST, false);
    InnerTestLookUpWithMultiSegment(true, EXPACK_NO_POSITION_LIST, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithMultiSegment)
{
    InnerTestLookUpWithMultiSegment(false, EXPACK_NO_TERM_FREQUENCY, false);
    InnerTestLookUpWithMultiSegment(false, EXPACK_NO_TERM_FREQUENCY, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithMultiSegmentWithHighFreq)
{
    InnerTestLookUpWithMultiSegment(true, EXPACK_NO_TERM_FREQUENCY, false);
    InnerTestLookUpWithMultiSegment(true, EXPACK_NO_TERM_FREQUENCY, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForDumpEmptySegment) { InnerTestDumpEmptySegment(EXPACK_OPTION_FLAG_ALL); }

TEST_F(ExpackIndexReaderTest, TestCaseForNoPositionListDumpEmptySegment)
{
    InnerTestDumpEmptySegment(EXPACK_NO_POSITION_LIST);
}

TEST_F(ExpackIndexReaderTest, TestCaseForTfBitmapOneDoc)
{
    InnerTestLookUpWithOneDoc(true, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap);
    InnerTestLookUpWithOneDoc(true, EXPACK_NO_PAYLOAD | of_tf_bitmap);
    InnerTestLookUpWithOneDoc(true, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap, true);
    InnerTestLookUpWithOneDoc(true, EXPACK_NO_PAYLOAD | of_tf_bitmap, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForTfBitmapManyDoc)
{
    InnerTestLookUpWithManyDoc(false, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap, false);
    InnerTestLookUpWithManyDoc(false, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap, true);
    InnerTestLookUpWithManyDoc(false, EXPACK_NO_PAYLOAD | of_tf_bitmap, false);
    InnerTestLookUpWithManyDoc(false, EXPACK_NO_PAYLOAD | of_tf_bitmap, true);
}

TEST_F(ExpackIndexReaderTest, TestCaseForTfBitmapMultiSegment)
{
    InnerTestLookUpWithMultiSegment(true, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap, false);
    InnerTestLookUpWithMultiSegment(true, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap, true);
    InnerTestLookUpWithMultiSegment(true, EXPACK_NO_PAYLOAD | of_tf_bitmap, false);
    InnerTestLookUpWithMultiSegment(true, EXPACK_NO_PAYLOAD | of_tf_bitmap, true);
}

} // namespace indexlib::index
