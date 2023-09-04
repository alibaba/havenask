#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/InvertedIndexReaderImpl.h"
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/test_util/InvertedTestUtil.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/test/InvertedIndexConfigCreator.h"
#include "indexlib/index/inverted_index/test/InvertedTestHelper.h"
#include "indexlib/util/NumericUtil.h"
#include "unittest/unittest.h"

using namespace std;

using namespace autil;
using namespace autil::legacy;
using namespace autil::mem_pool;

using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::util;
using namespace indexlib::index;

namespace indexlibv2 { namespace index {

#define VOL_NAME "reader_vol1"
#define VOL_CONTENT "token0"

class PackIndexReaderTest : public TESTBASE
{
public:
    PackIndexReaderTest()
    {
        _vol.reset(new HighFrequencyVocabulary);
        _vol->Init("pack", it_pack, VOL_CONTENT, {});
    }
    ~PackIndexReaderTest() {}

public:
    typedef indexlib::index::InvertedTestHelper::Answer Answer;
    typedef indexlib::index::InvertedTestHelper::PostingAnswerMap PostingAnswerMap;
    typedef indexlib::index::InvertedTestHelper::SectionAnswerMap SectionAnswerMap;
    typedef indexlib::index::InvertedTestHelper::KeyAnswer KeyAnswer;
    typedef indexlib::index::InvertedTestHelper::KeyAnswerInDoc KeyAnswerInDoc;

    struct PositionCheckInfo {
        docid_t docId;
        TermMatchData tmd;
        KeyAnswerInDoc keyAnswerInDoc;
    };

    void setUp() override
    {
        string dir = GET_TEMP_DATA_PATH();
        _testUtil = make_shared<InvertedTestUtil>(dir, _vol);
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
    void InnerTestLookUpWithManyDoc(bool hasHighFreq, optionflag_t optionFlag, bool hasSectionAttribute,
                                    bool setMultiSharding);
    void InnerTestLookUpWithOneDoc(bool hasHighFreq, optionflag_t optionFlag, bool hasSectionAttribute,
                                   bool setMultiSharding);
    void InnerTestLookUpWithMultiSegment(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding);
    void InnerTestDumpEmptySegment(optionflag_t optionFlag, bool setMultiSharding);

    void CheckIndexReaderLookup(const vector<uint32_t>& docNums, const shared_ptr<InvertedIndexReader>& indexReader,
                                const shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig, Answer& answer);

    void CheckSection(docid_t docId, const std::shared_ptr<InDocPositionIterator>& inDocPosIter,
                      SectionAnswerMap& sectionAnswerMap);

    void CreateMultiSegmentsData(const vector<uint32_t>& docNums,
                                 const shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                 Answer& answer);
    void CreateOneSegmentData(segmentid_t segId, uint32_t docCount, docid_t baseDocId,
                              const shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig, Answer& answer);
    shared_ptr<indexlibv2::config::InvertedIndexConfig> CreateIndexConfig(optionflag_t optionFlag, bool hasHighFreq,
                                                                          bool hasSectionAttribute, bool multiSharding);

private:
    shared_ptr<InvertedTestUtil> _testUtil;
    shared_ptr<config::ITabletSchema> _schema;
    shared_ptr<HighFrequencyVocabulary> _vol;
};

void PackIndexReaderTest::InnerTestLookUpWithManyDoc(bool hasHighFreq, optionflag_t optionFlag, bool hasSectionAttr,
                                                     bool setMultiSharding)
{
    tearDown();
    setUp();
    vector<uint32_t> docNums;
    docNums.push_back(97);

    shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig =
        CreateIndexConfig(optionFlag, hasHighFreq, /*hasSectionAttr*/ false, setMultiSharding);
    Answer answer;
    CreateMultiSegmentsData(docNums, indexConfig, answer);

    shared_ptr<InvertedIndexReaderImpl> indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
    CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
}

void PackIndexReaderTest::CreateMultiSegmentsData(
    const vector<uint32_t>& docNums, const shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
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

void PackIndexReaderTest::CreateOneSegmentData(segmentid_t segId, uint32_t docCount, docid_t baseDocId,
                                               const shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                               Answer& answer)
{
    framework::SegmentInfo segmentInfo;
    segmentInfo.docCount = 0;

    shared_ptr<InvertedMemIndexer> writer = _testUtil->CreateIndexWriter(indexConfig,
                                                                         /*resetHighFreqVol=*/false);
    vector<std::shared_ptr<indexlib::document::IndexDocument>> indexDocs;
    Pool pool;
    indexlib::index::InvertedTestHelper::MakeIndexDocuments(&pool, indexDocs, docCount, baseDocId, &answer);
    indexlib::index::InvertedTestHelper::RewriteSectionAttributeInIndexDocuments(indexDocs, _schema);

    for (size_t idx = 0; idx < indexDocs.size(); ++idx) {
        IndexDocument::Iterator iter = indexDocs[idx]->CreateIterator();
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

void PackIndexReaderTest::CheckIndexReaderLookup(const vector<uint32_t>& docNums,
                                                 const shared_ptr<InvertedIndexReader>& indexReader,
                                                 const shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                                 Answer& answer)
{
    auto packIndexConfig = dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(indexConfig);
    ASSERT_TRUE(packIndexConfig.get() != nullptr);
    optionflag_t optionFlag = indexConfig->GetOptionFlag();

    ASSERT_EQ(indexReader->HasTermPayload(), (optionFlag & of_term_payload) == of_term_payload);
    ASSERT_EQ(indexReader->HasDocPayload(), (optionFlag & of_doc_payload) == of_doc_payload);
    ASSERT_EQ(indexReader->HasPositionPayload(), (optionFlag & of_position_payload) == of_position_payload);

    const PostingAnswerMap& postingAnswerMap = answer.postingAnswerMap;
    SectionAnswerMap& sectionAnswerMap = answer.sectionAnswerMap;

    bool hasHighFreq = (indexConfig->GetDictConfig() != nullptr);
    uint32_t hint = 0;
    PostingAnswerMap::const_iterator keyIter = postingAnswerMap.begin();
    for (; keyIter != postingAnswerMap.end(); ++keyIter) {
        bool usePool = (++hint % 2) == 0;
        string key = keyIter->first;

        indexlib::index::Term term(key, "");
        Pool pool;
        Pool* pPool = usePool ? &pool : nullptr;

        PostingIterator* iter = indexReader->Lookup(term, 1000, pt_default, pPool).ValueOrThrow();
        ASSERT_TRUE(iter != nullptr);

        bool hasPos = iter->HasPosition();
        if (hasHighFreq && _vol->Lookup(term.GetWord())) {
            PostingIterator* bitmapIt = indexReader->Lookup(term, 1000, pt_bitmap, pPool).ValueOrThrow();

            ASSERT_EQ(bitmapIt->GetType(), pi_bitmap);
            BitmapPostingIterator* bmIt = dynamic_cast<BitmapPostingIterator*>(bitmapIt);
            ASSERT_TRUE(bmIt != nullptr);
            ASSERT_EQ(bmIt->HasPosition(), false);
            indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, bitmapIt);
        } else {
            ASSERT_EQ(iter->HasPosition(), (optionFlag & of_position_list) != 0);
        }

        const KeyAnswer& keyAnswer = keyIter->second;
        uint32_t docCount = keyAnswer.docIdList.size();
        docid_t docId = INVALID_DOCID;

        TermMeta* tm = iter->GetTermMeta();
        if (optionFlag & of_term_payload) {
            ASSERT_EQ(keyAnswer.termPayload, tm->GetPayload());
        }
        if (optionFlag & of_term_frequency) {
            ASSERT_EQ(keyAnswer.totalTF, tm->GetTotalTermFreq());
        }
        ASSERT_EQ((df_t)keyAnswer.docIdList.size(), tm->GetDocFreq());
        vector<PositionCheckInfo> positionCheckInfos;
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
            }

            if (!hasPos) {
                continue;
            }
            if (optionFlag & of_tf_bitmap) {
                PositionCheckInfo info;
                info.docId = docId;
                info.tmd = tmd;
                info.keyAnswerInDoc = answerInDoc;
                positionCheckInfos.push_back(info);
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

                if (packIndexConfig->HasSectionAttribute()) {
                    CheckSection(docId, inDocPosIter, sectionAnswerMap);
                }
            }
        }
        // check in doc position in random order
        random_shuffle(positionCheckInfos.begin(), positionCheckInfos.end());
        for (uint32_t i = 0; i < positionCheckInfos.size(); ++i) {
            docid_t docId = positionCheckInfos[i].docId;
            TermMatchData& tmd = positionCheckInfos[i].tmd;
            const KeyAnswerInDoc& answerInDoc = positionCheckInfos[i].keyAnswerInDoc;
            std::shared_ptr<InDocPositionIterator> inDocPosIter = tmd.GetInDocPositionIterator();
            pos_t pos = 0;
            uint32_t posCount = answerInDoc.positionList.size();
            for (uint32_t j = 0; j < posCount; j++) {
                pos = inDocPosIter->SeekPosition(pos);
                assert(answerInDoc.positionList[j] == pos);

                if (optionFlag & of_position_payload) {
                    pospayload_t posPayload = inDocPosIter->GetPosPayload();
                    ASSERT_EQ(answerInDoc.posPayloadList[j], posPayload);
                }

                ASSERT_EQ(answerInDoc.positionList[j], pos);
                CheckSection(docId, inDocPosIter, sectionAnswerMap);
            }
            ASSERT_EQ(INVALID_POSITION, inDocPosIter->SeekPosition(pos));
        }
        indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, iter);
    }
}

void PackIndexReaderTest::CheckSection(docid_t docId, const std::shared_ptr<InDocPositionIterator>& inDocPosIter,
                                       SectionAnswerMap& sectionAnswerMap)
{
    // not support section yet
    return;
}

void PackIndexReaderTest::InnerTestLookUpWithOneDoc(bool hasHighFreq, optionflag_t optionFlag, bool hasSectionAttribute,
                                                    bool setMultiSharding)
{
    tearDown();
    setUp();
    vector<uint32_t> docNums;
    docNums.push_back(1);

    // TODO(yonghao.fyh): support multi sharding in the future
    setMultiSharding = false;
    hasSectionAttribute = false;

    auto indexConfig = CreateIndexConfig(optionFlag, hasHighFreq, hasSectionAttribute, setMultiSharding);
    Answer answer;
    CreateMultiSegmentsData(docNums, indexConfig, answer);
    shared_ptr<InvertedIndexReaderImpl> indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
    CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
}

void PackIndexReaderTest::InnerTestLookUpWithMultiSegment(bool hasHighFreq, optionflag_t optionFlag,
                                                          bool setMultiSharding)
{
    tearDown();
    setUp();
    auto indexConfig = CreateIndexConfig(optionFlag, hasHighFreq, true, setMultiSharding);
    const int32_t segmentCounts[] = {1, 4, 9};
    for (size_t j = 0; j < sizeof(segmentCounts) / sizeof(int32_t); ++j) {
        vector<uint32_t> docNums;
        for (int32_t i = 0; i < segmentCounts[j]; ++i) {
            docNums.push_back(rand() % 37 + 7);
        }

        Answer answer;
        CreateMultiSegmentsData(docNums, indexConfig, answer);
        shared_ptr<InvertedIndexReaderImpl> indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    }
}

void PackIndexReaderTest::InitSchemaWithOnePackageConfig(uint32_t fieldNum, InvertedIndexType indexType,
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

shared_ptr<indexlibv2::config::InvertedIndexConfig> PackIndexReaderTest::CreateIndexConfig(optionflag_t optionFlag,
                                                                                           bool hasHighFreq,
                                                                                           bool hasSectionAttribute,
                                                                                           bool multiSharding)
{
    InitSchemaWithOnePackageConfig(10, it_pack, "PackIndex");
    auto config = _schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, "PackIndex");
    assert(config);
    auto indexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(config);
    assert(indexConfig);

    hasSectionAttribute = false;
    multiSharding = false;
    if (hasHighFreq) {
        auto dictConfig = make_shared<DictionaryConfig>(VOL_NAME, VOL_CONTENT);
        indexConfig->SetDictConfig(dictConfig);
        indexConfig->SetHighFreqencyTermPostingType(indexlib::index::hp_both);
    }
    indexConfig->SetHasSectionAttributeFlag(hasSectionAttribute);
    indexConfig->SetOptionFlag(optionFlag);

    if (multiSharding) {
        // set sharding count to 3
        indexlib::index::InvertedIndexConfigCreator::CreateShardingIndexConfigs(indexConfig, 3);
    }
    return indexConfig;
}

void PackIndexReaderTest::InnerTestDumpEmptySegment(optionflag_t optionFlag, bool setMultiSharding)
{
    tearDown();
    setUp();

    vector<uint32_t> docNums;
    docNums.push_back(100);
    docNums.push_back(23);

    shared_ptr<HighFrequencyVocabulary> vol;
    shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig =
        CreateIndexConfig(optionFlag, false, true, setMultiSharding);

    docid_t currBeginDocId = 0;
    framework::SegmentInfo segmentInfo;
    segmentInfo.docCount = 0;

    vector<std::shared_ptr<indexlib::document::IndexDocument>> indexDocs;
    indexDocs.push_back(
        std::shared_ptr<indexlib::document::IndexDocument>(new IndexDocument(_testUtil->_byteSlicePool)));
    indexlib::index::InvertedTestHelper::RewriteSectionAttributeInIndexDocuments(indexDocs, _schema);

    for (size_t x = 0; x < docNums.size(); ++x) {
        auto writer = _testUtil->CreateIndexWriter(indexConfig, /*resetHighFreqVol=*/
                                                   true);
        IndexDocument& emptyIndexDoc = *indexDocs[0];
        for (uint32_t i = 0; i < docNums[x]; i++) {
            emptyIndexDoc.SetDocId(i);
            writer->EndDocument(emptyIndexDoc);
        }
        segmentInfo.docCount = docNums[x];
        writer->Seal();
        _testUtil->DumpSegment(/*segmentId=*/x, writer, segmentInfo);
        currBeginDocId += segmentInfo.docCount;
    }
    shared_ptr<InvertedIndexReaderImpl> indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
}

TEST_F(PackIndexReaderTest, TestCaseForLookUpWithOneDoc)
{
    InnerTestLookUpWithOneDoc(false, OPTION_FLAG_ALL, true, true);
    InnerTestLookUpWithOneDoc(false, OPTION_FLAG_ALL, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForLookupWithOneDocWithoutSectionAttribute)
{
    InnerTestLookUpWithOneDoc(false, OPTION_FLAG_ALL, false, true);
    InnerTestLookUpWithOneDoc(false, OPTION_FLAG_ALL, false, false);
}

TEST_F(PackIndexReaderTest, TestCaseForLookUpWithOneDocWithHighFreq)
{
    InnerTestLookUpWithOneDoc(true, OPTION_FLAG_ALL, true, true);
    InnerTestLookUpWithOneDoc(true, OPTION_FLAG_ALL, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithOneDoc)
{
    InnerTestLookUpWithOneDoc(false, NO_PAYLOAD, true, true);
    InnerTestLookUpWithOneDoc(false, NO_PAYLOAD, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithOneDocWithHighFreq)
{
    InnerTestLookUpWithOneDoc(true, NO_PAYLOAD, true, true);
    InnerTestLookUpWithOneDoc(true, NO_PAYLOAD, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithOneDoc)
{
    InnerTestLookUpWithOneDoc(false, NO_POSITION_LIST, true, true);
    InnerTestLookUpWithOneDoc(false, NO_POSITION_LIST, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithOneDocWithHighFreq)
{
    InnerTestLookUpWithOneDoc(true, NO_POSITION_LIST, true, true);
    InnerTestLookUpWithOneDoc(true, NO_POSITION_LIST, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForLookUpWithManyDoc)
{
    InnerTestLookUpWithManyDoc(false, OPTION_FLAG_ALL, true, true);
    InnerTestLookUpWithManyDoc(false, OPTION_FLAG_ALL, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForLookUpWithManyDocWithHighFreq)
{
    InnerTestLookUpWithManyDoc(true, OPTION_FLAG_ALL, true, true);
    InnerTestLookUpWithManyDoc(true, OPTION_FLAG_ALL, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForLookupWithoutTf)
{
    InnerTestLookUpWithManyDoc(false, NO_TERM_FREQUENCY, true, true);
    InnerTestLookUpWithManyDoc(false, NO_TERM_FREQUENCY, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForLookupWithoutTfWithHighFreq)
{
    InnerTestLookUpWithManyDoc(true, NO_TERM_FREQUENCY, true, true);
    InnerTestLookUpWithManyDoc(true, NO_TERM_FREQUENCY, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithManyDoc)
{
    InnerTestLookUpWithManyDoc(false, NO_PAYLOAD, true, true);
    InnerTestLookUpWithManyDoc(false, NO_PAYLOAD, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithManyDocWithHighFreq)
{
    InnerTestLookUpWithManyDoc(true, NO_PAYLOAD, true, true);
    InnerTestLookUpWithManyDoc(true, NO_PAYLOAD, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithManyDoc)
{
    InnerTestLookUpWithManyDoc(false, NO_POSITION_LIST, true, true);
    InnerTestLookUpWithManyDoc(false, NO_POSITION_LIST, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithManyDocWithHighFreq)
{
    InnerTestLookUpWithManyDoc(true, NO_POSITION_LIST, true, true);
    InnerTestLookUpWithManyDoc(true, NO_POSITION_LIST, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForLookUpWithMultiSegment)
{
    InnerTestLookUpWithMultiSegment(false, OPTION_FLAG_ALL, true);
    InnerTestLookUpWithMultiSegment(false, OPTION_FLAG_ALL, false);
}

TEST_F(PackIndexReaderTest, TestCaseForLookUpWithMultiSegmentWithHighFreq)
{
    InnerTestLookUpWithMultiSegment(true, OPTION_FLAG_ALL, true);
    InnerTestLookUpWithMultiSegment(true, OPTION_FLAG_ALL, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithMultiSegment)
{
    InnerTestLookUpWithMultiSegment(false, NO_PAYLOAD, true);
    InnerTestLookUpWithMultiSegment(false, NO_PAYLOAD, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithMultiSegmentWithHighFreq)
{
    InnerTestLookUpWithMultiSegment(true, NO_PAYLOAD, true);
    InnerTestLookUpWithMultiSegment(true, NO_PAYLOAD, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithMultiSegment)
{
    InnerTestLookUpWithMultiSegment(false, NO_POSITION_LIST, true);
    InnerTestLookUpWithMultiSegment(false, NO_POSITION_LIST, false);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithMultiSegmentWithHighFreq)
{
    InnerTestLookUpWithMultiSegment(true, NO_POSITION_LIST, true);
    InnerTestLookUpWithMultiSegment(true, NO_POSITION_LIST, false);
}

TEST_F(PackIndexReaderTest, TestCaseForTfBitmapOneDoc)
{
    InnerTestLookUpWithOneDoc(true, OPTION_FLAG_ALL | of_tf_bitmap, true, true);
    InnerTestLookUpWithOneDoc(true, NO_PAYLOAD | of_tf_bitmap, true, true);

    InnerTestLookUpWithOneDoc(true, OPTION_FLAG_ALL | of_tf_bitmap, true, false);
    InnerTestLookUpWithOneDoc(true, NO_PAYLOAD | of_tf_bitmap, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForTfBitmapManyDoc_1)
{
    InnerTestLookUpWithManyDoc(true, OPTION_FLAG_ALL | of_tf_bitmap, true, true);
    InnerTestLookUpWithManyDoc(true, NO_PAYLOAD | of_tf_bitmap, true, true);
}

TEST_F(PackIndexReaderTest, TestCaseForTfBitmapManyDoc_2)
{
    InnerTestLookUpWithManyDoc(true, OPTION_FLAG_ALL | of_tf_bitmap, true, false);
    InnerTestLookUpWithManyDoc(true, NO_PAYLOAD | of_tf_bitmap, true, false);
}

TEST_F(PackIndexReaderTest, TestCaseForTfBitmapMultiSegment)
{
    InnerTestLookUpWithMultiSegment(true, OPTION_FLAG_ALL | of_tf_bitmap, true);
    InnerTestLookUpWithMultiSegment(true, NO_PAYLOAD | of_tf_bitmap, true);
    InnerTestLookUpWithMultiSegment(true, OPTION_FLAG_ALL | of_tf_bitmap, false);
    InnerTestLookUpWithMultiSegment(true, NO_PAYLOAD | of_tf_bitmap, false);
}

TEST_F(PackIndexReaderTest, TestCaseForDumpEmptySegment)
{
    InnerTestDumpEmptySegment(OPTION_FLAG_ALL, false);
    InnerTestDumpEmptySegment(OPTION_FLAG_ALL, true);
}

TEST_F(PackIndexReaderTest, TestCaseForNoPositionListDumpEmptySegment)
{
    InnerTestDumpEmptySegment(NO_POSITION_LIST, false);
    InnerTestDumpEmptySegment(NO_POSITION_LIST, true);
}

}} // namespace indexlibv2::index
