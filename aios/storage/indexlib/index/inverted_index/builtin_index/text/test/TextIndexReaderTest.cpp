#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/InvertedIndexReaderImpl.h"
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/test_util/InvertedTestUtil.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/inverted_index/config/test/InvertedIndexConfigCreator.h"
#include "indexlib/index/inverted_index/test/InvertedTestHelper.h"
#include "indexlib/util/NumericUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {
namespace {
using indexlibv2::framework::SegmentInfo;
} // namespace

#define VOL_NAME "reader_vol1"
#define VOL_CONTENT "token0"

class TextIndexReaderTest : public TESTBASE
{
public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 10240;

    TextIndexReaderTest()
    {
        _vol.reset(new config::HighFrequencyVocabulary);
        _vol->Init("text", it_text, VOL_CONTENT, {});
    }

    ~TextIndexReaderTest() {}

    typedef InvertedTestHelper::Answer Answer;
    typedef InvertedTestHelper::PostingAnswerMap PostingAnswerMap;
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

    void CreateMultiSegmentsData(const std::vector<uint32_t>& docNums,
                                 const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
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

    void CreateOneSegmentData(segmentid_t segId, uint32_t docCount, docid_t baseDocId,
                              const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                              Answer& answer)
    {
        SegmentInfo segmentInfo;
        segmentInfo.docCount = 0;

        auto writer = _testUtil->CreateIndexWriter(indexConfig,
                                                   /*resetHighFreqVol=*/false);

        std::vector<std::shared_ptr<document::IndexDocument>> indexDocs;
        autil::mem_pool::Pool pool;
        InvertedTestHelper::MakeIndexDocuments(&pool, indexDocs, docCount, baseDocId, &answer);

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

    void CheckIndexReaderLookup(const std::vector<uint32_t>& docNums,
                                const std::shared_ptr<InvertedIndexReaderImpl>& indexReader,
                                const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                Answer& answer, bool usePool = true)
    {
        optionflag_t optionFlag = indexConfig->GetOptionFlag();

        ASSERT_EQ(indexReader->HasTermPayload(), (optionFlag & of_term_payload) == of_term_payload);
        ASSERT_EQ(indexReader->HasDocPayload(), (optionFlag & of_doc_payload) == of_doc_payload);
        ASSERT_EQ(indexReader->HasPositionPayload(), (optionFlag & of_position_payload) == of_position_payload);

        const PostingAnswerMap& postingAnswerMap = answer.postingAnswerMap;
        bool hasHighFreq = (indexConfig->GetDictConfig() != nullptr);
        PostingAnswerMap::const_iterator keyIter = postingAnswerMap.begin();
        for (; keyIter != postingAnswerMap.end(); ++keyIter) {
            autil::mem_pool::Pool pool;
            autil::mem_pool::Pool* pPool = usePool ? &pool : nullptr;

            std::string key = keyIter->first;
            index::Term term(key, "");
            PostingIterator* iter = indexReader->Lookup(term, 1000, pt_default, pPool).ValueOrThrow();
            ASSERT_TRUE(iter != nullptr);

            bool hasPos = iter->HasPosition();
            if (hasHighFreq && _vol->Lookup(term.GetWord())) {
                PostingIterator* bitmapIt = indexReader->Lookup(term, 1000, pt_bitmap, pPool).ValueOrThrow();

                ASSERT_EQ(bitmapIt->GetType(), pi_bitmap);
                BitmapPostingIterator* bmIt = dynamic_cast<BitmapPostingIterator*>(bitmapIt);
                ASSERT_TRUE(bmIt != nullptr);
                ASSERT_EQ(bmIt->HasPosition(), false);
                const KeyAnswer& keyAnswer = keyIter->second;
                uint32_t docCount = keyAnswer.docIdList.size();
                docid_t docId = 0;
                for (uint32_t i = 0; i < docCount; ++i) {
                    docId = bmIt->SeekDoc(docId);
                    ASSERT_EQ(docId, keyAnswer.docIdList[i]);
                }
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
            ASSERT_EQ(keyAnswer.totalTF, tm->GetTotalTermFreq());
            ASSERT_EQ((df_t)keyAnswer.docIdList.size(), tm->GetDocFreq());

            for (uint32_t i = 0; i < docCount; i++) {
                docId = iter->SeekDoc(docId);
                ASSERT_EQ(docId, keyAnswer.docIdList[i]);

                TermMatchData tmd;
                if (optionFlag & of_doc_payload) {
                    tmd.SetHasDocPayload(true);
                }
                iter->Unpack(tmd);

                if (!hasPos) {
                    continue;
                }

                ASSERT_EQ(keyAnswer.tfList[i], tmd.GetTermFreq());
                const KeyAnswerInDoc& answerInDoc = keyAnswer.inDocAnswers.find(docId)->second;

                if (optionFlag & of_doc_payload) {
                    ASSERT_EQ(answerInDoc.docPayload, tmd.GetDocPayload());
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
                }
            }
            IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, iter);
        }
    }

    void TestLookUpWithManyDoc(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        std::vector<uint32_t> docNums;
        docNums.push_back(97);

        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig = CreateIndexConfig(hasHighFreq);
        Answer answer;
        CreateMultiSegmentsData(docNums, indexConfig, answer);

        std::shared_ptr<InvertedIndexReaderImpl> indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer, false);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer, true);
    }

    void TestDumpEmptySegment(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        tearDown();
        setUp();

        std::vector<uint32_t> docNums;
        docNums.push_back(100);
        docNums.push_back(23);

        std::vector<SegmentInfo> segmentInfos;
        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig = CreateIndexConfig(hasHighFreq);

        docid_t currBeginDocId = 0;
        SegmentInfo segmentInfo;
        segmentInfo.docCount = 0;

        autil::mem_pool::Pool pool;
        for (size_t x = 0; x < docNums.size(); ++x) {
            auto writer = _testUtil->CreateIndexWriter(indexConfig, /*resetHighFreqVol=*/
                                                       true);

            document::IndexDocument emptyIndexDoc(&pool);
            for (uint32_t i = 0; i < docNums[x]; i++) {
                emptyIndexDoc.SetDocId(i);
                writer->EndDocument(emptyIndexDoc);
            }

            segmentInfo.docCount = docNums[x];

            writer->Seal();
            _testUtil->DumpSegment(x, writer, segmentInfo);

            currBeginDocId += segmentInfo.docCount;
        }

        ASSERT_TRUE(_testUtil->CreateIndexReader(docNums, indexConfig));
    }

    void TestLookUpWithOneDoc(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        std::vector<uint32_t> docNums;
        docNums.push_back(1);

        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig = CreateIndexConfig(hasHighFreq);
        Answer answer;
        CreateMultiSegmentsData(docNums, indexConfig, answer);
        std::shared_ptr<InvertedIndexReaderImpl> indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    }

    void TestLookUpWithMultiSegment(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig = CreateIndexConfig(hasHighFreq);
        const int32_t segmentCounts[] = {1, 4, 7, 9};
        for (size_t j = 0; j < sizeof(segmentCounts) / sizeof(int32_t); ++j) {
            std::vector<uint32_t> docNums;
            for (int32_t i = 0; i < segmentCounts[j]; ++i) {
                docNums.push_back(rand() % 37 + 7);
            }

            Answer answer;
            CreateMultiSegmentsData(docNums, indexConfig, answer);
            std::shared_ptr<InvertedIndexReaderImpl> indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
            CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
        }
    }

    std::shared_ptr<indexlibv2::config::SingleFieldIndexConfig> CreateIndexConfig(bool hasHighFreq = false)
    {
        auto indexConfig = InvertedIndexConfigCreator::CreateSingleFieldIndexConfig();
        if (hasHighFreq) {
            std::shared_ptr<config::DictionaryConfig> dictConfig(new config::DictionaryConfig(VOL_NAME, VOL_CONTENT));
            indexConfig->SetDictConfig(dictConfig);
            indexConfig->SetHighFreqencyTermPostingType(indexlib::index::hp_both);
        }
        return indexConfig;
    }

private:
    std::shared_ptr<InvertedTestUtil> _testUtil;
    std::shared_ptr<config::HighFrequencyVocabulary> _vol;

    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, TextIndexReaderTest);

TEST_F(TextIndexReaderTest, testCaseForLookUpWithOneDoc)
{
    TestLookUpWithOneDoc(OPTION_FLAG_ALL);
    TestLookUpWithOneDoc(OPTION_FLAG_ALL, true);
}

TEST_F(TextIndexReaderTest, testCaseForNoPayloadLookUpWithOneDoc)
{
    TestLookUpWithOneDoc(NO_PAYLOAD);
    TestLookUpWithOneDoc(NO_PAYLOAD, true);
}

TEST_F(TextIndexReaderTest, testCaseForNoPositionListLookUpWithOneDoc)
{
    TestLookUpWithOneDoc(NO_POSITION_LIST);
    TestLookUpWithOneDoc(NO_POSITION_LIST, true);
}

TEST_F(TextIndexReaderTest, testCaseForLookUpWithManyDoc)
{
    TestLookUpWithManyDoc(OPTION_FLAG_ALL);
    TestLookUpWithManyDoc(OPTION_FLAG_ALL, true);
}

TEST_F(TextIndexReaderTest, testCaseForNoPayloadLookUpWithManyDoc)
{
    TestLookUpWithManyDoc(NO_PAYLOAD);
    TestLookUpWithManyDoc(NO_PAYLOAD, true);
}

TEST_F(TextIndexReaderTest, testCaseForNoPositionListLookUpWithManyDoc)
{
    TestLookUpWithManyDoc(NO_POSITION_LIST);
    TestLookUpWithManyDoc(NO_POSITION_LIST, true);
}

TEST_F(TextIndexReaderTest, testCaseForLookUpWithMultiSegment)
{
    TestLookUpWithMultiSegment(OPTION_FLAG_ALL);
    TestLookUpWithMultiSegment(OPTION_FLAG_ALL, true);
}

TEST_F(TextIndexReaderTest, testCaseForNoPayloadLookUpWithMultiSegment)
{
    TestLookUpWithMultiSegment(NO_PAYLOAD);
    TestLookUpWithMultiSegment(NO_PAYLOAD, true);
}

TEST_F(TextIndexReaderTest, testCaseForNoPositionListLookUpWithMultiSegment)
{
    TestLookUpWithMultiSegment(NO_POSITION_LIST);
    TestLookUpWithMultiSegment(NO_POSITION_LIST, true);
}

TEST_F(TextIndexReaderTest, testCaseForDumpEmptySegment)
{
    TestDumpEmptySegment(OPTION_FLAG_ALL);
    TestDumpEmptySegment(OPTION_FLAG_ALL, true);
}

TEST_F(TextIndexReaderTest, testCaseForNoPositionListDumpEmptySegment)
{
    TestDumpEmptySegment(NO_POSITION_LIST);
    TestDumpEmptySegment(NO_POSITION_LIST, true);
}

} // namespace indexlib::index
