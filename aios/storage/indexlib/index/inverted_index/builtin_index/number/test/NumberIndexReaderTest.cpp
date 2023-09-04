#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/framework/SegmentInfo.h"
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
#define VOL_CONTENT "0"

class NumberIndexReaderTest : public TESTBASE
{
public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 10240;

    template <typename IndexReaderType_, typename IndexWriterType_, FieldType fieldType_>
    class TestTraits
    {
    public:
        typedef IndexReaderType_ IndexReaderType;
        typedef shared_ptr<IndexReaderType> IndexReaderPtrType;
        typedef IndexWriterType_ IndexWriterType;
        typedef shared_ptr<IndexWriterType> IndexWriterPtrType;
        static const FieldType fieldType = fieldType_;
    };

    typedef indexlib::index::InvertedTestHelper::Answer Answer;
    typedef indexlib::index::InvertedTestHelper::PostingAnswerMap PostingAnswerMap;
    typedef indexlib::index::InvertedTestHelper::KeyAnswer KeyAnswer;
    typedef indexlib::index::InvertedTestHelper::KeyAnswerInDoc KeyAnswerInDoc;

    NumberIndexReaderTest()
    {
        _vol.reset(new HighFrequencyVocabulary);
        _vol->Init("number", it_number_int8, VOL_CONTENT, {});
    }

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

#define DEFINE_TEST_CASE(traitsType)                                                                                   \
    TEST_F(NumberIndexReaderTest, TestCaseForNoPositionListLookUpWithOneDoc##traitsType)                               \
    {                                                                                                                  \
        TestLookUpWithOneDoc<traitsType>(NO_POSITION_LIST);                                                            \
        TestLookUpWithOneDoc<traitsType>(NO_POSITION_LIST, true);                                                      \
    }                                                                                                                  \
    TEST_F(NumberIndexReaderTest, TestCaseForNoPositionListLookUpWithManyDoc##traitsType)                              \
    {                                                                                                                  \
        TestLookUpWithManyDoc<traitsType>(NO_POSITION_LIST);                                                           \
        TestLookUpWithManyDoc<traitsType>(NO_POSITION_LIST, true);                                                     \
    }                                                                                                                  \
    TEST_F(NumberIndexReaderTest, TestCaseForNoPositionListLookUpWithMultiSegment##traitsType)                         \
    {                                                                                                                  \
        TestLookUpWithMultiSegment<traitsType>(NO_POSITION_LIST);                                                      \
        TestLookUpWithMultiSegment<traitsType>(NO_POSITION_LIST, true);                                                \
    }                                                                                                                  \
    TEST_F(NumberIndexReaderTest, TestCaseForNoPositionListDumpEmptySegment##traitsType)                               \
    {                                                                                                                  \
        TestDumpEmptySegment<traitsType>(NO_POSITION_LIST);                                                            \
        TestDumpEmptySegment<traitsType>(NO_POSITION_LIST, true);                                                      \
    }

#define DEFINE_NUMBER_INDEX_READER_INT_TYPE_CASE(type)                                                                 \
    typedef NumberIndexReaderTest::TestTraits<InvertedIndexReaderImpl, InvertedMemIndexer, ft_int##type>               \
        TestTraitsInt##type;                                                                                           \
    DEFINE_TEST_CASE(TestTraitsInt##type)

#define DEFINE_NUMBER_INDEX_READER_UINT_TYPE_CASE(type)                                                                \
    typedef NumberIndexReaderTest::TestTraits<InvertedIndexReaderImpl, InvertedMemIndexer, ft_uint##type>              \
        TestTraitsUInt##type;                                                                                          \
    DEFINE_TEST_CASE(TestTraitsUInt##type)

private:
    template <typename TestTraitsType>
    void TestLookUpWithManyDoc(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        vector<uint32_t> docNums;
        docNums.push_back(97);

        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig =
            CreateIndexConfig<TestTraitsType>(optionFlag, hasHighFreq);
        Answer answer;
        CreateMultiSegmentsData<TestTraitsType>(docNums, indexConfig, answer);
        auto indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    }

    template <typename TestTraitsType>
    void TestLookUpWithOneDoc(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        vector<uint32_t> docNums;
        docNums.push_back(1);

        auto indexConfig = CreateIndexConfig<TestTraitsType>(optionFlag, hasHighFreq);
        Answer answer;
        CreateMultiSegmentsData<TestTraitsType>(docNums, indexConfig, answer);
        auto indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    }

    template <typename TestTraitsType>
    void TestLookUpWithMultiSegment(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig =
            CreateIndexConfig<TestTraitsType>(optionFlag, hasHighFreq);
        const int32_t segmentCounts[] = {1, 4, 7, 9};
        for (size_t j = 0; j < sizeof(segmentCounts) / sizeof(int32_t); ++j) {
            vector<uint32_t> docNums;
            for (int32_t i = 0; i < segmentCounts[j]; ++i) {
                docNums.push_back(rand() % 37 + 7);
            }
            Answer answer;
            CreateMultiSegmentsData<TestTraitsType>(docNums, indexConfig, answer);
            auto indexReader = _testUtil->CreateIndexReader(docNums, indexConfig);
            CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
        }
    }

    template <typename TestTraitsType>
    void TestDumpEmptySegment(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        tearDown();
        setUp();

        vector<uint32_t> docNums;
        docNums.push_back(100);
        docNums.push_back(23);

        auto indexConfig = CreateIndexConfig<TestTraitsType>(optionFlag, hasHighFreq);

        docid_t currBeginDocId = 0;
        framework::SegmentInfo segmentInfo;
        segmentInfo.docCount = 0;

        vector<segmentid_t> segIds;
        for (size_t x = 0; x < docNums.size(); ++x) {
            auto writer = _testUtil->CreateIndexWriter(indexConfig, /*resetHighFreqVol=*/
                                                       true);
            IndexDocument emptyIndexDoc(_testUtil->_byteSlicePool);
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

    template <typename TestTraitsType>
    void CreateMultiSegmentsData(const vector<uint32_t>& docNums,
                                 const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                 Answer& answer)
    {
        tearDown();
        setUp();
        docid_t baseDocId = 0;
        for (size_t i = 0; i < docNums.size(); ++i) {
            CreateOneSegmentData<TestTraitsType>(i, docNums[i], baseDocId, indexConfig, answer);
            baseDocId += docNums[i];
        }
    }

    template <typename TestTraitsType>
    void CreateOneSegmentData(segmentid_t segId, uint32_t docCount, docid_t baseDocId,
                              const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                              Answer& answer)
    {
        framework::SegmentInfo segmentInfo;
        segmentInfo.docCount = 0;
        auto writer = _testUtil->CreateIndexWriter(indexConfig,
                                                   /*resetHighFreqVol=*/false);
        Pool pool;
        vector<std::shared_ptr<indexlib::document::IndexDocument>> indexDocs;
        indexlib::index::InvertedTestHelper::MakeIndexDocuments(&pool, indexDocs, docCount, baseDocId, &answer,
                                                                tt_number);
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

    void CheckIndexReaderLookup(const vector<uint32_t>& docNums, const shared_ptr<InvertedIndexReader>& indexReader,
                                const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                Answer& answer)
    {
        optionflag_t optionFlag = indexConfig->GetOptionFlag();

        ASSERT_EQ(indexReader->HasTermPayload(), (optionFlag & of_term_payload) == of_term_payload);
        ASSERT_EQ(indexReader->HasDocPayload(), (optionFlag & of_doc_payload) == of_doc_payload);
        ASSERT_EQ(indexReader->HasPositionPayload(), (optionFlag & of_position_payload) == of_position_payload);

        const PostingAnswerMap& postingAnswerMap = answer.postingAnswerMap;
        bool hasHighFreq = (indexConfig->GetDictConfig() != nullptr);
        PostingAnswerMap::const_iterator keyIter = postingAnswerMap.begin();
        for (; keyIter != postingAnswerMap.end(); ++keyIter) {
            string key = keyIter->first;

            indexlib::index::Term term(key, "");
            shared_ptr<PostingIterator> iter(indexReader->Lookup(term).ValueOrThrow());
            ASSERT_TRUE(iter.get() != nullptr);

            bool hasPos = iter->HasPosition();
            if (hasHighFreq && _vol->Lookup(term.GetWord())) {
                std::shared_ptr<PostingIterator> bitmapIt(indexReader->Lookup(term, 1000, pt_bitmap).ValueOrThrow());

                ASSERT_EQ(bitmapIt->GetType(), pi_bitmap);
                std::shared_ptr<BitmapPostingIterator> bmIt = dynamic_pointer_cast<BitmapPostingIterator>(bitmapIt);
                ASSERT_TRUE(bmIt != nullptr);
                ASSERT_EQ(bmIt->HasPosition(), false);
                const KeyAnswer& keyAnswer = keyIter->second;
                uint32_t docCount = keyAnswer.docIdList.size();
                docid_t docId = 0;
                for (uint32_t i = 0; i < docCount; ++i) {
                    docId = bmIt->SeekDoc(docId);
                    ASSERT_EQ(docId, keyAnswer.docIdList[i]);
                }
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
        }
    }

    template <typename TestTraitsType>
    shared_ptr<indexlibv2::config::SingleFieldIndexConfig> CreateIndexConfig(optionflag_t optionFlag,
                                                                             bool hasHighFreq = false)
    {
        auto indexConfig = indexlib::index::InvertedIndexConfigCreator::CreateSingleFieldIndexConfig(
            "NumberIndex", it_number, TestTraitsType::fieldType, optionFlag);
        if (hasHighFreq) {
            std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig(VOL_NAME, VOL_CONTENT));
            indexConfig->SetDictConfig(dictConfig);
            indexConfig->SetHighFreqencyTermPostingType(indexlib::index::hp_both);
        }
        return indexConfig;
    }

private:
    shared_ptr<InvertedTestUtil> _testUtil;
    shared_ptr<HighFrequencyVocabulary> _vol;

    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, NumberIndexReaderTest);

DEFINE_NUMBER_INDEX_READER_INT_TYPE_CASE(8);
DEFINE_NUMBER_INDEX_READER_INT_TYPE_CASE(16);
DEFINE_NUMBER_INDEX_READER_INT_TYPE_CASE(32);
DEFINE_NUMBER_INDEX_READER_INT_TYPE_CASE(64);

DEFINE_NUMBER_INDEX_READER_UINT_TYPE_CASE(8);
DEFINE_NUMBER_INDEX_READER_UINT_TYPE_CASE(16);
DEFINE_NUMBER_INDEX_READER_UINT_TYPE_CASE(32);
DEFINE_NUMBER_INDEX_READER_UINT_TYPE_CASE(64);

}} // namespace indexlibv2::index
