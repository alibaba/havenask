#define TEXT_INDEX_READER_TEST
#define INDEX_ENGINE_UNITTEST

#include "autil/TimeUtility.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/test/index_document_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/payload_checker.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/NumericUtil.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {

#define VOL_NAME "reader_vol1"
#define VOL_CONTENT "token0"

class TextIndexReaderTest : public INDEXLIB_TESTBASE
{
public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 10240;

public:
    TextIndexReaderTest()
    {
        mVol.reset(new HighFrequencyVocabulary);
        mVol->Init("text", it_text, VOL_CONTENT, {});
    }

    ~TextIndexReaderTest() {}

public:
    typedef IndexDocumentMaker::Answer Answer;
    typedef IndexDocumentMaker::PostingAnswerMap PostingAnswerMap;
    typedef IndexDocumentMaker::KeyAnswer KeyAnswer;
    typedef IndexDocumentMaker::KeyAnswerInDoc KeyAnswerInDoc;

    DECLARE_CLASS_NAME(TextIndexReaderTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForLookUpWithOneDoc()
    {
        TestLookUpWithOneDoc(OPTION_FLAG_ALL);
        TestLookUpWithOneDoc(OPTION_FLAG_ALL, true);
    }

    void TestCaseForNoPayloadLookUpWithOneDoc()
    {
        TestLookUpWithOneDoc(NO_PAYLOAD);
        TestLookUpWithOneDoc(NO_PAYLOAD, true);
    }

    void TestCaseForNoPositionListLookUpWithOneDoc()
    {
        TestLookUpWithOneDoc(NO_POSITION_LIST);
        TestLookUpWithOneDoc(NO_POSITION_LIST, true);
    }

    void TestCaseForLookUpWithManyDoc()
    {
        TestLookUpWithManyDoc(OPTION_FLAG_ALL);
        TestLookUpWithManyDoc(OPTION_FLAG_ALL, true);
    }

    void TestCaseForNoPayloadLookUpWithManyDoc()
    {
        TestLookUpWithManyDoc(NO_PAYLOAD);
        TestLookUpWithManyDoc(NO_PAYLOAD, true);
    }

    void TestCaseForNoPositionListLookUpWithManyDoc()
    {
        TestLookUpWithManyDoc(NO_POSITION_LIST);
        TestLookUpWithManyDoc(NO_POSITION_LIST, true);
    }

    void TestCaseForLookUpWithMultiSegment()
    {
        TestLookUpWithMultiSegment(OPTION_FLAG_ALL);
        TestLookUpWithMultiSegment(OPTION_FLAG_ALL, true);
    }

    void TestCaseForNoPayloadLookUpWithMultiSegment()
    {
        TestLookUpWithMultiSegment(NO_PAYLOAD);
        TestLookUpWithMultiSegment(NO_PAYLOAD, true);
    }

    void TestCaseForNoPositionListLookUpWithMultiSegment()
    {
        TestLookUpWithMultiSegment(NO_POSITION_LIST);
        TestLookUpWithMultiSegment(NO_POSITION_LIST, true);
    }

    void TestDumpEmptySegment(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        tearDown();
        setUp();

        vector<uint32_t> docNums;
        docNums.push_back(100);
        docNums.push_back(23);

        SegmentInfos segmentInfos;
        IndexConfigPtr indexConfig = CreateIndexConfig(hasHighFreq);

        docid_t currBeginDocId = 0;
        SegmentInfo segmentInfo;
        segmentInfo.docCount = 0;

        Pool pool;
        for (size_t x = 0; x < docNums.size(); ++x) {
            NormalIndexWriter writer(INVALID_SEGMENTID, HASHMAP_INIT_SIZE, IndexPartitionOptions());
            writer.Init(indexConfig, NULL);

            IndexDocument emptyIndexDoc(&pool);
            for (uint32_t i = 0; i < docNums[x]; i++) {
                emptyIndexDoc.SetDocId(i);
                writer.EndDocument(emptyIndexDoc);
            }

            segmentInfo.docCount = docNums[x];

            writer.EndSegment();
            DumpSegment(x, segmentInfo, writer);

            currBeginDocId += segmentInfo.docCount;
        }

        TextIndexReader indexReader;

        PartitionDataPtr partData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), docNums.size());
        indexReader.Open(indexConfig, partData);
    }

    void TestCaseForDumpEmptySegment()
    {
        TestDumpEmptySegment(OPTION_FLAG_ALL);
        TestDumpEmptySegment(OPTION_FLAG_ALL, true);
    }

    void TestCaseForNoPositionListDumpEmptySegment()
    {
        TestDumpEmptySegment(NO_POSITION_LIST);
        TestDumpEmptySegment(NO_POSITION_LIST, true);
    }

private:
    void TestLookUpWithManyDoc(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        vector<uint32_t> docNums;
        docNums.push_back(97);

        IndexConfigPtr indexConfig = CreateIndexConfig(hasHighFreq);
        Answer answer;
        CreateMultiSegmentsData(docNums, indexConfig, answer);

        TextIndexReaderPtr indexReader = CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer, false);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer, true);
    }

    void CreateMultiSegmentsData(const vector<uint32_t>& docNums, const IndexConfigPtr& indexConfig, Answer& answer)
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
                              const IndexConfigPtr& indexConfig, Answer& answer)
    {
        SegmentInfo segmentInfo;
        segmentInfo.docCount = 0;

        NormalIndexWriter writer(INVALID_SEGMENTID, HASHMAP_INIT_SIZE, IndexPartitionOptions());
        writer.Init(indexConfig, NULL);

        vector<IndexDocumentPtr> indexDocs;
        Pool pool;
        IndexDocumentMaker::MakeIndexDocuments(&pool, indexDocs, docCount, baseDocId, &answer);
        IndexDocumentMaker::AddDocsToWriter(indexDocs, writer);
        segmentInfo.docCount = docCount;

        writer.EndSegment();
        DumpSegment(segId, segmentInfo, writer);
        indexDocs.clear();
    }

    void CheckIndexReaderLookup(const vector<uint32_t>& docNums, TextIndexReaderPtr& indexReader,
                                const IndexConfigPtr& indexConfig, Answer& answer, bool usePool = true)
    {
        optionflag_t optionFlag = indexConfig->GetOptionFlag();

        INDEXLIB_TEST_EQUAL(indexReader->HasTermPayload(), (optionFlag & of_term_payload) == of_term_payload);
        INDEXLIB_TEST_EQUAL(indexReader->HasDocPayload(), (optionFlag & of_doc_payload) == of_doc_payload);
        INDEXLIB_TEST_EQUAL(indexReader->HasPositionPayload(),
                            (optionFlag & of_position_payload) == of_position_payload);

        const PostingAnswerMap& postingAnswerMap = answer.postingAnswerMap;
        bool hasHighFreq = (indexConfig->GetDictConfig() != NULL);
        PostingAnswerMap::const_iterator keyIter = postingAnswerMap.begin();
        for (; keyIter != postingAnswerMap.end(); ++keyIter) {
            Pool pool;
            Pool* pPool = usePool ? &pool : NULL;

            string key = keyIter->first;
            index::Term term(key, "");
            PostingIterator* iter = indexReader->Lookup(term, 1000, pt_default, pPool).ValueOrThrow();
            INDEXLIB_TEST_TRUE(iter != NULL);

            bool hasPos = iter->HasPosition();
            if (hasHighFreq && mVol->Lookup(term.GetWord())) {
                PostingIterator* bitmapIt = indexReader->Lookup(term, 1000, pt_bitmap, pPool).ValueOrThrow();

                INDEXLIB_TEST_EQUAL(bitmapIt->GetType(), pi_bitmap);
                BitmapPostingIterator* bmIt = dynamic_cast<BitmapPostingIterator*>(bitmapIt);
                INDEXLIB_TEST_TRUE(bmIt != NULL);
                INDEXLIB_TEST_EQUAL(bmIt->HasPosition(), false);
                const KeyAnswer& keyAnswer = keyIter->second;
                uint32_t docCount = keyAnswer.docIdList.size();
                docid_t docId = 0;
                for (uint32_t i = 0; i < docCount; ++i) {
                    docId = bmIt->SeekDoc(docId);
                    INDEXLIB_TEST_EQUAL(docId, keyAnswer.docIdList[i]);
                }
                IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, bitmapIt);
            } else {
                INDEXLIB_TEST_EQUAL(iter->HasPosition(), (optionFlag & of_position_list) != 0);
            }
            const KeyAnswer& keyAnswer = keyIter->second;
            uint32_t docCount = keyAnswer.docIdList.size();
            docid_t docId = 0;

            TermMeta* tm = iter->GetTermMeta();
            if (optionFlag & of_term_payload) {
                INDEXLIB_TEST_EQUAL(keyAnswer.termPayload, tm->GetPayload());
            }
            INDEXLIB_TEST_EQUAL(keyAnswer.totalTF, tm->GetTotalTermFreq());
            INDEXLIB_TEST_EQUAL((df_t)keyAnswer.docIdList.size(), tm->GetDocFreq());

            for (uint32_t i = 0; i < docCount; i++) {
                docId = iter->SeekDoc(docId);
                INDEXLIB_TEST_EQUAL(docId, keyAnswer.docIdList[i]);

                TermMatchData tmd;
                if (optionFlag & of_doc_payload) {
                    tmd.SetHasDocPayload(true);
                }
                iter->Unpack(tmd);

                if (!hasPos) {
                    continue;
                }

                INDEXLIB_TEST_EQUAL(keyAnswer.tfList[i], tmd.GetTermFreq());
                const KeyAnswerInDoc& answerInDoc = keyAnswer.inDocAnswers.find(docId)->second;

                if (optionFlag & of_doc_payload) {
                    INDEXLIB_TEST_EQUAL(answerInDoc.docPayload, tmd.GetDocPayload());
                }

                std::shared_ptr<InDocPositionIterator> inDocPosIter = tmd.GetInDocPositionIterator();
                pos_t pos = 0;
                uint32_t posCount = answerInDoc.positionList.size();
                for (uint32_t j = 0; j < posCount; j++) {
                    pos = inDocPosIter->SeekPosition(pos);
                    INDEXLIB_TEST_EQUAL(answerInDoc.positionList[j], pos);

                    if (optionFlag & of_position_payload) {
                        pospayload_t posPayload = inDocPosIter->GetPosPayload();
                        INDEXLIB_TEST_EQUAL(answerInDoc.posPayloadList[j], posPayload);
                    }
                }
            }
            IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, iter);
        }
    }

    TextIndexReaderPtr CreateIndexReader(const vector<uint32_t>& docNums, const IndexConfigPtr& indexConfig)
    {
        TextIndexReaderPtr indexReader(new TextIndexReader);

        uint32_t segCount = docNums.size();
        PartitionDataPtr partData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
        indexReader->Open(indexConfig, partData);

        return indexReader;
    }

    void TestLookUpWithOneDoc(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        vector<uint32_t> docNums;
        docNums.push_back(1);

        IndexConfigPtr indexConfig = CreateIndexConfig(hasHighFreq);
        Answer answer;
        CreateMultiSegmentsData(docNums, indexConfig, answer);
        TextIndexReaderPtr indexReader = CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    }

    void TestLookUpWithMultiSegment(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        IndexConfigPtr indexConfig = CreateIndexConfig(hasHighFreq);
        const int32_t segmentCounts[] = {1, 4, 7, 9};
        for (size_t j = 0; j < sizeof(segmentCounts) / sizeof(int32_t); ++j) {
            vector<uint32_t> docNums;
            for (int32_t i = 0; i < segmentCounts[j]; ++i) {
                docNums.push_back(rand() % 37 + 7);
            }

            Answer answer;
            CreateMultiSegmentsData(docNums, indexConfig, answer);
            TextIndexReaderPtr indexReader = CreateIndexReader(docNums, indexConfig);
            CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
        }
    }

    void DumpSegment(uint32_t segId, const SegmentInfo& segmentInfo, NormalIndexWriter& writer)
    {
        stringstream ss;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0";
        DirectoryPtr segDirectory = GET_PARTITION_DIRECTORY()->MakeDirectory(ss.str());
        DirectoryPtr indexDirectory = segDirectory->MakeDirectory(INDEX_DIR_NAME);

        writer.Dump(indexDirectory, &mSimplePool);
        segmentInfo.Store(segDirectory);
    }

    SingleFieldIndexConfigPtr CreateIndexConfig(bool hasHighFreq = false)
    {
        SingleFieldIndexConfigPtr indexConfig;
        IndexDocumentMaker::CreateSingleFieldIndexConfig(indexConfig);
        if (hasHighFreq) {
            std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig(VOL_NAME, VOL_CONTENT));
            indexConfig->SetDictConfig(dictConfig);
            indexConfig->SetHighFreqencyTermPostingType(hp_both);
        }
        return indexConfig;
    }

private:
    SimplePool mSimplePool;
    std::shared_ptr<HighFrequencyVocabulary> mVol;
};

INDEXLIB_UNIT_TEST_CASE(TextIndexReaderTest, TestCaseForLookUpWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(TextIndexReaderTest, TestCaseForNoPayloadLookUpWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(TextIndexReaderTest, TestCaseForNoPositionListLookUpWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(TextIndexReaderTest, TestCaseForLookUpWithManyDoc);
INDEXLIB_UNIT_TEST_CASE(TextIndexReaderTest, TestCaseForNoPayloadLookUpWithManyDoc);
INDEXLIB_UNIT_TEST_CASE(TextIndexReaderTest, TestCaseForNoPositionListLookUpWithManyDoc);
INDEXLIB_UNIT_TEST_CASE(TextIndexReaderTest, TestCaseForLookUpWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(TextIndexReaderTest, TestCaseForNoPayloadLookUpWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(TextIndexReaderTest, TestCaseForNoPositionListLookUpWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(TextIndexReaderTest, TestCaseForDumpEmptySegment);
INDEXLIB_UNIT_TEST_CASE(TextIndexReaderTest, TestCaseForNoPositionListDumpEmptySegment);
}} // namespace indexlib::index
