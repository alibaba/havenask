#define NUMBER_INDEX_READER_TEST
#define INDEX_ENGINE_UNITTEST

#include "autil/TimeUtility.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/common_define.h"
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
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index {

#define VOL_NAME "reader_vol1"
#define VOL_CONTENT "0"

class NumberIndexReaderTest : public INDEXLIB_TESTBASE
{
public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 10240;

    template <typename IndexReaderType_, typename IndexWriterType_, FieldType fieldType_>
    class TestTraits
    {
    public:
        typedef IndexReaderType_ IndexReaderType;
        typedef std::shared_ptr<IndexReaderType> IndexReaderPtrType;
        typedef IndexWriterType_ IndexWriterType;
        typedef std::shared_ptr<IndexWriterType> IndexWriterPtrType;
        static const FieldType fieldType = fieldType_;
    };

public:
    NumberIndexReaderTest()
    {
        mVol.reset(new HighFrequencyVocabulary);
        mVol->Init("number", it_number_int8, VOL_CONTENT, {});
    }

    ~NumberIndexReaderTest() {}

public:
    typedef IndexDocumentMaker::Answer Answer;
    typedef IndexDocumentMaker::PostingAnswerMap PostingAnswerMap;
    typedef IndexDocumentMaker::KeyAnswer KeyAnswer;
    typedef IndexDocumentMaker::KeyAnswerInDoc KeyAnswerInDoc;

    DECLARE_CLASS_NAME(TestTraits);
    void CaseSetUp() override
    {
        mTestDir = GET_TEMP_DATA_PATH();

        mByteSlicePool = new Pool(&mAllocator, DEFAULT_CHUNK_SIZE);
        mBufferPool = new RecyclePool(&mAllocator, DEFAULT_CHUNK_SIZE);
    }

    void CaseTearDown() override
    {
        DELETE_AND_SET_NULL(mByteSlicePool);
        DELETE_AND_SET_NULL(mBufferPool);
    }

#define DEFINE_TEST_CASE(traitsType)                                                                                   \
    void TestCaseForNoPositionListLookUpWithOneDoc##traitsType()                                                       \
    {                                                                                                                  \
        TestLookUpWithOneDoc<traitsType>(NO_POSITION_LIST);                                                            \
        TestLookUpWithOneDoc<traitsType>(NO_POSITION_LIST, true);                                                      \
    }                                                                                                                  \
    void TestCaseForNoPositionListLookUpWithManyDoc##traitsType()                                                      \
    {                                                                                                                  \
        TestLookUpWithManyDoc<traitsType>(NO_POSITION_LIST);                                                           \
        TestLookUpWithManyDoc<traitsType>(NO_POSITION_LIST, true);                                                     \
    }                                                                                                                  \
    void TestCaseForNoPositionListLookUpWithMultiSegment##traitsType()                                                 \
    {                                                                                                                  \
        TestLookUpWithMultiSegment<traitsType>(NO_POSITION_LIST);                                                      \
        TestLookUpWithMultiSegment<traitsType>(NO_POSITION_LIST, true);                                                \
    }                                                                                                                  \
    void TestCaseForNoPositionListDumpEmptySegment##traitsType()                                                       \
    {                                                                                                                  \
        TestDumpEmptySegment<traitsType>(NO_POSITION_LIST);                                                            \
        TestDumpEmptySegment<traitsType>(NO_POSITION_LIST, true);                                                      \
    }

#define DEFINE_NUMBER_INDEX_READER_INT_TYPE_CASE(type)                                                                 \
    typedef TestTraits<NumberInt##type##IndexReader, NormalIndexWriter, ft_int##type> TestTraitsInt##type;             \
    DEFINE_TEST_CASE(TestTraitsInt##type)

#define DEFINE_NUMBER_INDEX_READER_UINT_TYPE_CASE(type)                                                                \
    typedef TestTraits<NumberUInt##type##IndexReader, NormalIndexWriter, ft_uint##type> TestTraitsUInt##type;          \
    DEFINE_TEST_CASE(TestTraitsUInt##type)

    DEFINE_NUMBER_INDEX_READER_INT_TYPE_CASE(8);
    DEFINE_NUMBER_INDEX_READER_INT_TYPE_CASE(16);
    DEFINE_NUMBER_INDEX_READER_INT_TYPE_CASE(32);
    DEFINE_NUMBER_INDEX_READER_INT_TYPE_CASE(64);

    DEFINE_NUMBER_INDEX_READER_UINT_TYPE_CASE(8);
    DEFINE_NUMBER_INDEX_READER_UINT_TYPE_CASE(16);
    DEFINE_NUMBER_INDEX_READER_UINT_TYPE_CASE(32);
    DEFINE_NUMBER_INDEX_READER_UINT_TYPE_CASE(64);

private:
    template <typename TestTraitsType>
    void TestLookUpWithManyDoc(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        vector<uint32_t> docNums;
        docNums.push_back(97);

        IndexConfigPtr indexConfig = CreateIndexConfig<TestTraitsType>(optionFlag, hasHighFreq);
        Answer answer;
        CreateMultiSegmentsData<TestTraitsType>(docNums, indexConfig, answer);

        typename TestTraitsType::IndexReaderPtrType indexReader =
            CreateIndexReader<TestTraitsType>(docNums, indexConfig);
        CheckIndexReaderLookup<TestTraitsType>(docNums, indexReader, indexConfig, answer);
    }

    template <typename TestTraitsType>
    void TestLookUpWithOneDoc(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        vector<uint32_t> docNums;
        docNums.push_back(1);

        IndexConfigPtr indexConfig = CreateIndexConfig<TestTraitsType>(optionFlag, hasHighFreq);
        Answer answer;
        CreateMultiSegmentsData<TestTraitsType>(docNums, indexConfig, answer);
        typename TestTraitsType::IndexReaderPtrType indexReader =
            CreateIndexReader<TestTraitsType>(docNums, indexConfig);
        CheckIndexReaderLookup<TestTraitsType>(docNums, indexReader, indexConfig, answer);
    }

    template <typename TestTraitsType>
    void TestLookUpWithMultiSegment(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        IndexConfigPtr indexConfig = CreateIndexConfig<TestTraitsType>(optionFlag, hasHighFreq);
        const int32_t segmentCounts[] = {1, 4, 7, 9};
        for (size_t j = 0; j < sizeof(segmentCounts) / sizeof(int32_t); ++j) {
            vector<uint32_t> docNums;
            for (int32_t i = 0; i < segmentCounts[j]; ++i) {
                docNums.push_back(rand() % 37 + 7);
            }

            Answer answer;
            CreateMultiSegmentsData<TestTraitsType>(docNums, indexConfig, answer);
            typename TestTraitsType::IndexReaderPtrType indexReader =
                CreateIndexReader<TestTraitsType>(docNums, indexConfig);
            CheckIndexReaderLookup<TestTraitsType>(docNums, indexReader, indexConfig, answer);
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

        SegmentInfos segmentInfos;
        IndexConfigPtr indexConfig = CreateIndexConfig<TestTraitsType>(optionFlag, hasHighFreq);

        docid_t currBeginDocId = 0;
        SegmentInfo segmentInfo;
        segmentInfo.docCount = 0;

        for (size_t x = 0; x < docNums.size(); ++x) {
            typename TestTraitsType::IndexWriterType writer(HASHMAP_INIT_SIZE, IndexPartitionOptions());
            writer.Init(indexConfig, NULL);

            IndexDocument emptyIndexDoc(mByteSlicePool);
            for (uint32_t i = 0; i < docNums[x]; i++) {
                emptyIndexDoc.SetDocId(i);
                writer.EndDocument(emptyIndexDoc);
            }

            segmentInfo.docCount = docNums[x];

            writer.EndSegment();
            DumpSegment<TestTraitsType>(x, segmentInfo, writer);

            currBeginDocId += segmentInfo.docCount;
        }

        typename TestTraitsType::IndexReaderType indexReader;
        PartitionDataPtr partData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), docNums.size());
        indexReader.Open(indexConfig, partData);
    }

    template <typename TestTraitsType>
    void CreateMultiSegmentsData(const vector<uint32_t>& docNums, const IndexConfigPtr& indexConfig, Answer& answer)
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
                              const IndexConfigPtr& indexConfig, Answer& answer)
    {
        SegmentInfo segmentInfo;
        segmentInfo.docCount = 0;

        typename TestTraitsType::IndexWriterType writer(HASHMAP_INIT_SIZE, IndexPartitionOptions());
        writer.Init(indexConfig, NULL);

        Pool pool;
        vector<IndexDocumentPtr> indexDocs;
        IndexDocumentMaker::MakeIndexDocuments(&pool, indexDocs, docCount, baseDocId, &answer, tt_number);
        IndexDocumentMaker::AddDocsToWriter(indexDocs, writer);
        segmentInfo.docCount = docCount;

        writer.EndSegment();
        DumpSegment<TestTraitsType>(segId, segmentInfo, writer);
        indexDocs.clear();
    }

    template <typename TestTraitsType>
    void CheckIndexReaderLookup(const vector<uint32_t>& docNums,
                                typename TestTraitsType::IndexReaderPtrType& indexReader,
                                const IndexConfigPtr& indexConfig, Answer& answer)
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
            string key = keyIter->first;

            index::Term term(key, "");
            std::shared_ptr<PostingIterator> iter(indexReader->Lookup(term).ValueOrThrow());
            INDEXLIB_TEST_TRUE(iter.get() != NULL);

            bool hasPos = iter->HasPosition();
            if (hasHighFreq && mVol->Lookup(term.GetWord())) {
                std::shared_ptr<PostingIterator> bitmapIt(indexReader->Lookup(term, 1000, pt_bitmap).ValueOrThrow());

                INDEXLIB_TEST_EQUAL(bitmapIt->GetType(), pi_bitmap);
                std::shared_ptr<BitmapPostingIterator> bmIt = dynamic_pointer_cast<BitmapPostingIterator>(bitmapIt);
                INDEXLIB_TEST_TRUE(bmIt != NULL);
                INDEXLIB_TEST_EQUAL(bmIt->HasPosition(), false);
                const KeyAnswer& keyAnswer = keyIter->second;
                uint32_t docCount = keyAnswer.docIdList.size();
                docid_t docId = 0;
                for (uint32_t i = 0; i < docCount; ++i) {
                    docId = bmIt->SeekDoc(docId);
                    INDEXLIB_TEST_EQUAL(docId, keyAnswer.docIdList[i]);
                }
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
        }
    }

    template <typename TestTraitsType>
    typename TestTraitsType::IndexReaderPtrType CreateIndexReader(const vector<uint32_t>& docNums,
                                                                  const IndexConfigPtr& indexConfig)
    {
        typedef typename TestTraitsType::IndexReaderType IndexReaderType;
        typename TestTraitsType::IndexReaderPtrType indexReader(new IndexReaderType);

        uint32_t segCount = docNums.size();
        PartitionDataPtr partData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
        indexReader->Open(indexConfig, partData);

        return indexReader;
    }

    template <typename TestTraitsType>
    void DumpSegment(uint32_t segId, const SegmentInfo& segmentInfo, typename TestTraitsType::IndexWriterType& writer)
    {
        stringstream ss;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0";
        DirectoryPtr segDirectory = GET_PARTITION_DIRECTORY()->MakeDirectory(ss.str());
        DirectoryPtr indexDirectory = segDirectory->MakeDirectory(INDEX_DIR_NAME);

        writer.Dump(indexDirectory, &mSimplePool);
        segmentInfo.Store(segDirectory);
    }

    template <typename TestTraitsType>
    SingleFieldIndexConfigPtr CreateIndexConfig(optionflag_t optionFlag, bool hasHighFreq = false)
    {
        SingleFieldIndexConfigPtr indexConfig;
        IndexDocumentMaker::CreateSingleFieldIndexConfig(indexConfig, "NumberIndex", it_number,
                                                         TestTraitsType::fieldType, optionFlag);
        if (hasHighFreq) {
            std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig(VOL_NAME, VOL_CONTENT));
            indexConfig->SetDictConfig(dictConfig);
            indexConfig->SetHighFreqencyTermPostingType(hp_both);
        }
        return indexConfig;
    }

private:
    string mTestDir;
    SimpleAllocator mAllocator;
    Pool* mByteSlicePool;
    RecyclePool* mBufferPool;
    SimplePool mSimplePool;
    std::shared_ptr<HighFrequencyVocabulary> mVol;
};

#define REGISTER_TEST_CASE(traitsType)                                                                                 \
    INDEXLIB_UNIT_TEST_CASE(NumberIndexReaderTest, TestCaseForNoPositionListLookUpWithOneDoc##traitsType);             \
    INDEXLIB_UNIT_TEST_CASE(NumberIndexReaderTest, TestCaseForNoPositionListLookUpWithManyDoc##traitsType);            \
    INDEXLIB_UNIT_TEST_CASE(NumberIndexReaderTest, TestCaseForNoPositionListLookUpWithMultiSegment##traitsType);       \
    INDEXLIB_UNIT_TEST_CASE(NumberIndexReaderTest, TestCaseForNoPositionListDumpEmptySegment##traitsType);

#define REGISTER_TEST_CASE_INT_TYPE(type)                                                                              \
    typedef NumberIndexReaderTest::TestTraits<NumberInt##type##IndexReader, NormalIndexWriter, ft_int##type>           \
        TestTraitsInt##type;                                                                                           \
    REGISTER_TEST_CASE(TestTraitsInt##type)

#define REGISTER_TEST_CASE_UINT_TYPE(type)                                                                             \
    typedef NumberIndexReaderTest::TestTraits<NumberUInt##type##IndexReader, NormalIndexWriter, ft_uint##type>         \
        TestTraitsUInt##type;                                                                                          \
    REGISTER_TEST_CASE(TestTraitsUInt##type)

REGISTER_TEST_CASE_INT_TYPE(8);
REGISTER_TEST_CASE_INT_TYPE(16);
REGISTER_TEST_CASE_INT_TYPE(32);
REGISTER_TEST_CASE_INT_TYPE(64);

REGISTER_TEST_CASE_UINT_TYPE(8);
REGISTER_TEST_CASE_UINT_TYPE(16);
REGISTER_TEST_CASE_UINT_TYPE(32);
REGISTER_TEST_CASE_UINT_TYPE(64);
}} // namespace indexlib::index
