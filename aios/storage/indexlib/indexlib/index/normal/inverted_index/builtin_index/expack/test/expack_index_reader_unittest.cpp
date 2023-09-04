#include "autil/TimeUtility.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/format/NormalInDocPositionIterator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/expack/test/expack_index_document_maker.h"
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

class ExpackIndexReaderTest : public INDEXLIB_TESTBASE
{
public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 10240;

public:
    ExpackIndexReaderTest()
    {
        mVol.reset(new HighFrequencyVocabulary);
        mVol->Init("expack", it_expack, VOL_CONTENT, {});
    }

    ~ExpackIndexReaderTest() {}

public:
    typedef IndexDocumentMaker::Answer Answer;
    typedef IndexDocumentMaker::PostingAnswerMap PostingAnswerMap;
    typedef IndexDocumentMaker::SectionAnswerMap SectionAnswerMap;
    typedef IndexDocumentMaker::KeyAnswer KeyAnswer;
    typedef IndexDocumentMaker::KeyAnswerInDoc KeyAnswerInDoc;

    DECLARE_CLASS_NAME(ExpackIndexReaderTest);
    void CaseSetUp() override
    {
        mTestDir = GET_TEMP_DATA_PATH();

        mByteSlicePool.reset(new Pool(&mAllocator, DEFAULT_CHUNK_SIZE));
    }

    void CaseTearDown() override
    {
        ExpackIndexDocumentMaker::CleanDir(mTestDir);
        mByteSlicePool.reset();
    }

private:
    void InnerTestDumpEmptySegment(optionflag_t optionFlag);
    void InnerTestLookUpWithManyDoc(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding);
    void InnerTestLookUpWithOneDoc(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding = false);
    void InnerTestLookUpWithMultiSegment(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding);
    void InnerTestGenFieldMapMask(const vector<string>& termFieldNames, fieldmap_t targetFieldMap,
                                  bool isSuccess = true);

    void CheckIndexReaderLookup(const vector<uint32_t>& docNums, std::shared_ptr<InvertedIndexReader>& indexReader,
                                const IndexConfigPtr& indexConfig, Answer& answer, bool usePool = false);
    void CheckSection(docid_t docId, const std::shared_ptr<InDocPositionIterator>& inDocPosIter,
                      SectionAnswerMap& sectionAnswerMap);

    std::shared_ptr<InvertedIndexReader> CreateIndexReader(const vector<uint32_t>& docNums,
                                                           const IndexConfigPtr& indexConfig);
    void CreateOneSegmentData(segmentid_t segId, uint32_t docCount, docid_t baseDocId,
                              const IndexConfigPtr& indexConfig, Answer& answer);
    void CreateMultiSegmentsData(const vector<uint32_t>& docNums, const IndexConfigPtr& indexConfig, Answer& answer);
    void EndAndDumpSegment(uint32_t segmentId, IndexWriterPtr writer, const SegmentInfo& segmentInfo);
    IndexConfigPtr CreateIndexConfig(optionflag_t optionFlag, bool hasHighFreq, bool multiSharding);
    IndexWriterPtr CreateIndexWriter(const IndexConfigPtr& indexConfig, bool resetHighFreqVol);
    std::shared_ptr<LegacyIndexAccessoryReader> CreateIndexAccessoryReader(const IndexConfigPtr& indexConfig,
                                                                           const PartitionDataPtr& partitionData);

private:
    IndexPartitionSchemaPtr mSchema;
    string mTestDir;
    SimpleAllocator mAllocator;
    SimplePool mSimplePool;
    PoolPtr mByteSlicePool;
    std::shared_ptr<HighFrequencyVocabulary> mVol;
};

void ExpackIndexReaderTest::InnerTestDumpEmptySegment(optionflag_t optionFlag)
{
    vector<uint32_t> docNums;
    docNums.push_back(100);
    docNums.push_back(23);

    SegmentInfos segmentInfos;
    std::shared_ptr<HighFrequencyVocabulary> vol;
    IndexConfigPtr indexConfig = CreateIndexConfig(optionFlag, false, false);

    docid_t currBeginDocId = 0;
    SegmentInfo segmentInfo;
    segmentInfo.docCount = 0;

    vector<document::IndexDocumentPtr> indexDocs;
    indexDocs.push_back(IndexDocumentPtr(new IndexDocument(mByteSlicePool.get())));
    IndexDocumentMaker::RewriteSectionAttributeInIndexDocuments(indexDocs, mSchema);

    for (size_t x = 0; x < docNums.size(); ++x) {
        IndexWriterPtr writer = CreateIndexWriter(indexConfig, /*resetHighFreqVol=*/true);
        IndexDocument& emptyIndexDoc = *indexDocs[0];
        for (uint32_t i = 0; i < docNums[x]; i++) {
            emptyIndexDoc.SetDocId(i);
            writer->EndDocument(emptyIndexDoc);
        }
        segmentInfo.docCount = docNums[x];
        EndAndDumpSegment(/*segmentId=*/x, writer, segmentInfo);
        currBeginDocId += segmentInfo.docCount;
    }

    ExpackIndexReader indexReader;
    PartitionDataPtr partData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), docNums.size());
    indexReader.Open(indexConfig, partData);
}

void ExpackIndexReaderTest::InnerTestLookUpWithManyDoc(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding)
{
    vector<uint32_t> docNums;
    docNums.push_back(6);

    IndexConfigPtr indexConfig = CreateIndexConfig(optionFlag, hasHighFreq, setMultiSharding);
    Answer answer;
    CreateMultiSegmentsData(docNums, indexConfig, answer);

    std::shared_ptr<InvertedIndexReader> indexReader = CreateIndexReader(docNums, indexConfig);
    CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer, true);
}

void ExpackIndexReaderTest::InnerTestLookUpWithOneDoc(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding)
{
    vector<uint32_t> docNums;
    docNums.push_back(1);

    IndexConfigPtr indexConfig = CreateIndexConfig(optionFlag, hasHighFreq, setMultiSharding);
    Answer answer;
    CreateMultiSegmentsData(docNums, indexConfig, answer);
    std::shared_ptr<InvertedIndexReader> indexReader = CreateIndexReader(docNums, indexConfig);
    CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
}

void ExpackIndexReaderTest::InnerTestLookUpWithMultiSegment(bool hasHighFreq, optionflag_t optionFlag,
                                                            bool setMultiSharding)
{
    IndexConfigPtr indexConfig = CreateIndexConfig(optionFlag, hasHighFreq, setMultiSharding);
    const int32_t segmentCounts[] = {1, 4, 9};
    for (size_t j = 0; j < sizeof(segmentCounts) / sizeof(int32_t); ++j) {
        vector<uint32_t> docNums;
        for (int32_t i = 0; i < segmentCounts[j]; ++i) {
            docNums.push_back(rand() % 37 + 7);
        }

        Answer answer;
        CreateMultiSegmentsData(docNums, indexConfig, answer);
        std::shared_ptr<InvertedIndexReader> indexReader = CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    }
}

void ExpackIndexReaderTest::InnerTestGenFieldMapMask(const vector<string>& termFieldNames, fieldmap_t targetFieldMap,
                                                     bool isSuccess)
{
    IndexConfigPtr indexConfig = CreateIndexConfig(EXPACK_OPTION_FLAG_ALL, false, false);
    ExpackIndexReader indexReader;
    indexReader.SetIndexConfig(indexConfig);
    fieldmap_t fieldMap = 0;
    INDEXLIB_TEST_EQUAL(isSuccess, indexReader.GenFieldMapMask("", termFieldNames, fieldMap));
    INDEXLIB_TEST_EQUAL(targetFieldMap, fieldMap);
}

void ExpackIndexReaderTest::CheckIndexReaderLookup(const vector<uint32_t>& docNums,
                                                   std::shared_ptr<InvertedIndexReader>& indexReader,
                                                   const IndexConfigPtr& indexConfig, Answer& answer, bool usePool)
{
    optionflag_t optionFlag = indexConfig->GetOptionFlag();

    INDEXLIB_TEST_EQUAL(indexReader->HasTermPayload(), (optionFlag & of_term_payload) == of_term_payload);
    INDEXLIB_TEST_EQUAL(indexReader->HasDocPayload(), (optionFlag & of_doc_payload) == of_doc_payload);
    INDEXLIB_TEST_EQUAL(indexReader->HasPositionPayload(), (optionFlag & of_position_payload) == of_position_payload);

    const PostingAnswerMap& postingAnswerMap = answer.postingAnswerMap;
    SectionAnswerMap& sectionAnswerMap = answer.sectionAnswerMap;

    bool hasHighFreq = (indexConfig->GetDictConfig() != NULL);
    PostingAnswerMap::const_iterator keyIter = postingAnswerMap.begin();
    for (; keyIter != postingAnswerMap.end(); ++keyIter) {
        string key = keyIter->first;

        index::Term term(key, "");
        Pool pool;
        Pool* pPool = usePool ? &pool : NULL;

        PostingIterator* iter = indexReader->Lookup(term, 1000, pt_default, pPool).ValueOrThrow();
        INDEXLIB_TEST_TRUE(iter != NULL);

        bool hasPos = iter->HasPosition();
        if (hasHighFreq && mVol->Lookup(term.GetWord())) {
            PostingIterator* bitmapIt = indexReader->Lookup(term, 1000, pt_bitmap, pPool).ValueOrThrow();

            INDEXLIB_TEST_EQUAL(bitmapIt->GetType(), pi_bitmap);
            BitmapPostingIterator* bmIt = dynamic_cast<BitmapPostingIterator*>(bitmapIt);
            INDEXLIB_TEST_TRUE(bmIt != NULL);
            INDEXLIB_TEST_EQUAL(bmIt->HasPosition(), false);
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

        if (optionFlag & of_term_frequency) {
            INDEXLIB_TEST_EQUAL(keyAnswer.totalTF, tm->GetTotalTermFreq());
        }

        INDEXLIB_TEST_EQUAL((df_t)keyAnswer.docIdList.size(), tm->GetDocFreq());

        for (uint32_t i = 0; i < docCount; i++) {
            docId = iter->SeekDoc(docId);
            INDEXLIB_TEST_EQUAL(docId, keyAnswer.docIdList[i]);

            TermMatchData tmd;
            if (optionFlag & of_doc_payload) {
                tmd.SetHasDocPayload(true);
            }
            iter->Unpack(tmd);
            const KeyAnswerInDoc& answerInDoc = keyAnswer.inDocAnswers.find(docId)->second;

            if (key == VOL_CONTENT && hasHighFreq) {
                INDEXLIB_TEST_EQUAL(1, tmd.GetTermFreq());
                if (optionFlag & of_doc_payload) {
                    INDEXLIB_TEST_EQUAL(0, tmd.GetDocPayload());
                }
            } else {
                if (optionFlag & of_term_frequency) {
                    INDEXLIB_TEST_EQUAL(keyAnswer.tfList[i], tmd.GetTermFreq());
                } else {
                    INDEXLIB_TEST_EQUAL(1, tmd.GetTermFreq());
                }

                if (optionFlag & of_doc_payload) {
                    INDEXLIB_TEST_EQUAL(answerInDoc.docPayload, tmd.GetDocPayload());
                }
                INDEXLIB_TEST_EQUAL(answerInDoc.fieldMap, tmd.GetFieldMap());
            }

            if (!hasPos) {
                continue;
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

                CheckSection(docId, inDocPosIter, sectionAnswerMap);
            }
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, iter);
    }
}

void ExpackIndexReaderTest::CheckSection(docid_t docId, const std::shared_ptr<InDocPositionIterator>& inDocPosIter,
                                         SectionAnswerMap& sectionAnswerMap)
{
    std::shared_ptr<NormalInDocPositionIterator> expackInDocIter =
        std::dynamic_pointer_cast<NormalInDocPositionIterator>(inDocPosIter);
    INDEXLIB_TEST_TRUE(expackInDocIter != NULL);

    stringstream ss;
    ss << docId << ";" << expackInDocIter->GetFieldPosition() << ";" << expackInDocIter->GetSectionId();

    pos_t pos = expackInDocIter->GetCurrentPosition();

    string idString = ss.str();
    pos_t firstPosInSection = sectionAnswerMap[idString].firstPos;
    (void)firstPosInSection; // avoid warning in release mode
    assert(firstPosInSection <= pos);
    INDEXLIB_TEST_TRUE(sectionAnswerMap[ss.str()].firstPos <= pos);

    pos_t endPosInSection = sectionAnswerMap[idString].firstPos + sectionAnswerMap[idString].sectionMeta.length;
    INDEXLIB_TEST_TRUE(pos < endPosInSection);
    INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.length, expackInDocIter->GetSectionLength());
    INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.weight, expackInDocIter->GetSectionWeight());
    INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.fieldId, expackInDocIter->GetFieldPosition());
}

std::shared_ptr<InvertedIndexReader> ExpackIndexReaderTest::CreateIndexReader(const vector<uint32_t>& docNums,
                                                                              const IndexConfigPtr& indexConfig)
{
    uint32_t segCount = docNums.size();
    PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
    IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
    assert(shardingType != IndexConfig::IST_IS_SHARDING);
    std::shared_ptr<LegacyIndexReader> indexReader;

    if (shardingType == IndexConfig::IST_NO_SHARDING) {
        indexReader.reset(new ExpackIndexReader());
        indexReader->Open(indexConfig, partitionData);
    } else {
        assert(shardingType == IndexConfig::IST_NEED_SHARDING);
        indexReader.reset(new MultiShardingIndexReader(nullptr));
        indexReader->Open(indexConfig, partitionData);
    }
    std::shared_ptr<LegacyIndexAccessoryReader> accessoryReader =
        CreateIndexAccessoryReader(indexConfig, partitionData);
    indexReader->SetLegacyAccessoryReader(accessoryReader);
    return indexReader;
}

void ExpackIndexReaderTest::CreateOneSegmentData(segmentid_t segId, uint32_t docCount, docid_t baseDocId,
                                                 const IndexConfigPtr& indexConfig, Answer& answer)
{
    SegmentInfo segmentInfo;
    segmentInfo.docCount = 0;
    IndexWriterPtr writer = CreateIndexWriter(indexConfig,
                                              /*resetHighFreqVol=*/false);
    vector<IndexDocumentPtr> indexDocs;
    Pool pool;
    PackageIndexConfigPtr packageIndexConfig = dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
    ExpackIndexDocumentMaker::MakeIndexDocuments(&pool, indexDocs, docCount, baseDocId, &answer, packageIndexConfig);
    IndexDocumentMaker::RewriteSectionAttributeInIndexDocuments(indexDocs, mSchema);
    IndexDocumentMaker::AddDocsToWriter(indexDocs, *writer);

    segmentInfo.docCount = docCount;

    EndAndDumpSegment(segId, writer, segmentInfo);
    indexDocs.clear();
}

void ExpackIndexReaderTest::CreateMultiSegmentsData(const vector<uint32_t>& docNums, const IndexConfigPtr& indexConfig,
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

void ExpackIndexReaderTest::EndAndDumpSegment(uint32_t segmentId, IndexWriterPtr writer, const SegmentInfo& segmentInfo)
{
    writer->EndSegment();

    std::stringstream ss;
    ss << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId << "_level_0";
    DirectoryPtr segDirectory = GET_PARTITION_DIRECTORY()->MakeDirectory(ss.str());
    DirectoryPtr indexDirectory = segDirectory->MakeDirectory(INDEX_DIR_NAME);

    writer->Dump(indexDirectory, &mSimplePool);

    segmentInfo.Store(segDirectory);
}

IndexConfigPtr ExpackIndexReaderTest::CreateIndexConfig(optionflag_t optionFlag, bool hasHighFreq, bool multiSharding)
{
    PackageIndexConfigPtr indexConfig;
    ExpackIndexDocumentMaker::CreatePackageIndexConfig(indexConfig, mSchema, 8, it_expack, "ExpackIndex");
    if (hasHighFreq) {
        std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig(VOL_NAME, VOL_CONTENT));
        indexConfig->SetDictConfig(dictConfig);
        indexConfig->SetHighFreqencyTermPostingType(hp_both);
    }
    indexConfig->SetOptionFlag(optionFlag);
    if (multiSharding) {
        // set sharding count to 3
        IndexConfigCreator::CreateShardingIndexConfigs(indexConfig, 3);
    }
    return indexConfig;
}

IndexWriterPtr ExpackIndexReaderTest::CreateIndexWriter(const config::IndexConfigPtr& indexConfig,
                                                        bool resetHighFreqVol)
{
    IndexPartitionOptions options;
    IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
    assert(shardingType != IndexConfig::IST_IS_SHARDING);
    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    metrics->SetDistinctTermCount(indexConfig->GetIndexName(), HASHMAP_INIT_SIZE);
    IndexWriterPtr indexWriter;
    if (shardingType == IndexConfig::IST_NEED_SHARDING) {
        MultiShardingIndexWriter* writer = new MultiShardingIndexWriter(INVALID_SEGMENTID, metrics, options);
        writer->Init(indexConfig, /*buildResourceMetrics=*/nullptr);
        if (resetHighFreqVol) {
            std::shared_ptr<HighFrequencyVocabulary> vol;
            std::vector<NormalIndexWriterPtr>& shardingWriters = writer->GetShardingIndexWriters();
            for (size_t i = 0; i < shardingWriters.size(); ++i) {
                shardingWriters[i]->TEST_SetVocabulary(vol);
            }
        }
        indexWriter.reset(writer);
        return indexWriter;
    }

    NormalIndexWriter* writer = new NormalIndexWriter(INVALID_SEGMENTID, HASHMAP_INIT_SIZE, options);
    writer->Init(indexConfig, /*buildResourceMetrics=*/nullptr);
    if (resetHighFreqVol) {
        std::shared_ptr<HighFrequencyVocabulary> vol;
        writer->TEST_SetVocabulary(vol);
    }
    indexWriter.reset(writer);
    return indexWriter;
}

std::shared_ptr<LegacyIndexAccessoryReader>
ExpackIndexReaderTest::CreateIndexAccessoryReader(const IndexConfigPtr& indexConfig,
                                                  const PartitionDataPtr& partitionData)
{
    IndexSchemaPtr indexSchema(new IndexSchema());
    indexSchema->AddIndexConfig(indexConfig);
    std::shared_ptr<LegacyIndexAccessoryReader> accessoryReader(new LegacyIndexAccessoryReader(indexSchema));

    if (!accessoryReader->Open(partitionData)) {
        accessoryReader.reset();
    }
    return accessoryReader;
}

TEST_F(ExpackIndexReaderTest, TestCaseForGenFieldMapMask)
{
    {
        vector<string> termFieldNames;
        termFieldNames.push_back("field5");
        fieldmap_t targetFieldMap = 32;
        InnerTestGenFieldMapMask(termFieldNames, targetFieldMap);
    }
    {
        vector<string> termFieldNames;
        termFieldNames.push_back("field1");
        termFieldNames.push_back("field2");
        fieldmap_t targetFieldMap = 6;
        InnerTestGenFieldMapMask(termFieldNames, targetFieldMap);
    }

    {
        vector<string> termFieldNames;
        termFieldNames.push_back("field0");
        termFieldNames.push_back("field1");
        termFieldNames.push_back("field2");
        termFieldNames.push_back("field3");
        termFieldNames.push_back("field4");
        fieldmap_t targetFieldMap = 31;
        InnerTestGenFieldMapMask(termFieldNames, targetFieldMap);
    }
    {
        vector<string> termFieldNames;
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
        vector<string> termFieldNames;
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

}} // namespace indexlib::index
