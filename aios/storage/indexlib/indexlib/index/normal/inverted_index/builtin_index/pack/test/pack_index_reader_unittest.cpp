#include "autil/TimeUtility.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/test/index_document_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/payload_checker.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/NumericUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::mem_pool;

using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index {

#define VOL_NAME "reader_vol1"
#define VOL_CONTENT "token0"

class PackIndexReaderTest : public INDEXLIB_TESTBASE
{
public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 10240;

public:
    PackIndexReaderTest()
    {
        mVol.reset(new HighFrequencyVocabulary);
        mVol->Init("pack", it_pack, VOL_CONTENT, {});
    }

    ~PackIndexReaderTest() {}

public:
    typedef IndexDocumentMaker::Answer Answer;
    typedef IndexDocumentMaker::PostingAnswerMap PostingAnswerMap;
    typedef IndexDocumentMaker::SectionAnswerMap SectionAnswerMap;
    typedef IndexDocumentMaker::KeyAnswer KeyAnswer;
    typedef IndexDocumentMaker::KeyAnswerInDoc KeyAnswerInDoc;

    struct PositionCheckInfo {
        docid_t docId;
        TermMatchData tmd;
        KeyAnswerInDoc keyAnswerInDoc;
    };

    DECLARE_CLASS_NAME(PackIndexReaderTest);
    void CaseSetUp() override
    {
        mTestDir = GET_TEMP_DATA_PATH();
        mByteSlicePool = new Pool(&mAllocator, DEFAULT_CHUNK_SIZE);
    }

    void CaseTearDown() override
    {
        IndexDocumentMaker::CleanDir(mTestDir);
        delete mByteSlicePool;
    }

private:
    void InnerTestLookUpWithManyDoc(bool hasHighFreq, optionflag_t optionFlag, bool hasSectionAttribute,
                                    bool setMultiSharding);
    void InnerTestLookUpWithOneDoc(bool hasHighFreq, optionflag_t optionFlag, bool hasSectionAttribute,
                                   bool setMultiSharding);
    void InnerTestLookUpWithMultiSegment(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding);
    void InnerTestDumpEmptySegment(optionflag_t optionFlag, bool setMultiSharding);

private:
    void CheckIndexReaderLookup(const vector<uint32_t>& docNums, std::shared_ptr<InvertedIndexReader>& indexReader,
                                const IndexConfigPtr& indexConfig, Answer& answer);
    void CheckSection(docid_t docId, const std::shared_ptr<InDocPositionIterator>& inDocPosIter,
                      SectionAnswerMap& sectionAnswerMap);

    void CreateMultiSegmentsData(const vector<uint32_t>& docNums, const IndexConfigPtr& indexConfig, Answer& answer);
    void CreateOneSegmentData(segmentid_t segId, uint32_t docCount, docid_t baseDocId,
                              const IndexConfigPtr& indexConfig, Answer& answer);
    void EndAndDumpSegment(uint32_t segmentId, IndexWriterPtr writer, const SegmentInfo& segmentInfo);

    IndexWriterPtr CreateIndexWriter(const IndexConfigPtr& indexConfig, bool resetHighFreqVol);
    std::shared_ptr<InvertedIndexReader> CreateIndexReader(const vector<uint32_t>& docNums,
                                                           const IndexConfigPtr& indexConfig);
    IndexConfigPtr CreateIndexConfig(optionflag_t optionFlag, bool hasHighFreq, bool hasSectionAttribute,
                                     bool multiSharding);
    std::shared_ptr<LegacyIndexAccessoryReader> CreateIndexAccessoryReader(const IndexConfigPtr& indexConfig,
                                                                           const PartitionDataPtr& partitionData);

private:
    IndexPartitionSchemaPtr mSchema;
    string mTestDir;
    SimpleAllocator mAllocator;
    Pool* mByteSlicePool;
    SimplePool mSimplePool;
    std::shared_ptr<HighFrequencyVocabulary> mVol;
};

void PackIndexReaderTest::InnerTestLookUpWithManyDoc(bool hasHighFreq, optionflag_t optionFlag,
                                                     bool hasSectionAttribute, bool setMultiSharding)
{
    tearDown();
    setUp();
    vector<uint32_t> docNums;
    docNums.push_back(97);

    IndexConfigPtr indexConfig = CreateIndexConfig(optionFlag, hasHighFreq, true, setMultiSharding);
    Answer answer;
    CreateMultiSegmentsData(docNums, indexConfig, answer);

    std::shared_ptr<InvertedIndexReader> indexReader = CreateIndexReader(docNums, indexConfig);
    CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
}

void PackIndexReaderTest::CreateMultiSegmentsData(const vector<uint32_t>& docNums, const IndexConfigPtr& indexConfig,
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
                                               const IndexConfigPtr& indexConfig, Answer& answer)
{
    SegmentInfo segmentInfo;
    segmentInfo.docCount = 0;

    IndexWriterPtr writer = CreateIndexWriter(indexConfig,
                                              /*resetHighFreqVol=*/false);
    vector<IndexDocumentPtr> indexDocs;
    Pool pool;
    IndexDocumentMaker::MakeIndexDocuments(&pool, indexDocs, docCount, baseDocId, &answer);
    IndexDocumentMaker::RewriteSectionAttributeInIndexDocuments(indexDocs, mSchema);
    IndexDocumentMaker::AddDocsToWriter(indexDocs, *writer);

    segmentInfo.docCount = docCount;

    EndAndDumpSegment(segId, writer, segmentInfo);
    indexDocs.clear();
}

void PackIndexReaderTest::CheckIndexReaderLookup(const vector<uint32_t>& docNums,
                                                 std::shared_ptr<InvertedIndexReader>& indexReader,
                                                 const IndexConfigPtr& indexConfig, Answer& answer)
{
    PackageIndexConfigPtr packIndexConfig = dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
    INDEXLIB_TEST_TRUE(packIndexConfig.get() != NULL);
    optionflag_t optionFlag = indexConfig->GetOptionFlag();

    INDEXLIB_TEST_EQUAL(indexReader->HasTermPayload(), (optionFlag & of_term_payload) == of_term_payload);
    INDEXLIB_TEST_EQUAL(indexReader->HasDocPayload(), (optionFlag & of_doc_payload) == of_doc_payload);
    INDEXLIB_TEST_EQUAL(indexReader->HasPositionPayload(), (optionFlag & of_position_payload) == of_position_payload);

    const PostingAnswerMap& postingAnswerMap = answer.postingAnswerMap;
    SectionAnswerMap& sectionAnswerMap = answer.sectionAnswerMap;

    bool hasHighFreq = (indexConfig->GetDictConfig() != NULL);
    uint32_t hint = 0;
    PostingAnswerMap::const_iterator keyIter = postingAnswerMap.begin();
    for (; keyIter != postingAnswerMap.end(); ++keyIter) {
        bool usePool = (++hint % 2) == 0;
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
        docid_t docId = INVALID_DOCID;

        TermMeta* tm = iter->GetTermMeta();
        if (optionFlag & of_term_payload) {
            INDEXLIB_TEST_EQUAL(keyAnswer.termPayload, tm->GetPayload());
        }
        if (optionFlag & of_term_frequency) {
            INDEXLIB_TEST_EQUAL(keyAnswer.totalTF, tm->GetTotalTermFreq());
        }
        INDEXLIB_TEST_EQUAL((df_t)keyAnswer.docIdList.size(), tm->GetDocFreq());
        vector<PositionCheckInfo> positionCheckInfos;
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
                INDEXLIB_TEST_EQUAL(answerInDoc.positionList[j], pos);

                if (optionFlag & of_position_payload) {
                    pospayload_t posPayload = inDocPosIter->GetPosPayload();
                    INDEXLIB_TEST_EQUAL(answerInDoc.posPayloadList[j], posPayload);
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
                    INDEXLIB_TEST_EQUAL(answerInDoc.posPayloadList[j], posPayload);
                }

                INDEXLIB_TEST_EQUAL(answerInDoc.positionList[j], pos);
                CheckSection(docId, inDocPosIter, sectionAnswerMap);
            }
            INDEXLIB_TEST_EQUAL(INVALID_POSITION, inDocPosIter->SeekPosition(pos));
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, iter);
    }
}

void PackIndexReaderTest::CheckSection(docid_t docId, const std::shared_ptr<InDocPositionIterator>& inDocPosIter,
                                       SectionAnswerMap& sectionAnswerMap)
{
    std::shared_ptr<NormalInDocPositionIterator> packInDocIter =
        dynamic_pointer_cast<NormalInDocPositionIterator>(inDocPosIter);
    INDEXLIB_TEST_TRUE(packInDocIter != NULL);

    stringstream ss;
    ss << docId << ";" << packInDocIter->GetFieldPosition() << ";" << packInDocIter->GetSectionId();

    pos_t pos = packInDocIter->GetCurrentPosition();

    string idString = ss.str();
    pos_t firstPosInSection = sectionAnswerMap[idString].firstPos;
    (void)firstPosInSection; // avoid warning in release mode
    assert(firstPosInSection <= pos);
    INDEXLIB_TEST_TRUE(sectionAnswerMap[ss.str()].firstPos <= pos);

    pos_t endPosInSection = sectionAnswerMap[idString].firstPos + sectionAnswerMap[idString].sectionMeta.length;
    INDEXLIB_TEST_TRUE(pos < endPosInSection);

    INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.length, packInDocIter->GetSectionLength());
    INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.weight, packInDocIter->GetSectionWeight());
    INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.fieldId, packInDocIter->GetFieldPosition());
}

void PackIndexReaderTest::InnerTestLookUpWithOneDoc(bool hasHighFreq, optionflag_t optionFlag, bool hasSectionAttribute,
                                                    bool setMultiSharding)
{
    tearDown();
    setUp();
    vector<uint32_t> docNums;
    docNums.push_back(1);

    IndexConfigPtr indexConfig = CreateIndexConfig(optionFlag, hasHighFreq, hasSectionAttribute, setMultiSharding);
    Answer answer;
    CreateMultiSegmentsData(docNums, indexConfig, answer);
    std::shared_ptr<InvertedIndexReader> indexReader = CreateIndexReader(docNums, indexConfig);
    CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
}

void PackIndexReaderTest::InnerTestLookUpWithMultiSegment(bool hasHighFreq, optionflag_t optionFlag,
                                                          bool setMultiSharding)
{
    tearDown();
    setUp();
    IndexConfigPtr indexConfig = CreateIndexConfig(optionFlag, hasHighFreq, true, setMultiSharding);
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

std::shared_ptr<InvertedIndexReader> PackIndexReaderTest::CreateIndexReader(const vector<uint32_t>& docNums,
                                                                            const IndexConfigPtr& indexConfig)
{
    uint32_t segCount = docNums.size();
    PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
    IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
    assert(shardingType != IndexConfig::IST_IS_SHARDING);
    std::shared_ptr<LegacyIndexReader> indexReader;

    if (shardingType == IndexConfig::IST_NO_SHARDING) {
        indexReader.reset(new PackIndexReader());
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

IndexWriterPtr PackIndexReaderTest::CreateIndexWriter(const config::IndexConfigPtr& indexConfig, bool resetHighFreqVol)
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

IndexConfigPtr PackIndexReaderTest::CreateIndexConfig(optionflag_t optionFlag, bool hasHighFreq,
                                                      bool hasSectionAttribute, bool multiSharding)
{
    config::PackageIndexConfigPtr indexConfig;
    IndexDocumentMaker::CreatePackageIndexConfig(indexConfig, mSchema);
    if (hasHighFreq) {
        std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig(VOL_NAME, VOL_CONTENT));
        indexConfig->SetDictConfig(dictConfig);
        indexConfig->SetHighFreqencyTermPostingType(hp_both);
    }
    indexConfig->SetHasSectionAttributeFlag(hasSectionAttribute);
    indexConfig->SetOptionFlag(optionFlag);

    if (multiSharding) {
        // set sharding count to 3
        IndexConfigCreator::CreateShardingIndexConfigs(indexConfig, 3);
    }
    return indexConfig;
}

std::shared_ptr<LegacyIndexAccessoryReader>
PackIndexReaderTest::CreateIndexAccessoryReader(const IndexConfigPtr& indexConfig,
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

void PackIndexReaderTest::InnerTestDumpEmptySegment(optionflag_t optionFlag, bool setMultiSharding)
{
    tearDown();
    setUp();

    vector<uint32_t> docNums;
    docNums.push_back(100);
    docNums.push_back(23);

    SegmentInfos segmentInfos;
    std::shared_ptr<HighFrequencyVocabulary> vol;
    IndexConfigPtr indexConfig = CreateIndexConfig(optionFlag, false, true, setMultiSharding);

    docid_t currBeginDocId = 0;
    SegmentInfo segmentInfo;
    segmentInfo.docCount = 0;

    vector<document::IndexDocumentPtr> indexDocs;
    indexDocs.push_back(IndexDocumentPtr(new IndexDocument(mByteSlicePool)));
    IndexDocumentMaker::RewriteSectionAttributeInIndexDocuments(indexDocs, mSchema);

    for (size_t x = 0; x < docNums.size(); ++x) {
        IndexWriterPtr writer = CreateIndexWriter(indexConfig, /*resetHighFreqVol=*/
                                                  true);
        IndexDocument& emptyIndexDoc = *indexDocs[0];
        for (uint32_t i = 0; i < docNums[x]; i++) {
            emptyIndexDoc.SetDocId(i);
            writer->EndDocument(emptyIndexDoc);
        }
        segmentInfo.docCount = docNums[x];
        EndAndDumpSegment(/*segmentId=*/x, writer, segmentInfo);
        currBeginDocId += segmentInfo.docCount;
    }

    std::shared_ptr<InvertedIndexReader> indexReader = CreateIndexReader(docNums, indexConfig);
}

void PackIndexReaderTest::EndAndDumpSegment(uint32_t segmentId, IndexWriterPtr writer, const SegmentInfo& segmentInfo)
{
    writer->EndSegment();

    std::stringstream ss;
    ss << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId << "_level_0";
    DirectoryPtr segDirectory = GET_PARTITION_DIRECTORY()->MakeDirectory(ss.str());
    DirectoryPtr indexDirectory = segDirectory->MakeDirectory(INDEX_DIR_NAME);

    writer->Dump(indexDirectory, &mSimplePool);

    segmentInfo.Store(segDirectory);
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

}} // namespace indexlib::index
