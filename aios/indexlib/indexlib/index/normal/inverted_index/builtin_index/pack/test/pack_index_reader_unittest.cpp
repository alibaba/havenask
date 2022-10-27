#define PACK_INDEX_READER_TEST
#define INDEX_ENGINE_UNITTEST

#include <autil/TimeUtility.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h"
#include "indexlib/index/test/index_document_maker.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/index/test/payload_checker.h"
#include "indexlib/util/numeric_util.h"
#include "indexlib/index/test/index_test_util.h"
#include <autil/mem_pool/SimpleAllocator.h>

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace autil::legacy;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);


IE_NAMESPACE_BEGIN(index);

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
        mVol->Init("pack", it_pack, VOL_CONTENT);
    }

    ~PackIndexReaderTest() 
    {
    }
    
public:
    typedef IndexDocumentMaker::Answer Answer;
    typedef IndexDocumentMaker::PostingAnswerMap PostingAnswerMap;
    typedef IndexDocumentMaker::SectionAnswerMap SectionAnswerMap;
    typedef IndexDocumentMaker::KeyAnswer KeyAnswer;
    typedef IndexDocumentMaker::KeyAnswerInDoc KeyAnswerInDoc;

    struct PositionCheckInfo
    {
        docid_t docId;
        TermMatchData tmd;
        KeyAnswerInDoc keyAnswerInDoc;
    };
    
    DECLARE_CLASS_NAME(PackIndexReaderTest);
    void CaseSetUp() override
    {
        mTestDir = GET_TEST_DATA_PATH();
        mByteSlicePool = new Pool(&mAllocator, DEFAULT_CHUNK_SIZE);
    }

    void CaseTearDown() override
    {
        IndexDocumentMaker::CleanDir(mTestDir);
        delete mByteSlicePool;
    }
    
    void TestCaseForLookUpWithOneDoc()
    {
        TestLookUpWithOneDoc(false, OPTION_FLAG_ALL, true, true);
        TestLookUpWithOneDoc(false, OPTION_FLAG_ALL, true, false);        
    }

    void TestCaseForLookupWithOneDocWithoutSectionAttribute()
    {
        TestLookUpWithOneDoc(false, OPTION_FLAG_ALL, false, true);
        TestLookUpWithOneDoc(false, OPTION_FLAG_ALL, false, false);        
    }

    void TestCaseForLookUpWithOneDocWithHighFreq()
    {
        TestLookUpWithOneDoc(true, OPTION_FLAG_ALL, true, true);
        TestLookUpWithOneDoc(true, OPTION_FLAG_ALL, true, false);        
    }

     void TestCaseForNoPayloadLookUpWithOneDoc()
     {
         TestLookUpWithOneDoc(false, NO_PAYLOAD, true, true);
         TestLookUpWithOneDoc(false, NO_PAYLOAD, true, false);         
     }

    void TestCaseForNoPayloadLookUpWithOneDocWithHighFreq()
    {
        TestLookUpWithOneDoc(true, NO_PAYLOAD, true, true);
        TestLookUpWithOneDoc(true, NO_PAYLOAD, true, false);        
    }

    void TestCaseForNoPositionListLookUpWithOneDoc()
    {
        TestLookUpWithOneDoc(false, NO_POSITION_LIST, true, true);
        TestLookUpWithOneDoc(false, NO_POSITION_LIST, true, false);         
    }

    void TestCaseForNoPositionListLookUpWithOneDocWithHighFreq()
    {
        TestLookUpWithOneDoc(true, NO_POSITION_LIST, true, true);
        TestLookUpWithOneDoc(true, NO_POSITION_LIST, true, false);        
    }

    void TestCaseForLookUpWithManyDoc()
    {
        TestLookUpWithManyDoc(false, OPTION_FLAG_ALL, true, true);
        TestLookUpWithManyDoc(false, OPTION_FLAG_ALL, true, false);        
    }

    void TestCaseForLookUpWithManyDocWithHighFreq()
    {
        TestLookUpWithManyDoc(true, OPTION_FLAG_ALL, true, true);
        TestLookUpWithManyDoc(true, OPTION_FLAG_ALL, true, false);        
    }

    void TestCaseForLookupWithoutTf()
    {
        TestLookUpWithManyDoc(false, NO_TERM_FREQUENCY, true, true);
        TestLookUpWithManyDoc(false, NO_TERM_FREQUENCY, true, false);        
    }

    void TestCaseForLookupWithoutTfWithHighFreq()
    {
        TestLookUpWithManyDoc(true, NO_TERM_FREQUENCY, true, true);
        TestLookUpWithManyDoc(true, NO_TERM_FREQUENCY, true, false);        
    }

    void TestCaseForNoPayloadLookUpWithManyDoc()
    {
        TestLookUpWithManyDoc(false, NO_PAYLOAD, true, true);
        TestLookUpWithManyDoc(false, NO_PAYLOAD, true, false);        
    }

    void TestCaseForNoPayloadLookUpWithManyDocWithHighFreq()
    {
        TestLookUpWithManyDoc(true, NO_PAYLOAD, true, true);
        TestLookUpWithManyDoc(true, NO_PAYLOAD, true, false);        
    }

     void TestCaseForNoPositionListLookUpWithManyDoc()
     {
         TestLookUpWithManyDoc(false, NO_POSITION_LIST, true, true);
         TestLookUpWithManyDoc(false, NO_POSITION_LIST, true, false);         
     }

    void TestCaseForNoPositionListLookUpWithManyDocWithHighFreq()
    {
        TestLookUpWithManyDoc(true, NO_POSITION_LIST, true, true);
        TestLookUpWithManyDoc(true, NO_POSITION_LIST, true, false);        
    }

    void TestCaseForLookUpWithMultiSegment()
    {
        TestLookUpWithMultiSegment(false, OPTION_FLAG_ALL, true);
        TestLookUpWithMultiSegment(false, OPTION_FLAG_ALL, false);        
    }

    void TestCaseForLookUpWithMultiSegmentWithHighFreq()
    {
        TestLookUpWithMultiSegment(true, OPTION_FLAG_ALL, true);
        TestLookUpWithMultiSegment(true, OPTION_FLAG_ALL, false);        
    }

    void TestCaseForNoPayloadLookUpWithMultiSegment()
    {
        TestLookUpWithMultiSegment(false, NO_PAYLOAD, true);
        TestLookUpWithMultiSegment(false, NO_PAYLOAD, false);        
    }
    
    void TestCaseForNoPayloadLookUpWithMultiSegmentWithHighFreq()
    {
        TestLookUpWithMultiSegment(true, NO_PAYLOAD, true);
        TestLookUpWithMultiSegment(true, NO_PAYLOAD, false);        
    }

    void TestCaseForNoPositionListLookUpWithMultiSegment()
    {
        TestLookUpWithMultiSegment(false, NO_POSITION_LIST, true);
        TestLookUpWithMultiSegment(false, NO_POSITION_LIST, false);        
    }

    void TestCaseForNoPositionListLookUpWithMultiSegmentWithHighFreq()
    {
        TestLookUpWithMultiSegment(true, NO_POSITION_LIST, true);
        TestLookUpWithMultiSegment(true, NO_POSITION_LIST, false);        
    }

    void TestCaseForTfBitmapOneDoc()
    {
        TestLookUpWithOneDoc(true, OPTION_FLAG_ALL | of_tf_bitmap, true, true);
        TestLookUpWithOneDoc(true, NO_PAYLOAD | of_tf_bitmap, true, true);

        TestLookUpWithOneDoc(true, OPTION_FLAG_ALL | of_tf_bitmap, true, false);
        TestLookUpWithOneDoc(true, NO_PAYLOAD | of_tf_bitmap, true, false); 
    }

    void TestCaseForTfBitmapManyDoc()
    {
        TestLookUpWithManyDoc(true, OPTION_FLAG_ALL | of_tf_bitmap, true, true);
        TestLookUpWithManyDoc(true, NO_PAYLOAD | of_tf_bitmap, true, true);
        
        TestLookUpWithManyDoc(true, OPTION_FLAG_ALL | of_tf_bitmap, true, false);
        TestLookUpWithManyDoc(true, NO_PAYLOAD | of_tf_bitmap, true, false); 
    }

    void TestCaseForTfBitmapMultiSegment()
    {
        TestLookUpWithMultiSegment(true, OPTION_FLAG_ALL | of_tf_bitmap, true);
        TestLookUpWithMultiSegment(true, NO_PAYLOAD | of_tf_bitmap, true);
        TestLookUpWithMultiSegment(true, OPTION_FLAG_ALL | of_tf_bitmap, false);
        TestLookUpWithMultiSegment(true, NO_PAYLOAD | of_tf_bitmap, false); 
    }

    void TestDumpEmptySegment(optionflag_t optionFlag, bool setMultiSharding)
    {
        TearDown();
        SetUp();

        vector<uint32_t> docNums;
        docNums.push_back(100);
        docNums.push_back(23);
        
        SegmentInfos segmentInfos;
        HighFrequencyVocabularyPtr vol;
        IndexConfigPtr indexConfig = CreateIndexConfig(
                optionFlag, false, true, setMultiSharding);

        docid_t currBeginDocId = 0;
        SegmentInfo segmentInfo;
        segmentInfo.docCount = 0;
	
	vector<document::IndexDocumentPtr> indexDocs;
	indexDocs.push_back(IndexDocumentPtr(new IndexDocument(mByteSlicePool)));
	IndexDocumentMaker::RewriteSectionAttributeInIndexDocuments(
	    indexDocs, mSchema);

        for (size_t x = 0; x < docNums.size(); ++x)
        {
            IndexWriterPtr writer = CreateIndexWriter(indexConfig, true);
            IndexDocument& emptyIndexDoc = *indexDocs[0];
            for (uint32_t i = 0; i < docNums[x]; i++)
            {
                emptyIndexDoc.SetDocId(i);
                writer->EndDocument(emptyIndexDoc);
            }
            segmentInfo.docCount = docNums[x];
            writer->EndSegment();
            DumpSegment(x, segmentInfo, writer);
            currBeginDocId += segmentInfo.docCount;
        }

        IndexReaderPtr indexReader = CreateIndexReader(docNums, indexConfig);
    }

    void TestCaseForDumpEmptySegment()
    {
        TestDumpEmptySegment(OPTION_FLAG_ALL, false);        
        TestDumpEmptySegment(OPTION_FLAG_ALL, true);
    }

    void TestCaseForNoPositionListDumpEmptySegment()
    {
        TestDumpEmptySegment(NO_POSITION_LIST, false);        
        TestDumpEmptySegment(NO_POSITION_LIST, true);
    }

private:
    void TestLookUpWithManyDoc(bool hasHighFreq, optionflag_t optionFlag,
                               bool hasSectionAttribute, bool setMultiSharding)
    {
        TearDown();
        SetUp();        
        vector<uint32_t> docNums;
        docNums.push_back(97);

        IndexConfigPtr indexConfig = CreateIndexConfig(
                optionFlag, hasHighFreq, true, setMultiSharding);
        Answer answer;
        CreateMultiSegmentsData(docNums, indexConfig, answer);

        IndexReaderPtr indexReader = CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    }

    void CreateMultiSegmentsData(const vector<uint32_t>& docNums,
                                 const IndexConfigPtr& indexConfig,
                                 Answer& answer)
    {
        TearDown();
        SetUp();

        docid_t baseDocId = 0;
        for (size_t i = 0; i < docNums.size(); ++i)
        {
            CreateOneSegmentData(i, docNums[i], baseDocId, indexConfig, answer);
            baseDocId += docNums[i];
        }
    }

    void CreateOneSegmentData(segmentid_t segId, uint32_t docCount,  
                              docid_t baseDocId, const IndexConfigPtr& indexConfig,
                              Answer& answer)
    {
        SegmentInfo segmentInfo;
        segmentInfo.docCount = 0;

        IndexWriterPtr writer = CreateIndexWriter(indexConfig);
        vector<IndexDocumentPtr> indexDocs;
        Pool pool;
        IndexDocumentMaker::MakeIndexDocuments(&pool, indexDocs, docCount, 
                baseDocId, &answer);
	IndexDocumentMaker::RewriteSectionAttributeInIndexDocuments(
	    indexDocs, mSchema);
        IndexDocumentMaker::AddDocsToWriter(indexDocs, *writer);
        segmentInfo.docCount = docCount;

        writer->EndSegment();
        DumpSegment(segId, segmentInfo, writer);
        indexDocs.clear();
    }

    void CheckIndexReaderLookup(const vector<uint32_t>& docNums,
                                IndexReaderPtr& indexReader,
                                const IndexConfigPtr& indexConfig,
                                Answer& answer)
    {
        PackageIndexConfigPtr packIndexConfig = 
            dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
        INDEXLIB_TEST_TRUE(packIndexConfig.get() != NULL);
        optionflag_t optionFlag = indexConfig->GetOptionFlag();

        INDEXLIB_TEST_EQUAL(indexReader->HasTermPayload(),
                          (optionFlag & of_term_payload) == of_term_payload);
        INDEXLIB_TEST_EQUAL(indexReader->HasDocPayload(),
                          (optionFlag & of_doc_payload) == of_doc_payload);
        INDEXLIB_TEST_EQUAL(indexReader->HasPositionPayload(),
                          (optionFlag & of_position_payload) == of_position_payload);

        const PostingAnswerMap& postingAnswerMap = answer.postingAnswerMap;
        SectionAnswerMap& sectionAnswerMap = answer.sectionAnswerMap;

        bool hasHighFreq = (indexConfig->GetDictConfig() != NULL);
        uint32_t hint = 0;
        PostingAnswerMap::const_iterator keyIter = postingAnswerMap.begin();
        for (; keyIter != postingAnswerMap.end(); ++keyIter)
        {
            bool usePool = (++hint % 2) == 0;
            string key = keyIter->first;

            common::Term term(key, "");
            Pool pool;
            Pool *pPool = usePool ? &pool : NULL;

            PostingIterator *iter = indexReader->Lookup(term, 1000, pt_default, pPool);
            INDEXLIB_TEST_TRUE(iter != NULL);

            bool hasPos = iter->HasPosition();
            if (hasHighFreq && mVol->Lookup(term.GetWord()))
            {
                PostingIterator *bitmapIt = indexReader->Lookup(
                        term, 1000, pt_bitmap, pPool);

                INDEXLIB_TEST_EQUAL(bitmapIt->GetType(), pi_bitmap);
                BitmapPostingIterator *bmIt =
                    dynamic_cast<BitmapPostingIterator*>(bitmapIt);
                INDEXLIB_TEST_TRUE(bmIt != NULL);
                INDEXLIB_TEST_EQUAL(bmIt->HasPosition(), false);
                IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, bitmapIt);
            }
            else
            {
                INDEXLIB_TEST_EQUAL(iter->HasPosition(), 
                        (optionFlag & of_position_list) != 0);
            }
            
            const KeyAnswer& keyAnswer = keyIter->second;
            uint32_t docCount = keyAnswer.docIdList.size();
            docid_t docId = INVALID_DOCID;

            index::TermMeta *tm = iter->GetTermMeta();
            if (optionFlag & of_term_payload)
            {
                INDEXLIB_TEST_EQUAL(keyAnswer.termPayload, tm->GetPayload());
            }
            if (optionFlag & of_term_frequency)
            {
                INDEXLIB_TEST_EQUAL(keyAnswer.totalTF, tm->GetTotalTermFreq());
            }
            INDEXLIB_TEST_EQUAL((df_t)keyAnswer.docIdList.size(), tm->GetDocFreq());
            vector<PositionCheckInfo> positionCheckInfos;
            for (uint32_t i = 0; i < docCount; i++)
            {
                docId = iter->SeekDoc(docId);
                INDEXLIB_TEST_EQUAL(docId, keyAnswer.docIdList[i]);

                TermMatchData tmd;
                if (optionFlag & of_doc_payload)
                {
                    tmd.SetHasDocPayload(true);
                }
                iter->Unpack(tmd);

                const KeyAnswerInDoc& answerInDoc = 
                    keyAnswer.inDocAnswers.find(docId)->second;
                if (key == VOL_CONTENT && hasHighFreq)
                {
                    INDEXLIB_TEST_EQUAL(1, tmd.GetTermFreq());
                    if (optionFlag & of_doc_payload)
                    {
                        INDEXLIB_TEST_EQUAL(0, tmd.GetDocPayload());
                    }
                }
                else
                {
                    if (optionFlag & of_term_frequency)
                    {
                        INDEXLIB_TEST_EQUAL(keyAnswer.tfList[i], tmd.GetTermFreq());
                    }
                    else
                    {
                        INDEXLIB_TEST_EQUAL(1, tmd.GetTermFreq());
                    }
                    if (optionFlag & of_doc_payload)
                    {
                        INDEXLIB_TEST_EQUAL(answerInDoc.docPayload, tmd.GetDocPayload());
                    }
                }

                if (!hasPos)
                {  
                    continue;
                }
                if (optionFlag & of_tf_bitmap)
                {
                    PositionCheckInfo info;
                    info.docId = docId;
                    info.tmd = tmd;
                    info.keyAnswerInDoc = answerInDoc;
                    positionCheckInfos.push_back(info);
                    continue;
                }
                InDocPositionIteratorPtr inDocPosIter =
                    tmd.GetInDocPositionIterator();
                pos_t pos = 0;
                uint32_t posCount = answerInDoc.positionList.size();
                for (uint32_t j = 0; j < posCount; j++)
                {
                    pos = inDocPosIter->SeekPosition(pos);
                    INDEXLIB_TEST_EQUAL(answerInDoc.positionList[j], pos);

                    if (optionFlag & of_position_payload)
                    {
                        pospayload_t posPayload = inDocPosIter->GetPosPayload();
                        INDEXLIB_TEST_EQUAL(answerInDoc.posPayloadList[j], posPayload);
                    }

                    if (packIndexConfig->HasSectionAttribute())
                    {
                        CheckSection(docId, inDocPosIter, sectionAnswerMap);
                    }
                }
            }
            // check in doc position in random order
            random_shuffle(positionCheckInfos.begin(), positionCheckInfos.end());
            for (uint32_t i = 0; i < positionCheckInfos.size(); ++i)
            {
                docid_t docId = positionCheckInfos[i].docId;
                TermMatchData &tmd = positionCheckInfos[i].tmd;
                const KeyAnswerInDoc& answerInDoc = positionCheckInfos[i].keyAnswerInDoc;
                InDocPositionIteratorPtr inDocPosIter = tmd.GetInDocPositionIterator();
                pos_t pos = 0;
                uint32_t posCount = answerInDoc.positionList.size();
                for (uint32_t j = 0; j < posCount; j++)
                {
                    pos = inDocPosIter->SeekPosition(pos);
                    assert(answerInDoc.positionList[j] == pos);

                    if (optionFlag & of_position_payload)
                    {
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

    void CheckSection(docid_t docId,
                      const InDocPositionIteratorPtr& inDocPosIter, 
                      SectionAnswerMap& sectionAnswerMap)
    {
        NormalInDocPositionIteratorPtr packInDocIter = 
            dynamic_pointer_cast<NormalInDocPositionIterator>(inDocPosIter);
        INDEXLIB_TEST_TRUE(packInDocIter != NULL);

        stringstream ss;
        ss << docId << ";" << packInDocIter->GetFieldPosition() << ";" 
           << packInDocIter->GetSectionId();

        pos_t pos = packInDocIter->GetCurrentPosition();

        string idString = ss.str();
        pos_t firstPosInSection = sectionAnswerMap[idString].firstPos;
        (void)firstPosInSection; //avoid warning in release mode
        assert(firstPosInSection <= pos);
        INDEXLIB_TEST_TRUE(sectionAnswerMap[ss.str()].firstPos <= pos);

        pos_t endPosInSection =  sectionAnswerMap[idString].firstPos + 
                                 sectionAnswerMap[idString].sectionMeta.length;
        INDEXLIB_TEST_TRUE(pos < endPosInSection);
                    
        INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.length, 
                            packInDocIter->GetSectionLength());
        INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.weight,
                            packInDocIter->GetSectionWeight());
        INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.fieldId,
                            packInDocIter->GetFieldPosition());
    }
    
    IndexWriterPtr CreateIndexWriter(const IndexConfigPtr& indexConfig,
            bool resetHighFreqVol = false)
    {
        IndexPartitionOptions options;
        IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
        assert(shardingType != IndexConfig::IST_IS_SHARDING);
        SegmentMetricsPtr metrics(new SegmentMetrics);
        metrics->SetDistinctTermCount(indexConfig->GetIndexName(), HASHMAP_INIT_SIZE);
        IndexWriterPtr indexWriter;
        if (shardingType == IndexConfig::IST_NEED_SHARDING)
        {
            MultiShardingIndexWriter* writer = new MultiShardingIndexWriter(
                    metrics, options);
            writer->Init(indexConfig, NULL);
            if (resetHighFreqVol)
            {
                HighFrequencyVocabularyPtr vol;
                vector<NormalIndexWriterPtr>& shardingWriters =
                    writer->GetShardingIndexWriters();
                for (size_t i = 0; i < shardingWriters.size(); ++i)
                {
                    shardingWriters[i]->SetVocabulary(vol);
                }
            }

            indexWriter.reset(writer);
            return indexWriter;
        }
        
        NormalIndexWriter* writer = new NormalIndexWriter(HASHMAP_INIT_SIZE, options);
        writer->Init(indexConfig, NULL);
        if (resetHighFreqVol)
        {
            HighFrequencyVocabularyPtr vol;
            writer->SetVocabulary(vol);
        }
        indexWriter.reset(writer);
        return indexWriter;
    }

    void TestLookUpWithOneDoc(bool hasHighFreq, optionflag_t optionFlag,
                              bool hasSectionAttribute, bool setMultiSharding)
    {
        TearDown();
        SetUp();                
        vector<uint32_t> docNums;
        docNums.push_back(1);

        IndexConfigPtr indexConfig = CreateIndexConfig(optionFlag, 
                hasHighFreq, hasSectionAttribute, setMultiSharding);
        Answer answer;
        CreateMultiSegmentsData(docNums, indexConfig, answer);
        IndexReaderPtr indexReader = CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    }

    void TestLookUpWithMultiSegment(bool hasHighFreq, optionflag_t optionFlag,
                                    bool setMultiSharding)
    {
        TearDown();
        SetUp();                
        IndexConfigPtr indexConfig = CreateIndexConfig(
                optionFlag, hasHighFreq, true, setMultiSharding);
        const int32_t segmentCounts[] = {1, 4, 7, 9};
        for (size_t j = 0; j < sizeof(segmentCounts) / sizeof(int32_t); ++j)
        {
            vector<uint32_t> docNums;
            for (int32_t i = 0; i < segmentCounts[j]; ++i)
            {
                docNums.push_back(rand() % 37 + 7);
            }
            
            Answer answer;
            CreateMultiSegmentsData(docNums, indexConfig, answer);
            IndexReaderPtr indexReader = CreateIndexReader(docNums, indexConfig);
            CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
        }
    }

    void DumpSegment(uint32_t segId, const SegmentInfo& segmentInfo,
                     const IndexWriterPtr& writer)
    {
        stringstream ss;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0";
        DirectoryPtr segDirectory = 
            GET_PARTITION_DIRECTORY()->MakeDirectory(ss.str());

        DirectoryPtr indexDirectory =
            segDirectory->MakeDirectory(INDEX_DIR_NAME);
        writer->Dump(indexDirectory, &mSimplePool);

        segmentInfo.Store(segDirectory);
    }

    IndexReaderPtr CreateIndexReader(const vector<uint32_t>& docNums, 
            const IndexConfigPtr& indexConfig) 
    {
        uint32_t segCount = docNums.size();
        PartitionDataPtr partitionData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
        IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
        assert(shardingType != IndexConfig::IST_IS_SHARDING);
        IndexReaderPtr indexReader;
        
        if (shardingType == IndexConfig::IST_NO_SHARDING)
        {
            indexReader.reset(new PackIndexReader());
            indexReader->Open(indexConfig, partitionData);
        }
        else 
        {
            assert(shardingType == IndexConfig::IST_NEED_SHARDING);
            indexReader.reset(new MultiShardingIndexReader());
            indexReader->Open(indexConfig, partitionData);
        }
        IndexAccessoryReaderPtr accessoryReader =
            CreateIndexAccessoryReader(indexConfig, partitionData);        
        indexReader->SetAccessoryReader(accessoryReader);
        return indexReader;
    }

    IndexConfigPtr CreateIndexConfig(
            optionflag_t optionFlag, 
            bool hasHighFreq, bool hasSectionAttribute, bool multiSharding)
    {
        config::PackageIndexConfigPtr indexConfig;
        IndexDocumentMaker::CreatePackageIndexConfig(indexConfig, mSchema);
        if (hasHighFreq)
        {
            DictionaryConfigPtr dictConfig(new DictionaryConfig(VOL_NAME, VOL_CONTENT));
            indexConfig->SetDictConfig(dictConfig);
            indexConfig->SetHighFreqencyTermPostingType(hp_both);
        }
        indexConfig->SetHasSectionAttributeFlag(hasSectionAttribute);
        indexConfig->SetOptionFlag(optionFlag);

        if (multiSharding)
        {
            // set sharding count to 3 
            IndexConfigCreator::CreateShardingIndexConfigs(indexConfig, 3);
        }
        return indexConfig;
    }

    IndexAccessoryReaderPtr CreateIndexAccessoryReader(
            const IndexConfigPtr& indexConfig, 
            const PartitionDataPtr& partitionData)
    {
        IndexSchemaPtr indexSchema(new IndexSchema());
        indexSchema->AddIndexConfig(indexConfig);
        IndexAccessoryReaderPtr accessoryReader(
                new IndexAccessoryReader(indexSchema));

        if (!accessoryReader->Open(partitionData))
        {
            accessoryReader.reset();
        }
        return accessoryReader;
    }

private:
    IndexPartitionSchemaPtr mSchema;
    string mTestDir;
    SimpleAllocator mAllocator;
    Pool* mByteSlicePool;
    SimplePool mSimplePool;
    HighFrequencyVocabularyPtr mVol;
};

INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForLookUpWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForLookUpWithManyDoc);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithManyDoc);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithManyDoc);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForLookUpWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForLookUpWithOneDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithOneDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithOneDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForLookUpWithManyDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithManyDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithManyDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForLookUpWithMultiSegmentWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPayloadLookUpWithMultiSegmentWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPositionListLookUpWithMultiSegmentWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForDumpEmptySegment);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForNoPositionListDumpEmptySegment);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForLookupWithoutTf);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForLookupWithoutTfWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForLookupWithOneDocWithoutSectionAttribute);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForTfBitmapOneDoc);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForTfBitmapManyDoc);
INDEXLIB_UNIT_TEST_CASE(PackIndexReaderTest, TestCaseForTfBitmapMultiSegment);

IE_NAMESPACE_END(index);
