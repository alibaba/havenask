#define EXPACK_INDEX_READER_TEST
#define INDEX_ENGINE_UNITTEST

#include <autil/TimeUtility.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/expack/test/expack_index_document_maker.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_position_iterator.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/test/payload_checker.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/util/numeric_util.h"

#include "indexlib/index/test/index_test_util.h"
#include "indexlib/config/index_config_creator.h"
#include <autil/mem_pool/SimpleAllocator.h>

using namespace std;
using namespace std::tr1;
using namespace autil::legacy;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);


IE_NAMESPACE_BEGIN(index);

#define VOL_NAME "reader_vol1"
#define VOL_CONTENT "token0"

class ExpackIndexReaderTest : public INDEXLIB_TESTBASE
{
public:
    typedef NormalInDocPositionIterator ExpackInDocPositionIterator;
    typedef NormalInDocPositionIteratorPtr ExpackInDocPositionIteratorPtr;

public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 10240;

public:
    ExpackIndexReaderTest()
    {
        mVol.reset(new HighFrequencyVocabulary);
        mVol->Init("expack", it_expack, VOL_CONTENT);
    }

    ~ExpackIndexReaderTest() 
    {
    }
public:
    typedef IndexDocumentMaker::Answer Answer;
    typedef IndexDocumentMaker::PostingAnswerMap PostingAnswerMap;
    typedef IndexDocumentMaker::SectionAnswerMap SectionAnswerMap;
    typedef IndexDocumentMaker::KeyAnswer KeyAnswer;
    typedef IndexDocumentMaker::KeyAnswerInDoc KeyAnswerInDoc;

    DECLARE_CLASS_NAME(ExpackIndexReaderTest);
    void CaseSetUp() override
    {
        mTestDir = GET_TEST_DATA_PATH();

        mByteSlicePool = new Pool(&mAllocator, DEFAULT_CHUNK_SIZE);
    }

    void CaseTearDown() override
    {
        ExpackIndexDocumentMaker::CleanDir(mTestDir);
        delete mByteSlicePool;
    }

    void TestCaseForGenFieldMapMask()
    {
        {
            vector<string> termFieldNames;
            termFieldNames.push_back("field5");
            fieldmap_t targetFieldMap = 32;
            TestGenFieldMapMask(termFieldNames, targetFieldMap);
        }
        {
            vector<string> termFieldNames;
            termFieldNames.push_back("field1");
            termFieldNames.push_back("field2");
            fieldmap_t targetFieldMap = 6;
            TestGenFieldMapMask(termFieldNames, targetFieldMap);
        }

        {
            vector<string> termFieldNames;
            termFieldNames.push_back("field0");
            termFieldNames.push_back("field1");
            termFieldNames.push_back("field2");
            termFieldNames.push_back("field3");
            termFieldNames.push_back("field4");
            fieldmap_t targetFieldMap = 31;
            TestGenFieldMapMask(termFieldNames, targetFieldMap);
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
            TestGenFieldMapMask(termFieldNames, targetFieldMap);
        }
        {
            vector<string> termFieldNames;
            termFieldNames.push_back("not_exist");
            fieldmap_t targetFieldMap = 0;
            TestGenFieldMapMask(termFieldNames, targetFieldMap, false);
        }
    }

    void TestCaseForLookUpWithOneDoc()
    {
        TestLookUpWithOneDoc(false, EXPACK_OPTION_FLAG_ALL);
        TestLookUpWithOneDoc(false, EXPACK_OPTION_FLAG_ALL, true);        
    }

    void TestCaseForLookUpWithOneDocWithHighFreq()
    {
        TestLookUpWithOneDoc(true, EXPACK_OPTION_FLAG_ALL);
        TestLookUpWithOneDoc(true, EXPACK_OPTION_FLAG_ALL, true);        
    }

    void TestCaseForNoPayloadLookUpWithOneDoc()
    {
        TestLookUpWithOneDoc(false, EXPACK_NO_PAYLOAD);
        TestLookUpWithOneDoc(false, EXPACK_NO_PAYLOAD, true);        
    }

    void TestCaseForNoPayloadLookUpWithOneDocWithHighFreq()
    {
        TestLookUpWithOneDoc(true, EXPACK_NO_PAYLOAD);
        TestLookUpWithOneDoc(true, EXPACK_NO_PAYLOAD, true);
    }

    void TestCaseForNoPositionListLookUpWithOneDoc()
    {
        TestLookUpWithOneDoc(false, EXPACK_NO_POSITION_LIST);
        TestLookUpWithOneDoc(false, EXPACK_NO_POSITION_LIST, true);        
    }

    void TestCaseForNoPositionListLookUpWithOneDocWithHighFreq()
    {
        TestLookUpWithOneDoc(true, EXPACK_NO_POSITION_LIST);
        TestLookUpWithOneDoc(true, EXPACK_NO_POSITION_LIST, true);        
    }

    void TestCaseForNoTermFrequencyLookupWithOneDoc()
    {
        TestLookUpWithOneDoc(false, EXPACK_NO_TERM_FREQUENCY);
        TestLookUpWithOneDoc(false, EXPACK_NO_TERM_FREQUENCY, true);        
    }

    void TestCaseForNoTermFrequencyLookupWithOneDocWithHighFreq()
    {
        TestLookUpWithOneDoc(true, EXPACK_NO_TERM_FREQUENCY);
        TestLookUpWithOneDoc(true, EXPACK_NO_TERM_FREQUENCY, true);        
    }

    void TestCaseForLookUpWithManyDoc()
    {
        TestLookUpWithManyDoc(false, EXPACK_OPTION_FLAG_ALL, false);
        TestLookUpWithManyDoc(false, EXPACK_OPTION_FLAG_ALL, true);        
    }

    void TestCaseForLookUpWithManyDocWithHighFreq()
    {
        TestLookUpWithManyDoc(true, EXPACK_OPTION_FLAG_ALL, false);
        TestLookUpWithManyDoc(true, EXPACK_OPTION_FLAG_ALL, true);        
    }

    void TestCaseForNoPayloadLookUpWithManyDoc()
    {
        TestLookUpWithManyDoc(false, EXPACK_NO_PAYLOAD, false);
        TestLookUpWithManyDoc(false, EXPACK_NO_PAYLOAD, true);        
    }

    void TestCaseForNoPayloadLookUpWithManyDocWithHighFreq()
    {
        TestLookUpWithManyDoc(true, EXPACK_NO_PAYLOAD, false);
        TestLookUpWithManyDoc(true, EXPACK_NO_PAYLOAD, true);        
    }

    void TestCaseForNoPositionListLookUpWithManyDoc()
    {
        TestLookUpWithManyDoc(false, EXPACK_NO_POSITION_LIST, false);
        TestLookUpWithManyDoc(false, EXPACK_NO_POSITION_LIST, true);        
    }

    void TestCaseForNoPositionListLookUpWithManyDocWithHighFreq()
    {
        TestLookUpWithManyDoc(true, EXPACK_NO_POSITION_LIST, false);
        TestLookUpWithManyDoc(true, EXPACK_NO_POSITION_LIST, true);        
    }

    void TestCaseForNoTermFrequencyLookupWithManyDoc()
    {
        TestLookUpWithManyDoc(false, EXPACK_NO_TERM_FREQUENCY, false);
        TestLookUpWithManyDoc(false, EXPACK_NO_TERM_FREQUENCY, true);        
    }

    void TestCaseForNoTermFrequencyLookupWithManyDocWithHighFreq()
    {
        TestLookUpWithManyDoc(true, EXPACK_NO_TERM_FREQUENCY, false);
        TestLookUpWithManyDoc(true, EXPACK_NO_TERM_FREQUENCY, true);        
    }

    void TestCaseForLookUpWithMultiSegment()
    {
        TestLookUpWithMultiSegment(false, EXPACK_OPTION_FLAG_ALL, false);
        TestLookUpWithMultiSegment(false, EXPACK_OPTION_FLAG_ALL, true); 
    }

    void TestCaseForLookUpWithMultiSegmentWithHighFreq()
    {
        TestLookUpWithMultiSegment(true, EXPACK_OPTION_FLAG_ALL, false);
        TestLookUpWithMultiSegment(true, EXPACK_OPTION_FLAG_ALL, true); 
    }

    void TestCaseForNoPayloadLookUpWithMultiSegment()
    {
        TestLookUpWithMultiSegment(false, EXPACK_NO_PAYLOAD, false);
        TestLookUpWithMultiSegment(false, EXPACK_NO_PAYLOAD, true); 
    }

    void TestCaseForNoPayloadLookUpWithMultiSegmentWithHighFreq()
    {
        TestLookUpWithMultiSegment(true, EXPACK_NO_PAYLOAD, false);
        TestLookUpWithMultiSegment(true, EXPACK_NO_PAYLOAD, true); 
    }

    void TestCaseForNoPositionListLookUpWithMultiSegment()
    {
        TestLookUpWithMultiSegment(false, EXPACK_NO_POSITION_LIST, false);
        TestLookUpWithMultiSegment(false, EXPACK_NO_POSITION_LIST, true); 
    }

    void TestCaseForNoPositionListLookUpWithMultiSegmentWithHighFreq()
    {
        TestLookUpWithMultiSegment(true, EXPACK_NO_POSITION_LIST, false);
        TestLookUpWithMultiSegment(true, EXPACK_NO_POSITION_LIST, true); 
    }

    void TestCaseForNoTermFrequencyLookupWithMultiSegment()
    {
        TestLookUpWithMultiSegment(false, EXPACK_NO_TERM_FREQUENCY, false);
        TestLookUpWithMultiSegment(false, EXPACK_NO_TERM_FREQUENCY, true); 
    }

    void TestCaseForNoTermFrequencyLookupWithMultiSegmentWithHighFreq()
    {
        TestLookUpWithMultiSegment(true, EXPACK_NO_TERM_FREQUENCY, false);
        TestLookUpWithMultiSegment(true, EXPACK_NO_TERM_FREQUENCY, true); 
    }

    void TestDumpEmptySegment(optionflag_t optionFlag)
    {
        vector<uint32_t> docNums;
        docNums.push_back(100);
        docNums.push_back(23);

        SegmentInfos segmentInfos;
        HighFrequencyVocabularyPtr vol;
        IndexConfigPtr indexConfig = CreateIndexConfig(optionFlag, false, false);

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

        ExpackIndexReader indexReader;
        PartitionDataPtr partData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), docNums.size());
        indexReader.Open(indexConfig, partData);
    }

    void TestCaseForDumpEmptySegment()
    {
        TestDumpEmptySegment(EXPACK_OPTION_FLAG_ALL);
    }

    void TestCaseForNoPositionListDumpEmptySegment()
    {
        TestDumpEmptySegment(EXPACK_NO_POSITION_LIST);
    }

    void TestCaseForTfBitmapOneDoc()
    {
        TestLookUpWithOneDoc(true, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap);
        TestLookUpWithOneDoc(true, EXPACK_NO_PAYLOAD | of_tf_bitmap);
        TestLookUpWithOneDoc(true, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap, true);
        TestLookUpWithOneDoc(true, EXPACK_NO_PAYLOAD | of_tf_bitmap, true); 
    }

    void TestCaseForTfBitmapManyDoc()
    {
        TestLookUpWithManyDoc(false, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap, false);
        TestLookUpWithManyDoc(false, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap, true);        
        TestLookUpWithManyDoc(false, EXPACK_NO_PAYLOAD | of_tf_bitmap, false);
        TestLookUpWithManyDoc(false, EXPACK_NO_PAYLOAD | of_tf_bitmap, true);        
    }

    void TestCaseForTfBitmapMultiSegment()
    {
        TestLookUpWithMultiSegment(true, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap, false);
        TestLookUpWithMultiSegment(true, EXPACK_OPTION_FLAG_ALL | of_tf_bitmap, true); 
        TestLookUpWithMultiSegment(true, EXPACK_NO_PAYLOAD | of_tf_bitmap, false);
        TestLookUpWithMultiSegment(true, EXPACK_NO_PAYLOAD | of_tf_bitmap, true); 
    }

private:
    void TestLookUpWithManyDoc(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding)
    {
        vector<uint32_t> docNums;
        docNums.push_back(6);

        IndexConfigPtr indexConfig = CreateIndexConfig(
                optionFlag, hasHighFreq, setMultiSharding);
        Answer answer;
        CreateMultiSegmentsData(docNums, indexConfig, answer);

        IndexReaderPtr indexReader = CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer, true);
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
        IndexWriterPtr writer = CreateIndexWriter(indexConfig, false); 
        vector<IndexDocumentPtr> indexDocs;
        Pool pool;
        PackageIndexConfigPtr packageIndexConfig = 
            dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
        ExpackIndexDocumentMaker::MakeIndexDocuments(&pool, indexDocs, docCount, 
                baseDocId, &answer, packageIndexConfig);
	ExpackIndexDocumentMaker::RewriteSectionAttributeInIndexDocuments(
	    indexDocs, mSchema);
        ExpackIndexDocumentMaker::AddDocsToWriter(indexDocs, *writer);
        segmentInfo.docCount = docCount;

        writer->EndSegment();
        DumpSegment(segId, segmentInfo, writer);
        indexDocs.clear();
    }

    void CheckIndexReaderLookup(const vector<uint32_t>& docNums,
                                IndexReaderPtr& indexReader,
                                const IndexConfigPtr& indexConfig,
                                Answer& answer,
                                bool usePool = false)
    {
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
        PostingAnswerMap::const_iterator keyIter = postingAnswerMap.begin();
        for (; keyIter != postingAnswerMap.end(); ++keyIter)
        {
            string key = keyIter->first;

            common::Term term(key, "");
            Pool pool;
            Pool *pPool = usePool ? &pool : NULL;

            PostingIterator *iter = indexReader->Lookup(term, 1000, pt_default, pPool);
            INDEXLIB_TEST_TRUE(iter != NULL);

            bool hasPos = iter->HasPosition();
            if (hasHighFreq && mVol->Lookup(term.GetWord()))
            {
                PostingIterator *bitmapIt = indexReader->Lookup(term,
                        1000, pt_bitmap, pPool);

                INDEXLIB_TEST_EQUAL(bitmapIt->GetType(), pi_bitmap);
                BitmapPostingIterator *bmIt =
                    dynamic_cast<BitmapPostingIterator*>(bitmapIt);
                INDEXLIB_TEST_TRUE(bmIt != NULL);
                INDEXLIB_TEST_EQUAL(bmIt->HasPosition(),false);
                IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, bitmapIt);
            }
            else
            {
                INDEXLIB_TEST_EQUAL(iter->HasPosition(), 
                        (optionFlag & of_position_list) != 0);
            }
            
            const KeyAnswer& keyAnswer = keyIter->second;
            uint32_t docCount = keyAnswer.docIdList.size();
            docid_t docId = 0;

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
                    INDEXLIB_TEST_EQUAL(answerInDoc.fieldMap, tmd.GetFieldMap());
                }

                if (!hasPos)
                {  
                    continue;
                }

                InDocPositionIteratorPtr inDocPosIter = tmd.GetInDocPositionIterator();
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

                    CheckSection(docId, inDocPosIter, sectionAnswerMap);
                }
            }
            IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, iter);
        }
    }

    void CheckSection(docid_t docId,
                      const InDocPositionIteratorPtr& inDocPosIter, 
                      SectionAnswerMap& sectionAnswerMap)
    {
        ExpackInDocPositionIteratorPtr expackInDocIter = 
            dynamic_pointer_cast<ExpackInDocPositionIterator>(inDocPosIter);
        INDEXLIB_TEST_TRUE(expackInDocIter != NULL);

        stringstream ss;
        ss << docId << ";" << expackInDocIter->GetFieldPosition() << ";" 
           << expackInDocIter->GetSectionId();

        pos_t pos = expackInDocIter->GetCurrentPosition();

        string idString = ss.str();
        pos_t firstPosInSection = sectionAnswerMap[idString].firstPos;
        (void)firstPosInSection; //avoid warning in release mode
        assert(firstPosInSection <= pos);
        INDEXLIB_TEST_TRUE(sectionAnswerMap[ss.str()].firstPos <= pos);

        pos_t endPosInSection =  sectionAnswerMap[idString].firstPos + 
                                 sectionAnswerMap[idString].sectionMeta.length;
        INDEXLIB_TEST_TRUE(pos < endPosInSection);
                    
        INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.length, 
                            expackInDocIter->GetSectionLength());
        INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.weight,
                            expackInDocIter->GetSectionWeight());
        INDEXLIB_TEST_EQUAL(sectionAnswerMap[idString].sectionMeta.fieldId,
                            expackInDocIter->GetFieldPosition());
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
            indexReader.reset(new ExpackIndexReader()); 
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
    
    void TestLookUpWithOneDoc(bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding = false)
    {
        vector<uint32_t> docNums;
        docNums.push_back(1);

        IndexConfigPtr indexConfig = CreateIndexConfig(optionFlag, hasHighFreq, setMultiSharding);
        Answer answer;
        CreateMultiSegmentsData(docNums, indexConfig, answer);
        IndexReaderPtr indexReader = CreateIndexReader(docNums, indexConfig);
        CheckIndexReaderLookup(docNums, indexReader, indexConfig, answer);
    }

    void TestLookUpWithMultiSegment(
            bool hasHighFreq, optionflag_t optionFlag, bool setMultiSharding)
    {
        IndexConfigPtr indexConfig = CreateIndexConfig(
                optionFlag, hasHighFreq, setMultiSharding);
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

    void DumpSegment(
            uint32_t segId, const SegmentInfo& segmentInfo, const IndexWriterPtr& writer)
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

    IndexConfigPtr CreateIndexConfig(optionflag_t optionFlag,
            bool hasHighFreq, bool multiSharding)
    {
        PackageIndexConfigPtr indexConfig; 
        ExpackIndexDocumentMaker::CreatePackageIndexConfig(
	    indexConfig, mSchema, 8, it_expack, "ExpackIndex"); 
        if (hasHighFreq)
        {
            DictionaryConfigPtr dictConfig(new DictionaryConfig(VOL_NAME, VOL_CONTENT)); 
            indexConfig->SetDictConfig(dictConfig); 
            indexConfig->SetHighFreqencyTermPostingType(hp_both); 
        }
        indexConfig->SetOptionFlag(optionFlag); 
        if (multiSharding)
        {
            // set sharding count to 3 
            IndexConfigCreator::CreateShardingIndexConfigs(indexConfig, 3); 
        }
        return indexConfig; 
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

    void TestGenFieldMapMask(const vector<string>& termFieldNames, fieldmap_t targetFieldMap,
                             bool isSuccess = true)
    {
        IndexConfigPtr indexConfig = CreateIndexConfig(EXPACK_OPTION_FLAG_ALL, false, false);        
        ExpackIndexReader indexReader;
        indexReader.SetIndexConfig(indexConfig);
        fieldmap_t fieldMap = 0;
        INDEXLIB_TEST_EQUAL(isSuccess, 
                            indexReader.GenFieldMapMask("", termFieldNames, fieldMap));
        INDEXLIB_TEST_EQUAL(targetFieldMap, fieldMap);
    }

private:
    IndexPartitionSchemaPtr mSchema;
    string mTestDir;
    SimpleAllocator mAllocator;
    SimplePool mSimplePool;
    Pool* mByteSlicePool;
    HighFrequencyVocabularyPtr mVol;
};

INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForGenFieldMapMask);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForLookUpWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForLookUpWithManyDoc);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithManyDoc);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithManyDoc);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForLookUpWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForLookUpWithOneDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithOneDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithOneDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForLookUpWithManyDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithManyDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithManyDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForLookUpWithMultiSegmentWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPayloadLookUpWithMultiSegmentWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPositionListLookUpWithMultiSegmentWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForDumpEmptySegment);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoPositionListDumpEmptySegment);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithOneDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithManyDoc);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithManyDocWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithMultiSegment);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForNoTermFrequencyLookupWithMultiSegmentWithHighFreq);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForTfBitmapOneDoc);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForTfBitmapManyDoc);
INDEXLIB_UNIT_TEST_CASE(ExpackIndexReaderTest, TestCaseForTfBitmapMultiSegment);

IE_NAMESPACE_END(index);
