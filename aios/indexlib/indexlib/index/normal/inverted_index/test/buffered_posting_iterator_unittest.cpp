#include <vector>
#include <tr1/memory>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <fslib/fslib.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"

#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/test/payload_checker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/test/pack_posting_maker.h"
#include "indexlib/index/test/section_data_maker.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"

using namespace std;
using namespace std::tr1;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);


IE_NAMESPACE_BEGIN(index);

class BufferedPostingIteratorTest : public INDEXLIB_TESTBASE
{
public:
    typedef PostingMaker::DocMap AnswerMap;

public:
    DECLARE_CLASS_NAME(BufferedPostingIteratorTest);
    void CaseSetUp() override
    {
        autil::StringUtil::serializeUInt64(0x123456789ULL, mKey);
        mDir = GET_TEST_DATA_PATH();
	mSchema = AttributeTestUtil::MakeSchemaWithPackageIndexConfig(
	    "test_iterator", MAX_FIELD_COUNT);
	
        mIndexConfig = DYNAMIC_POINTER_CAST(
	    PackageIndexConfig,
	    mSchema->GetIndexSchema()->GetIndexConfig("test_iterator"));
        mTestOptionFlags.clear();

        // All
        mTestOptionFlags.push_back(
                of_doc_payload | of_position_payload | of_position_list
                | of_term_frequency | of_tf_bitmap | of_fieldmap);

        // no doc_payload
        mTestOptionFlags.push_back( 
                of_position_payload | of_position_list |
                of_term_frequency | of_tf_bitmap | of_fieldmap);

        // no position_payload
        mTestOptionFlags.push_back( 
                of_doc_payload | of_position_list |
                of_term_frequency | of_tf_bitmap | of_fieldmap);

        // no position_list, no position_payload
        mTestOptionFlags.push_back(
                of_doc_payload |
                of_term_frequency | of_tf_bitmap | of_fieldmap);

        // no term_frequency, no tf_bitmap
        mTestOptionFlags.push_back(
                of_doc_payload | of_fieldmap);
        
        // no fieldmap
        mTestOptionFlags.push_back(
                of_doc_payload | of_position_payload | of_position_list |
                of_term_frequency  | of_tf_bitmap);

        // no tf_bitmap
        mTestOptionFlags.push_back(
                of_doc_payload | of_position_payload | of_position_list |
                of_term_frequency | of_fieldmap);

        // no docpayload, no position_list
        mTestOptionFlags.push_back(
                of_term_frequency | of_tf_bitmap | of_fieldmap);

        // no docpayload, no expack_occ
        mTestOptionFlags.push_back(
                of_position_payload | of_position_list |
                of_term_frequency | of_tf_bitmap | of_fieldmap);

        // no docpayload, no tf_bitmap
        mTestOptionFlags.push_back(
                of_position_list |
                of_term_frequency | of_fieldmap);

        // no docpayload, no fieldmap
        mTestOptionFlags.push_back(
                of_position_payload | of_position_list |
                of_term_frequency  | of_tf_bitmap);

        // no term_frequency, no fieldmap
        mTestOptionFlags.push_back(
                of_doc_payload | of_position_payload | of_position_list |
                of_term_frequency  | of_tf_bitmap);

        // no expack_occ, not tf_bitmap, 
        mTestOptionFlags.push_back(
                of_doc_payload | of_position_payload | of_position_list |
                of_term_frequency  | of_fieldmap);

        // no doc_payload, no position_list, no TF
        mTestOptionFlags.push_back(of_fieldmap);

        // no doc_payload, no position_list, no occ
        mTestOptionFlags.push_back(
                of_term_frequency | of_tf_bitmap | of_fieldmap);

        // no doc_payload, no position_list, no fieldmap
        mTestOptionFlags.push_back(
                of_doc_payload | of_term_frequency);

        // no doc payload, no occ,  no tf_bitmap
        mTestOptionFlags.push_back(
                of_position_payload | of_position_list |
                of_term_frequency | of_fieldmap);

        // no doc payload, no occ,  no fieldmap
        mTestOptionFlags.push_back(
                of_position_payload | of_position_list |
                of_term_frequency | of_tf_bitmap);

        // no doc payload, no tf_bitmap, no fieldmap
        mTestOptionFlags.push_back(
                of_position_payload | of_position_list | of_term_frequency);

        // no position_list, no tf, no occ
        mTestOptionFlags.push_back(
                of_doc_payload | of_fieldmap);

        // no position_list, no tf, no fieldmap
        mTestOptionFlags.push_back(of_doc_payload);
        
        // no doc_payload, no position_list, no tf, no occ
        mTestOptionFlags.push_back(of_fieldmap);

        // no doc_payload, no position_list, no tf_bitmap, no fieldmap
        mTestOptionFlags.push_back(of_term_frequency);

        mTestOptionFlags.push_back(of_term_frequency | of_tf_bitmap);
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForSeekDocInOneSegment()
    {
        for (size_t i = 0; i < mTestOptionFlags.size(); i++)
        {
            TestSeekDocInOneSegment(mTestOptionFlags[i]);
        }
    }

    void TestCaseForSeekDocInTwoSegments()
    {
        for (size_t i = 0; i < mTestOptionFlags.size(); i++)
        {
            TestSeekDocInTwoSegments(mTestOptionFlags[i]);
        }
    }

    void TestCaseForSeekDocInManySegmentsLongCaseTest()
    {
        for (size_t i = 0; i < mTestOptionFlags.size(); i++)
        {
            TestSeekDocInMultiSegments(mTestOptionFlags[i]);
        }
    }

    void TestCaseForUnpack()
    {
        for (size_t i = 0; i < mTestOptionFlags.size(); i++) 
        {
            TestUnpack(mTestOptionFlags[i], false);
            TestUnpack(mTestOptionFlags[i], true);
        }
    }

private:
    void CheckIterator(const vector<string>& strs,
                       const vector<string>& sectionStrs, 
                       optionflag_t optionFlag)
    {
        CheckIterator(strs, sectionStrs, optionFlag, true, false);
        CheckIterator(strs, sectionStrs, optionFlag, true, true);
        CheckIterator(strs, sectionStrs, optionFlag, false, true);
    }

    void CheckIterator(const vector<string>& strs,
                       const vector<string>& sectionStrs, 
                       optionflag_t optionFlag, bool usePool, bool hasSectionAttr)
    {
        TearDown();
        SetUp();
        mIndexConfig->SetHasSectionAttributeFlag(hasSectionAttr);
        mIndexConfig->SetOptionFlag(optionFlag);

        IndexFormatOption indexFormatOption;
        indexFormatOption.Init(mIndexConfig);

        vector<uint32_t> docCounts = CalcDocCounts(sectionStrs);
        vector<docid_t> baseDocIds = CreateBaseDocIds(docCounts);
        vector<uint8_t> compressModes;

        vector<PostingMaker::DocMap> docMaps;

        IndexTestUtil::BuildMultiSegmentsFromDataStrings(strs, mDir, 
                baseDocIds, docMaps, compressModes, indexFormatOption);

        vector<file_system::InMemFileNodePtr> fileReaders;
        SegmentPostingVectorPtr segmentPostingVect;
        PrepareSegmentPostingVector(baseDocIds, compressModes, 
                fileReaders, segmentPostingVect,
                indexFormatOption.GetPostingFormatOption());

        SectionAttributeReaderImplPtr sectionReader;
        if ((optionFlag & of_position_list) && hasSectionAttr)
        {
            SectionDataMaker::BuildMultiSegmentsSectionData(
		mSchema, mIndexConfig->GetIndexName(),
                sectionStrs, GET_PARTITION_DIRECTORY());
            sectionReader = PrepareSectionReader(docCounts.size());
        }

        merger::SegmentDirectoryPtr segDir = IndexTestUtil::CreateSegmentDirectory(mDir,
                docCounts.size());
        autil::mem_pool::Pool sessionPool;
        autil::mem_pool::Pool *pPool = usePool ? &sessionPool : NULL;

        PostingFormatOption postingFormatOption(optionFlag);
        BufferedPostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(pPool,
                BufferedPostingIterator, postingFormatOption, pPool);
        iter->Init(segmentPostingVect, sectionReader.get(), 10);
        CheckAnswer(docMaps, iter, strs.size(), optionFlag);

        BufferedPostingIterator* cloneIter = dynamic_cast<BufferedPostingIterator*>(iter->Clone());
        CheckAnswer(docMaps, cloneIter, strs.size(), optionFlag);

        iter->Reset();
        CheckAnswer(docMaps, iter, strs.size(), optionFlag);

        IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, iter);
        IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, cloneIter);
    }

    void CheckAnswer(const vector<PostingMaker::DocMap>& docMaps, 
                     BufferedPostingIterator* iter, 
                     size_t segCount, optionflag_t optionFlag)
    {
        docid_t oldDocId = -1;
        int count = 0;

        for (size_t i = 0; i < segCount; ++i)
        {
            const PostingMaker::DocMap& docMap = docMaps[i];
            uint32_t seekedDocCount = 0;
            for (PostingMaker::DocMap::const_iterator it = docMap.begin();
                 it != docMap.end(); ++it)
            {
                docid_t docId = it->first.first;
                int dif = docId - oldDocId;
                if (dif > 0)
                {
                    docId -= rand() % dif;
                }

                oldDocId = it->first.first;

                seekedDocCount++;
                if (++count > 10 && (rand() % 4 == 2)) continue;

                docid_t seekedDocId = iter->SeekDoc(docId);
                assert(it->first.first == seekedDocId);
                INDEXLIB_TEST_EQUAL(it->first.first, seekedDocId);
                if (optionFlag & of_doc_payload)
                {
                    INDEXLIB_TEST_EQUAL(it->first.second, iter->GetDocPayload());
                }
                else
                {
                    INDEXLIB_TEST_EQUAL(0, iter->GetDocPayload());
                }

                if (optionFlag & of_position_list)
                {
                    pos_t oldPos = -1;
                    for (PackPostingMaker::PosValue::const_iterator it2 = it->second.begin();
                         it2 != it->second.end(); ++it2)
                    {
                        pos_t pos = it2->first;
                        int posdif = pos - oldPos;
                        if (posdif > 0)
                        {
                            pos -= rand() % posdif; 
                        }
                        pos_t n = iter->SeekPosition(pos);
                        INDEXLIB_TEST_EQUAL(seekedDocCount, iter->mState.GetSeekedDocCount());

                        INDEXLIB_TEST_EQUAL(it2->first, n);
                        if (optionFlag & of_position_payload)
                        {
                            INDEXLIB_TEST_EQUAL(it2->second, iter->GetPosPayload());
                        }
                        else
                        {
                            INDEXLIB_TEST_EQUAL(0, iter->GetPosPayload());
                        }
                        oldPos = it2->first;
                    }
                    INDEXLIB_TEST_EQUAL(INVALID_POSITION, iter->SeekPosition(oldPos + 7));
                }
                else
                {
                    INDEXLIB_TEST_EQUAL(INVALID_POSITION, iter->SeekPosition(0));
                }
            }
        }
        INDEXLIB_TEST_EQUAL(INVALID_DOCID, iter->SeekDoc(oldDocId + 7)); 
    }

    
private:
    SectionAttributeReaderImplPtr PrepareSectionReader(uint32_t segCount)
    {
        SectionAttributeReaderImplPtr reader(new SectionAttributeReaderImpl);
        PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(
                GET_FILE_SYSTEM(), segCount);

        reader->Open(mIndexConfig, partitionData);
        return reader;
    }

    void TestUnpack(optionflag_t optionFlag, bool hasSectionAttr)
    {
        TearDown();
        SetUp();
        string str = "1 3, (1 3, 4 6, 8 1, 12 1, 17 8); "
                     "4 1, (4 2, 17 1)";

        string secStr = "0 1 1;"
                        "0 2 7, 1 8 4, 2 4 3, 2 5 7;" //2 11 16 22
                        "0 1 1;"
                        "0 1 1;"
                        "0 1 1, 1 1 2, 1 1 2, 2 4 4, 2 8 8;"; //1 3 5 10 19
        string segPath = mDir + SEGMENT_FILE_NAME_PREFIX + "_0_level_0/";
        IndexTestUtil::ResetDir(segPath);
        string indexPath = segPath + INDEX_DIR_NAME + "/";
        IndexTestUtil::ResetDir(indexPath);
        string filePath = indexPath + POSTING_FILE_NAME;

        AnswerMap answerMap; // answerMap is not used in checking, and checking is hard-coded.
        docid_t baseDocId = 0;

        mIndexConfig->SetHasSectionAttributeFlag(hasSectionAttr);
        mIndexConfig->SetOptionFlag(optionFlag);

        IndexFormatOption indexFormatOption;
        indexFormatOption.Init(mIndexConfig);
        uint8_t compressMode = IndexTestUtil::BuildOneSegmentFromDataString(
                str, filePath, baseDocId, answerMap, indexFormatOption);
        file_system::InMemFileNodePtr fileReader(file_system::InMemFileNodeCreator::Create());
        fileReader->Open(filePath, file_system::FSOT_IN_MEM);
        fileReader->Populate();

        FileMeta meta;
        FileSystem::getFileMeta(filePath, meta);
        ByteSliceListPtr sliceListPtr(fileReader->Read(meta.fileLength, 0));

        vector<uint32_t> docCounts;
        docCounts.push_back(5);
        vector<docid_t> baseDocIds = CreateBaseDocIds(docCounts);
        vector<uint8_t> compressModes;
        compressModes.push_back(compressMode);

        vector<file_system::InMemFileNodePtr> fileReaders;
        SegmentPostingVectorPtr segmentPostingVect;
        PrepareSegmentPostingVector(baseDocIds, compressModes, 
                fileReaders, segmentPostingVect,
                indexFormatOption.GetPostingFormatOption());

        PostingFormatOption postingFormatOption = 
            indexFormatOption.GetPostingFormatOption();
        
        BufferedPostingIteratorPtr iter(
                new BufferedPostingIterator(postingFormatOption, NULL));

        SectionAttributeReaderImplPtr sectionReader;
        if ((optionFlag & of_position_list) && hasSectionAttr)
        {
            SectionDataMaker::BuildOneSegmentSectionData(
		mSchema, mIndexConfig->GetIndexName(),
                secStr, GET_PARTITION_DIRECTORY(), 0);
	    
	    sectionReader = PrepareSectionReader(docCounts.size());
        }
        merger::SegmentDirectoryPtr segDir = IndexTestUtil::CreateSegmentDirectory(
                mDir, docCounts.size());
        iter->Init(segmentPostingVect, sectionReader.get(), 10);
        
        INDEXLIB_TEST_EQUAL(1, iter->SeekDoc(0));
        TermMatchData tmd;
        iter->Unpack(tmd);
        INDEXLIB_TEST_TRUE(tmd.IsMatched());

        if (optionFlag & of_term_frequency)
        {
            INDEXLIB_TEST_EQUAL(5, tmd.GetTermFreq());
        }
        else
        {
            INDEXLIB_TEST_TRUE(tmd.GetInDocPositionState() == NULL);
        }

        if (optionFlag & of_doc_payload)
        {
            INDEXLIB_TEST_EQUAL(3, tmd.GetDocPayload());
        }

        if (optionFlag & of_fieldmap)
        {
            INDEXLIB_TEST_EQUAL(19, tmd.GetFieldMap());
        }
        
        InDocPositionIteratorPtr posIter = tmd.GetInDocPositionIterator();
        if (optionFlag & of_position_list)
        {
            INDEXLIB_TEST_TRUE(posIter != NULL);

            INDEXLIB_TEST_EQUAL((uint32_t)1, posIter->SeekPosition(0));
            if (optionFlag & of_position_payload)
            {
                INDEXLIB_TEST_EQUAL(3, posIter->GetPosPayload());
            }

            if (hasSectionAttr)
            {
                INDEXLIB_TEST_EQUAL(0, posIter->GetSectionId());
                INDEXLIB_TEST_EQUAL(2, posIter->GetSectionLength());
                INDEXLIB_TEST_EQUAL(7, posIter->GetSectionWeight());
                INDEXLIB_TEST_EQUAL(0, posIter->GetFieldId());
            }

            INDEXLIB_TEST_EQUAL((size_t)4, posIter->SeekPosition(3));
            if (optionFlag & of_position_payload)
            {
                INDEXLIB_TEST_EQUAL(6, posIter->GetPosPayload());
            }

            if (hasSectionAttr)
            {
                INDEXLIB_TEST_EQUAL(0, posIter->GetSectionId());
                INDEXLIB_TEST_EQUAL(8, posIter->GetSectionLength());
                INDEXLIB_TEST_EQUAL(4, posIter->GetSectionWeight());
                INDEXLIB_TEST_EQUAL(1, posIter->GetFieldId());
            }

            //test the second in doc position iterator
            if (optionFlag & of_position_list)
            {
                InDocPositionIteratorPtr posIter2
                    = tmd.GetInDocPositionIterator();

                INDEXLIB_TEST_EQUAL((size_t)1, posIter2->SeekPosition(0));
                if (optionFlag & of_position_payload)
                {
                    INDEXLIB_TEST_EQUAL(3, posIter2->GetPosPayload());
                }

                INDEXLIB_TEST_EQUAL((size_t)4, posIter2->SeekPosition(3));
                if (optionFlag & of_position_payload)
                {
                    INDEXLIB_TEST_EQUAL(6, posIter2->GetPosPayload());
                }
            }

            INDEXLIB_TEST_EQUAL((size_t)8, posIter->SeekPosition(5));
            if (optionFlag & of_position_payload)
            {
                INDEXLIB_TEST_EQUAL(1, posIter->GetPosPayload());
            }

            if (hasSectionAttr)
            {
                INDEXLIB_TEST_EQUAL(0, posIter->GetSectionId());
                INDEXLIB_TEST_EQUAL(8, posIter->GetSectionLength());
                INDEXLIB_TEST_EQUAL(4, posIter->GetSectionWeight());
                INDEXLIB_TEST_EQUAL(1, posIter->GetFieldId());
            }
            INDEXLIB_TEST_EQUAL((size_t)12, posIter->SeekPosition(10));
            if (optionFlag & of_position_payload)
            {
                INDEXLIB_TEST_EQUAL(1, posIter->GetPosPayload());
            }
            if (hasSectionAttr)
            {
                INDEXLIB_TEST_EQUAL(0, posIter->GetSectionId());
                INDEXLIB_TEST_EQUAL(4, posIter->GetSectionLength());
                INDEXLIB_TEST_EQUAL(3, posIter->GetSectionWeight());
                INDEXLIB_TEST_EQUAL(2, posIter->GetFieldId());
            }

            INDEXLIB_TEST_EQUAL((size_t)17, posIter->SeekPosition(13));
            if (optionFlag & of_position_payload)
            {
                INDEXLIB_TEST_EQUAL(8, posIter->GetPosPayload());
            }

            if (hasSectionAttr)
            {
                INDEXLIB_TEST_EQUAL(1, posIter->GetSectionId());
                INDEXLIB_TEST_EQUAL(5, posIter->GetSectionLength());
                INDEXLIB_TEST_EQUAL(7, posIter->GetSectionWeight());
                INDEXLIB_TEST_EQUAL(2, posIter->GetFieldId());
            }
            INDEXLIB_TEST_EQUAL(INVALID_POSITION, posIter->SeekPosition(999));
        }
        else
        {
            INDEXLIB_TEST_TRUE(posIter == NULL);
        }

        INDEXLIB_TEST_EQUAL(4, iter->SeekDoc(1));
        TermMatchData nextTmd;
        iter->Unpack(nextTmd);
        INDEXLIB_TEST_TRUE(nextTmd.IsMatched());
    }

    void TestSeekDocInOneSegment(optionflag_t optionFlag)
    {
        string str1 = "1 2, (1 3); 2 1, (5 1, 9 2); 7 0, (1 7)";
        vector<string> strs;
        strs.push_back(str1);

        vector<string> sectStrs;
        SectionDataMaker::CreateFakeSectionString(8, 20, strs, sectStrs);
        CheckIterator(strs, sectStrs, optionFlag);
    }

    void TestSeekDocInTwoSegments(optionflag_t optionFlag)
    {
        string str1 = "1 2, (1 3); 2 1, (5 1, 9 2)";
        string str2 = "11 2, (1 3); 12 1, (5 1, 9 2); 17 0, (1 7)";
        vector<string> strs;
        strs.push_back(str1);
        strs.push_back(str2);

        vector<string> sectStrs;
        SectionDataMaker::CreateFakeSectionString(18, 20, strs, sectStrs);
        CheckIterator(strs, sectStrs, optionFlag);
    }

    void TestSeekDocInMultiSegments(optionflag_t optionFlag)
    {
        const int32_t keyCount[] = {5, 17};
        for(size_t k = 0; k < sizeof(keyCount) / sizeof(int32_t); ++k)
        {
            vector<string> strs;
            int oldDocId = 0;
            int maxPos = 0;
            for (int x = 0; x < keyCount[k]; ++x)
            {
                ostringstream oss;
                int docCount = 23 + rand() % 123;
                for (int i = 0; i < docCount; ++i)
                {
                    oss << (oldDocId += rand() % 19 + 1) << " ";
                    oss << rand() % 54321 << ", (";
                    int posCount = rand() % 10 + 1;
                    if (rand() % 37 == 25)
                    {
                        posCount += rand() % 124;
                    }
                    int oldPos = 0;

                    for (int j = 0; j < posCount; ++j)
                    {
                        oss << (oldPos += rand() % 7 + 1) << " ";
                        oss << rand() % 77 << ", ";
                    }
                    oss << ");";
                    if (oldPos > maxPos)
                    {
                        maxPos = oldPos;
                    }
                }
                strs.push_back(oss.str());
            }
            vector<string> sectStrs;
            SectionDataMaker::CreateFakeSectionString(oldDocId, maxPos, strs, sectStrs);
            CheckIterator(strs, sectStrs, optionFlag);
        }
    }

    vector<uint32_t> CalcDocCounts(const vector<string>& sectionStrs)
    {
        vector<uint32_t> docCounts;
        for (size_t i = 0 ; i < sectionStrs.size(); ++i) 
        {
            StringTokenizer st(sectionStrs[i], ";", StringTokenizer::TOKEN_TRIM |
                    StringTokenizer::TOKEN_IGNORE_EMPTY);
            
            docCounts.push_back(st.getNumTokens());
        }
        return docCounts;
    }

    void PrepareSegmentPostingVector(const vector<docid_t>& baseDocIds,
            const vector<uint8_t>& compressModes,
            vector<file_system::InMemFileNodePtr>& fileReaders,
            SegmentPostingVectorPtr& segmentPostingVect,
            const PostingFormatOption& postingFormatOption)
    {
        segmentPostingVect.reset(new SegmentPostingVector);
        for (size_t i = 0; i < baseDocIds.size(); ++i) 
        {
            stringstream dumpDir;
            dumpDir << mDir << SEGMENT_FILE_NAME_PREFIX << "_" << i << "_level_0" << "/";
            string filePath = dumpDir.str() + INDEX_DIR_NAME + "/" + POSTING_FILE_NAME;

            FileMeta meta;
            FileSystem::getFileMeta(filePath, meta);
            
            fileReaders.push_back(file_system::InMemFileNodePtr(
                            file_system::InMemFileNodeCreator::Create()));
            fileReaders[i]->Open(filePath, file_system::FSOT_IN_MEM);
            fileReaders[i]->Populate();
            ByteSliceListPtr sliceListPtr(fileReaders[i]->Read(meta.fileLength, 0));

            SegmentInfo segmentInfo;
            segmentInfo.docCount = 0;
            SegmentPosting segmentPosting(postingFormatOption);

            segmentPosting.Init(compressModes[i], sliceListPtr,
                    baseDocIds[i], segmentInfo);
            segmentPostingVect->push_back(segmentPosting);
        }
    }                                    

    vector<docid_t> CreateBaseDocIds(const vector<uint32_t>& docCounts)
    {
        vector<docid_t> baseDocIds;
        docid_t baseDocId = 0;
        for (size_t i = 0; i < docCounts.size(); ++i)
        {
            baseDocIds.push_back(baseDocId);
            baseDocId += docCounts[i];
        }        
        return baseDocIds;
    }

private:
    static const uint32_t MAX_DOC_NUM = 1024 * 16;
    static const uint32_t MAX_FIELD_COUNT = 32;
    string mKey;
    string mDir;
    vector<optionflag_t> mTestOptionFlags;
    PackageIndexConfigPtr mIndexConfig;
    IndexPartitionSchemaPtr mSchema;
};

INDEXLIB_UNIT_TEST_CASE(BufferedPostingIteratorTest, TestCaseForSeekDocInOneSegment);
INDEXLIB_UNIT_TEST_CASE(BufferedPostingIteratorTest, TestCaseForSeekDocInTwoSegments);
INDEXLIB_UNIT_TEST_CASE(BufferedPostingIteratorTest, TestCaseForSeekDocInManySegmentsLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(BufferedPostingIteratorTest, TestCaseForUnpack);

IE_NAMESPACE_END(index);
