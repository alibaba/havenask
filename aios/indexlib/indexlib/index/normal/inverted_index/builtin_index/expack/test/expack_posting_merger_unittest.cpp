#include <autil/mem_pool/SimpleAllocator.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/expack/on_disk_expack_index_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/expack/test/expack_posting_maker.h"
#include "indexlib/index/test/posting_maker.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/test/pack_position_maker.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/impl/package_index_config_impl.h"
#include "indexlib/index/test/section_data_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/fake_reclaim_map.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger_impl.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"
#include "indexlib/file_system/normal_file_reader.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"

using namespace std;
using namespace autil::mem_pool;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);


IE_NAMESPACE_BEGIN(index);

#define INDEX_REL_PATH "index/phrase"

// TODO : move to PostingMergerImplTest
class ExpackPostingMergerTest : public INDEXLIB_TESTBASE
{
public:
    typedef PostingMaker::DocMap DocMap;
    typedef PostingMaker::DocKey DocKey;
    typedef PostingMaker::DocRecordVector DocRecordVector;
    typedef PostingMaker::DocRecord DocRecord;
    typedef PostingMaker::PostingList PostingList;
    typedef vector<PostingList> PostingListVec;

    typedef ExpackPostingMaker::ExpackDocMapWithFieldMap ExpackDocMapWithFieldMap;
    typedef ExpackPostingMaker::ExpackDocRecordVector ExpackDocRecordVector;
    typedef ExpackPostingMaker::ExpackDocRecord ExpackDocRecord;
    typedef ExpackPostingMaker::ExpackPostingList ExpackPostingList;
    typedef vector<ExpackPostingList> ExpackPostingListVec;

    DECLARE_CLASS_NAME(ExpackPostingMergerTest);
    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH();
        mMaxDocId = 0;
        mMaxPos = 50;

        InitPool();
        mDeletionMap.reset(new DeletionMapReader());
        mReclaimMap.reset(new ReclaimMap());
        mIndexConfig.reset(new PackageIndexConfig("phrase", it_expack));
    }

    void CaseTearDown() override
    {
    }

    void TestMergeOnePosting()
    {
        InternalTestOnePosting(EXPACK_OPTION_FLAG_ALL);
    }

    void TestMergeOnePostingWithoutTf()
    {
        InternalTestOnePosting(EXPACK_NO_TERM_FREQUENCY);
    }

    void TestMergeOnePostingWithoutPosition()
    {
        InternalTestOnePosting(EXPACK_NO_POSITION_LIST);
    }
    
    void TestMergeOnePostingWithTfBitmap()
    {
        InternalTestOnePosting(EXPACK_OPTION_FLAG_ALL);
    }

    void TestMergeOnePostingWithTfBitmapWithoutPayload()
    {
        InternalTestOnePosting(EXPACK_NO_PAYLOAD);
    }

    void TestMergeOnePostingWithTfBitmapWithoutPosition()
    {
        InternalTestOnePosting(EXPACK_NO_POSITION_LIST);
    }

    void TestSimpleMerge()
    {
        InternalTestSimpleMerge(EXPACK_OPTION_FLAG_ALL);
    }

    void TestSimpleMergeWithoutTf()
    {
        InternalTestSimpleMerge(EXPACK_NO_TERM_FREQUENCY);
    }

    void TestSimpleMergeWithoutPosition()
    {
        InternalTestOnePosting(EXPACK_NO_POSITION_LIST);
    }
    
    void TestSimpleMergeWithTfBitmap()
    {
        InternalTestSimpleMerge(EXPACK_OPTION_FLAG_ALL);
    }

    void TestSimpleMergeWithTfBitmapWithoutPayload()
    {
        InternalTestSimpleMerge(EXPACK_NO_PAYLOAD);
    }

    void TestSimpleMergeWithTfBitmapWithoutPosition()
    {
        InternalTestSimpleMerge(EXPACK_NO_POSITION_LIST| of_tf_bitmap);
    }
    
    void TestMultiMerge(optionflag_t optionFlag)
    {
        vector<uint32_t> docNums;
        for (uint32_t i = 0; i < 50; i++)
        {
            docNums.push_back(i % 300 + 2);
        }

        InternalTestMerge(docNums, optionFlag);
        InternalTestMerge(docNums, optionFlag, true);
    }

    void TestMultiMergeWithoutPosList()
    {
        TestMultiMerge(EXPACK_NO_POSITION_LIST);
    }

    void TestMultiMergeWithPosList()
    {
        TestMultiMerge(EXPACK_OPTION_FLAG_ALL);
    }

    void TestMultiMergeWithoutPayloads()
    {
        TestMultiMerge(EXPACK_NO_PAYLOAD);
    }

    void TestMultiMergeWithoutTf()
    {
        TestMultiMerge(EXPACK_NO_TERM_FREQUENCY);
    }

    void TestMultiMergeWithTfBitmap()
    {
        TestMultiMerge(EXPACK_OPTION_FLAG_ALL);
    }

    void TestMultiMergeWithTfBitmapWithoutPayload()
    {
        TestMultiMerge(EXPACK_NO_PAYLOAD);
    }

    void TestMultiMergeWithTfBitmapWithoutPosition()
    {
        TestMultiMerge(EXPACK_NO_POSITION_LIST);
    }
    
    void TestSpecificMerge()
    {
        uint32_t sampleNumbers[] = {
           1,
           MAX_DOC_PER_RECORD / 3 + 1,
           MAX_DOC_PER_RECORD + 1,
           MAX_DOC_PER_RECORD * 2 + 1,
           MAX_DOC_PER_RECORD * 2 + MAX_DOC_PER_RECORD / 3 + 1,
        };

        vector<uint32_t> docNums(sampleNumbers, 
                sampleNumbers + sizeof(sampleNumbers) / sizeof(uint32_t));
        InternalTestMerge(docNums, EXPACK_OPTION_FLAG_ALL);
        InternalTestMerge(docNums, EXPACK_OPTION_FLAG_ALL, true);
    }

    

private:
    void InternalTestOnePosting(optionflag_t optionFlag)
    {
        vector<uint32_t> docNums;
        docNums.push_back(200);
        InternalTestMerge(docNums, optionFlag);
        InternalTestMerge(docNums, optionFlag, true);
    }

    void InternalTestSimpleMerge(optionflag_t optionFlag)
    {
        vector<uint32_t> docNums;
        docNums.push_back(20);
        docNums.push_back(40);
        InternalTestMerge(docNums, optionFlag);
        InternalTestMerge(docNums, optionFlag, true);
    }

    void InternalTestMerge(const vector<uint32_t>& docNums, 
                           optionflag_t optionFlag,
                           bool isSortByWeight = false)
    {
        // make data
        FOR_EACH (i, IndexTestUtil::deleteFuncs)
        {
            TearDown();
            SetUp();
            mIndexConfig->SetOptionFlag(optionFlag);
            IndexFormatOption indexFormatOption;
            indexFormatOption.Init(mIndexConfig);

            PostingListVec postLists;
            ExpackPostingListVec expackPostLists;
            SegmentInfos segInfos;
            MakeData(optionFlag, docNums, postLists, 
                     expackPostLists, segInfos);
        
            SegmentMergeInfos segMergeInfos;
            SegmentTermInfos segTermInfos;
            CreateSegmentTermInfos(segInfos, postLists, 
                    segTermInfos, segMergeInfos, optionFlag);


            
            PostingMergerImplPtr merger = CreateMerger(optionFlag);
            

            //DeletionMapper before merge
            mDeletionMap = CreateDeletionMap(segInfos, IndexTestUtil::deleteFuncs[i]);
            OfflineAttributeSegmentReaderContainerPtr readerContainer;
            if (isSortByWeight) 
            {
                mReclaimMap.reset(new FakeReclaimMap());
                mReclaimMap->Init(segMergeInfos, mDeletionMap, readerContainer, false);
                merger->SortByWeightMerge(segTermInfos, mReclaimMap);
            }
            else
            {
                mReclaimMap.reset(new ReclaimMap());
                mReclaimMap->Init(segMergeInfos, mDeletionMap, readerContainer, false);
                merger->Merge(segTermInfos, mReclaimMap);
            }

            DirectoryPtr directory = DirectoryCreator::Create(mDir);
            stringstream ss;
            ss << "merged.post." << i;
            string mergedName = ss.str();

            IndexOutputSegmentResources outputSegmentResources;
            IndexOutputSegmentResourcePtr resource(new IndexOutputSegmentResource);
            util::SimplePool simplePool;
            FileWriterPtr mergedSegFile = directory->CreateFileWriter(mergedName);
            resource->mNormalIndexDataWriter.reset(new IndexDataWriter());
            resource->mNormalIndexDataWriter->postingWriter = mergedSegFile;
            resource->mNormalIndexDataWriter->dictWriter.reset(new TieredDictionaryWriter<dictkey_t>(&simplePool));
            resource->mNormalIndexDataWriter->dictWriter->Open(directory, mergedName + "_dict");
            outputSegmentResources.push_back(resource);
            merger->Dump(0, outputSegmentResources);
            resource->DestroyIndexDataWriters();

            // make answer
            PostingList answerList;
            ExpackPostingList expackAnswerList;
            if (isSortByWeight)
            {
                answerList = PostingMaker::SortByWeightMergePostingLists(
                        postLists, *mReclaimMap);
                expackAnswerList = ExpackPostingMaker::SortByWeightMergePostingLists(
                        expackPostLists, *mReclaimMap);
            }
            else
            {
                PostingList tmpAnswerList = PostingMaker::MergePostingLists(
                        postLists);
                ExpackPostingList tmpExpackAnswerList = 
                    ExpackPostingMaker::MergeExpackPostingLists(expackPostLists);

                tmpAnswerList.first = 0;
                tmpExpackAnswerList.first = 0;
                PostingMaker::CreateReclaimedPostingList(answerList, 
                        tmpAnswerList, *mReclaimMap);
                ExpackPostingMaker::CreateReclaimedPostingList(expackAnswerList, 
                        tmpExpackAnswerList, *mReclaimMap);
            }
            uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(
                    merger->GetDocFreq(), merger->GetTotalTF(), PFOR_DELTA_COMPRESS_MODE);
            CheckPosting(mDir + mergedName, answerList, expackAnswerList,
                         compressMode, optionFlag);

            for (uint32_t j = 0; j < segTermInfos.size(); j++)
            {
                delete segTermInfos[j];
            }
        }
    }

    void MakeData(optionflag_t optionFlag, const vector<uint32_t>& docNums, 
                  PostingListVec& postingLists, 
                  ExpackPostingListVec& expackPostingLists,
                  SegmentInfos& segInfos)
    {
        docid_t maxDocId = 0;
        postingLists.resize(docNums.size());
        expackPostingLists.resize(docNums.size());
        for (uint32_t i = 0; i < docNums.size(); i++)
        {
            stringstream ss;
            ss << mDir << "segment_" << i << "/" << INDEX_REL_PATH;
            string path = ss.str();

            MakeOneSegment(path, optionFlag, docNums[i], 
                           postingLists[i], expackPostingLists[i], maxDocId);

            postingLists[i].first = mMaxDocId;
            expackPostingLists[i].first = mMaxDocId;
            mMaxDocId += (maxDocId + 1);

            SegmentInfo segInfo;
            segInfo.docCount = (maxDocId + 1);
            segInfos.push_back(segInfo);
        }
        mSecAttrReader = CreateFakeSectionAttribute();
    }

    void CreateSegmentTermInfos(const SegmentInfos& segInfos, 
                                const PostingListVec& postLists,
                                SegmentTermInfos& segTermInfos,
                                SegmentMergeInfos& segMergeInfos,
                                optionflag_t optionFlag)
    {
        for (uint32_t i = 0; i < postLists.size(); i++)
        {
            stringstream ss;
            ss << "segment_" << i << "/" << INDEX_REL_PATH;

            DirectoryPtr indexDirectory = 
                GET_PARTITION_DIRECTORY()->GetDirectory(ss.str(), true);

            OnDiskExpackIndexIterator* onDiskIter = new OnDiskExpackIndexIterator(
                    mIndexConfig, indexDirectory, optionFlag);
            onDiskIter->Init();

            IndexIteratorPtr indexIter(onDiskIter);
            
            SegmentTermInfo* segTermInfo = new SegmentTermInfo(
                    (segmentid_t)i, postLists[i].first,indexIter);
            segTermInfos.push_back(segTermInfo);

            SegmentMergeInfo segMergeInfo;
            segMergeInfo.segmentId = (segmentid_t)i;
            segMergeInfo.baseDocId = postLists[i].first;
            segMergeInfo.deletedDocCount = 0;
            segMergeInfo.segmentInfo = segInfos[i];
            
            segMergeInfos.push_back(segMergeInfo);
            INDEXLIB_TEST_TRUE(segTermInfo->Next());
        }
    }

    void CheckPosting(const string& mergedPath, 
                      const PostingList& answerList, 
                      const ExpackPostingList& expackAnswerList, 
                      uint8_t compressMode,
                      optionflag_t optionFlag)
    {
        InMemFileNodePtr fileNode(InMemFileNodeCreator::Create());
        fileNode->Open(mergedPath, FSOT_IN_MEM);
        NormalFileReader fileReader(fileNode);
        fileReader.Open();
        if (answerList.second.size() ==0 )
        {
            INDEXLIB_TEST_EQUAL(0, fileReader.GetLength());
            return;
        }
        uint32_t postingLen = fileReader.ReadVUInt32();
        ByteSliceList* sliceList = fileReader.Read(postingLen,
                VByteCompressor::GetVInt32Length(postingLen));

        PostingFormatOption postingFormatOption(optionFlag);
        SegmentPosting segPosting(postingFormatOption);
        segPosting.Init(compressMode, ByteSliceListPtr(sliceList), 
                        0, SegmentInfo());
        
        SegmentPostingVectorPtr segPostings(new SegmentPostingVector);
        segPostings->push_back(segPosting);
        BufferedPostingIteratorPtr iter(new BufferedPostingIterator(postingFormatOption, NULL));
        iter->Init(segPostings, mSecAttrReader.get(), 2000);

        //begin check
        DocRecordVector docRecordVec = answerList.second;
        ExpackDocRecordVector expackDocRecordVec = expackAnswerList.second;
        
        uint32_t recordCursor = 0;
        if (docRecordVec.size() == 0)
        {
            return;
        }

        DocRecord currDocRecord = docRecordVec[0];
        ExpackDocRecord currExpackDocRecord = expackDocRecordVec[0];

        docid_t docId = 0;
        docpayload_t docPayload = 0;
        DocMap::const_iterator inRecordCursor = currDocRecord.begin();
        ExpackDocMapWithFieldMap::const_iterator inExpackRecordCursor = 
            currExpackDocRecord.begin();

        while ((docId = iter->SeekDoc(docId)) != INVALID_DOCID)
        {
            if (inRecordCursor == currDocRecord.end())
            {
                recordCursor++;
                currDocRecord = docRecordVec[recordCursor]; 
                currExpackDocRecord = expackDocRecordVec[recordCursor];
                inRecordCursor = currDocRecord.begin();
                inExpackRecordCursor = currExpackDocRecord.begin();
            }
            DocKey docKey = inRecordCursor->first;
            
            INDEXLIB_TEST_EQUAL(docKey.first, docId);

            if (optionFlag & of_doc_payload)
            {
                docPayload = iter->GetDocPayload();
                INDEXLIB_TEST_EQUAL(docKey.second, docPayload);
            }

            TermMatchData termMatchData;
            iter->Unpack(termMatchData);

            INDEXLIB_TEST_EQUAL(inExpackRecordCursor->second, 
                    termMatchData.GetFieldMap());
            ASSERT_FALSE(termMatchData.HasFirstOcc());

            PostingMaker::PosValue posValue = inRecordCursor->second;
            if (optionFlag & of_term_frequency)
            {
                INDEXLIB_TEST_EQUAL(posValue.size(),
                        (size_t)termMatchData.GetTermFreq());
            }
            else
            {
                INDEXLIB_TEST_EQUAL(1, termMatchData.GetTermFreq());
            }
            
            if (optionFlag & of_position_list)
            {
                uint32_t posCursor = 0;
                pos_t pos =0;
                pospayload_t posPayload = 0;
                while ((pos = iter->SeekPosition(pos)) != INVALID_POSITION)
                {
                    INDEXLIB_TEST_EQUAL(posValue[posCursor].first, pos);
                    if (optionFlag & of_position_payload)
                    {
                        posPayload = iter->GetPosPayload();
                        INDEXLIB_TEST_EQUAL(posValue[posCursor].second, posPayload);
                    }
                    posCursor++;
                }
            }
            inRecordCursor++;
            inExpackRecordCursor++;
        }
    }
    
    void InitPool()
    {
        mByteSlicePool.reset();
        mBufferPool.reset();
        mAllocator.reset();
        
        SimpleAllocator* allocator = new SimpleAllocator();
        mAllocator.reset(allocator);

        Pool* byteSlicePool = new Pool(allocator, DEFAULT_CHUNK_SIZE);
        mByteSlicePool.reset(byteSlicePool);

        RecyclePool* bufferPool = new RecyclePool(allocator, DEFAULT_CHUNK_SIZE);
        mBufferPool.reset(bufferPool);
    }

    PostingMergerImplPtr CreateMerger(optionflag_t flag)
    {
        index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
        index_base::OutputSegmentMergeInfo outputSegMergeInfo;
        outputSegMergeInfo.path = mDir;
        outputSegMergeInfo.directory = DirectoryCreator::Create(mDir);
        outputSegMergeInfo.targetSegmentId = 100;
        outputSegmentMergeInfos.push_back(outputSegMergeInfo);
        PostingMergerImplPtr merger;
        PostingFormatOption formatOption(flag);
        mWriterResource.reset(new PostingWriterResource(&mSimplePool,
                        mByteSlicePool.get(), mBufferPool.get(), formatOption));
        
        merger.reset(new PostingMergerImpl(mWriterResource.get(), outputSegmentMergeInfos));
        return merger;
    }

    PostingWriterImplPtr CreateWriter(optionflag_t flag)
    {
        PostingFormatOption formatOption(flag);
        mWriterResource.reset(new PostingWriterResource(&mSimplePool,
                        mByteSlicePool.get(), mBufferPool.get(), formatOption));
        PostingWriterImplPtr writer(new PostingWriterImpl(mWriterResource.get(), true));
        return writer;
    }


    void MakeOneSegment(const string& indexPath, 
                        optionflag_t optionFlag, uint32_t docNum, 
                        PostingList& postList, 
                        ExpackPostingList& expackPostList,
                        docid_t& maxDocId)
    {
        storage::FileSystemWrapper::MkDir(indexPath, true);
        auto indexDir = DirectoryCreator::Create(indexPath);

        PostingWriterImplPtr writer = CreateWriter(optionFlag);
        uint32_t totalTF;
        postList = PostingMaker::MakePositionData(docNum, maxDocId, totalTF);
        ExpackPostingMaker::MakeOneSegmentWithWriter(*writer,
                postList, expackPostList);
        
        writer->EndSegment();

        TermMeta termMeta(writer->GetDF(), writer->GetTotalTF());
        if (optionFlag & of_term_payload)
        {
            termMeta.SetPayload(writer->GetTermPayload());
        }

        PostingFormatOption options;
        options.SetIsCompressedPostingHeader(false);
        TermMetaDumper tmDumper(options);
        uint32_t postingLen = tmDumper.CalculateStoreSize(termMeta) + writer->GetDumpLength();
        IE_LOG(TRACE1, "Writing Posting length: [%u] ", postingLen);        

        FileWriterPtr file = indexDir->CreateFileWriter(POSTING_FILE_NAME);
        file->Write((void*)(&postingLen), sizeof(uint32_t));

        tmDumper.Dump(file, termMeta);
        writer->Dump(file);
        file->Close();
        writer.reset();

        TieredDictionaryWriter<dictkey_t> dictWriter(&mSimplePool);
        dictWriter.Open(indexDir, DICTIONARY_FILE_NAME);
        dictWriter.AddItem(0, 0);
        dictWriter.Close();
    }

    string CreateFakeSectionString(docid_t maxDocId, pos_t maxPos)
    {
        stringstream ss;
        for (docid_t i = 0; i <= maxDocId; i++)
        {
            pos_t secNum = maxPos / 0x7ff + 1;
            for (uint32_t j = 0; j < secNum; ++j)
            {
                ss << j << " 2047 1, ";
            }
            ss << ";";
        }
        return ss.str();
    }

    SectionAttributeReaderImplPtr CreateFakeSectionAttribute()
    {
        string sectionStr = CreateFakeSectionString(mMaxDocId, mMaxPos);
	IndexPartitionSchemaPtr schema =
	    AttributeTestUtil::MakeSchemaWithPackageIndexConfig(
		mIndexConfig->GetIndexName(), 8, it_expack);

	PackageIndexConfigPtr packIndexConfig = DYNAMIC_POINTER_CAST(
	    PackageIndexConfig, schema->GetIndexSchema()->GetIndexConfig(
		mIndexConfig->GetIndexName()));
	
	SectionDataMaker::BuildOneSegmentSectionData(
	    schema, mIndexConfig->GetIndexName(),
	    sectionStr, GET_PARTITION_DIRECTORY(), 0);

        SectionAttributeReaderImplPtr reader(new SectionAttributeReaderImpl);
        PartitionDataPtr partData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), 1);
        reader->Open(mIndexConfig, partData);
        return reader;
    }

    DeletionMapReaderPtr CreateDeletionMap(const SegmentInfos& segInfos, 
            IndexTestUtil::ToDelete toDelete)
    {
        vector<uint32_t> docCounts;
        for (uint32_t i = 0; i < segInfos.size(); i++)
        {
            docCounts.push_back((uint32_t)segInfos[i].docCount);
        }
        DeletionMapReaderPtr deletionMap = 
            IndexTestUtil::CreateDeletionMapReader(docCounts);
        for (uint32_t j = 0; j < segInfos.size(); ++j)
        {
            IndexTestUtil::CreateDeletionMap((segmentid_t)j, 
                    segInfos[j].docCount, toDelete, deletionMap);   
        }
        return deletionMap;
    }

private:
    string mDir;
    uint32_t mMaxDocId;
    pos_t mMaxPos;
    
    DeletionMapReaderPtr mDeletionMap;
    ReclaimMapPtr mReclaimMap;
    SectionAttributeReaderImplPtr mSecAttrReader;
    PackageIndexConfigPtr mIndexConfig;

    SimpleAllocatorPtr mAllocator;
    std::tr1::shared_ptr<Pool> mByteSlicePool;
    std::tr1::shared_ptr<RecyclePool> mBufferPool;
    SimplePool mSimplePool;
    PostingWriterResourcePtr mWriterResource;
    static const size_t DEFAULT_CHUNK_SIZE = 100 * 1024;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, ExpackPostingMergerTest);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMergeOnePosting);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMergeOnePostingWithoutTf);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMergeOnePostingWithoutPosition);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMergeOnePostingWithTfBitmap);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMergeOnePostingWithTfBitmapWithoutPayload);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMergeOnePostingWithTfBitmapWithoutPosition);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestSimpleMerge);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestSimpleMergeWithoutTf);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestSimpleMergeWithoutPosition);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestSimpleMergeWithTfBitmap);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestSimpleMergeWithTfBitmapWithoutPayload);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestSimpleMergeWithTfBitmapWithoutPosition);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMultiMergeWithoutPosList);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMultiMergeWithPosList);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMultiMergeWithoutPayloads);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMultiMergeWithoutTf);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMultiMergeWithTfBitmap);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMultiMergeWithTfBitmapWithoutPayload);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestMultiMergeWithTfBitmapWithoutPosition);
INDEXLIB_UNIT_TEST_CASE(ExpackPostingMergerTest, TestSpecificMerge);

IE_NAMESPACE_END(index);
