#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/common_define.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "indexlib/index/merger_util/reclaim_map/reclaim_map_creator.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger_impl.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/on_disk_pack_index_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/test/pack_position_maker.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/test/pack_posting_maker.h"
#include "indexlib/index/test/fake_reclaim_map.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/section_data_maker.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil::mem_pool;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
using indexlib::index::legacy::PostingMaker;
#define INDEX_REL_PATH "index/phrase"

// TODO : move to PostingMergerImplTest
class PackPostingMergerTest : public INDEXLIB_TESTBASE
{
public:
    typedef PostingMaker::DocMap DocMap;
    typedef PostingMaker::PosValue PosValue;
    typedef PostingMaker::DocKey DocKey;
    typedef PostingMaker::DocRecordVector DocRecordVector;
    typedef PostingMaker::DocRecord DocRecord;
    typedef PostingMaker::PostingList PostingList;
    typedef vector<PostingList> PostingListVec;

    DECLARE_CLASS_NAME(PackPostingMergerTest);
    void CaseSetUp() override
    {
        mDir = GET_TEMP_DATA_PATH();
        mMaxDocId = 0;
        mMaxPos = 50;

        InitPool();
        mDeletionMap.reset(new DeletionMapReader());
        mReclaimMap.reset(new ReclaimMap());
        mIndexConfig.reset(new PackageIndexConfig("phrase", it_pack));
    }

    void CaseTearDown() override {}

    void TestMergeOnePosting() { InnerTestOnePosting(OPTION_FLAG_ALL); }

    void TestMergeOnePostingWithoutTf() { InnerTestOnePosting(NO_TERM_FREQUENCY); }

    void TestMergeOnePostingWithoutPosition() { InnerTestOnePosting(NO_POSITION_LIST); }

    void TestMergeOnePostingWithTfBitmapWithoutPayload() { InnerTestOnePosting(NO_PAYLOAD | of_tf_bitmap); }

    void TestMergeOnePostingWithTfBitmapWithoutPosition() { InnerTestOnePosting(NO_POSITION_LIST | of_tf_bitmap); }

    void TestMergeOnePostingWithTfBitmap() { InnerTestOnePosting(OPTION_FLAG_ALL | of_tf_bitmap); }

    void TestSimpleMerge() { InnerTestSimpleMerge(OPTION_FLAG_ALL); }

    void TestSimpleMergeWithoutTf() { InnerTestSimpleMerge(NO_TERM_FREQUENCY); }

    void TestSimpleMergeWithoutPosition() { InnerTestSimpleMerge(NO_POSITION_LIST); }

    void TestSimpleMergeWithTfBitmapWithoutPayload() { InnerTestSimpleMerge(NO_PAYLOAD | of_tf_bitmap); }

    void TestSimpleMergeWithTfBitmapWithoutPosition() { InnerTestSimpleMerge(NO_POSITION_LIST | of_tf_bitmap); }

    void TestSimpleMergeWithTfBitmap() { InnerTestSimpleMerge(OPTION_FLAG_ALL | of_tf_bitmap); }

    void TestMultiMerge(optionflag_t optionFlag)
    {
        vector<uint32_t> docNums;
        for (uint32_t i = 0; i < 50; i++) {
            docNums.push_back(i % 30 + 2);
        }

        InternalTestMerge(docNums, optionFlag, false, false);
        InternalTestMerge(docNums, optionFlag, true, true);
    }

    void TestMultiMergeWithoutPosList() { TestMultiMerge(NO_POSITION_LIST); }

    void TestMultiMergeWithPosList() { TestMultiMerge(OPTION_FLAG_ALL); }

    void TestMultiMergeWithoutPayloads() { TestMultiMerge(NO_PAYLOAD); }

    void TestMultiMergeWithoutTf() { TestMultiMerge(NO_TERM_FREQUENCY); }

    void TestMultiMergeWithTfBitmap() { TestMultiMerge(OPTION_FLAG_ALL | of_tf_bitmap); }

    void TestMultiMergeWithTfBitmapWithoutPayload() { TestMultiMerge(NO_PAYLOAD | of_tf_bitmap); }

    void TestMultiMergeWithTfBitmapWithoutPosition() { TestMultiMerge(NO_POSITION_LIST | of_tf_bitmap); }

    void TestSpecificMerge()
    {
        uint32_t sampleNumbers[] = {
            1,
            MAX_DOC_PER_RECORD / 3 + 1,
            MAX_DOC_PER_RECORD + 1,
            MAX_DOC_PER_RECORD * 2 + 1,
            MAX_DOC_PER_RECORD * 2 + MAX_DOC_PER_RECORD / 3 + 1,
        };

        vector<uint32_t> docNums(sampleNumbers, sampleNumbers + sizeof(sampleNumbers) / sizeof(uint32_t));
        InternalTestMerge(docNums, OPTION_FLAG_ALL, false, true);
        InternalTestMerge(docNums, OPTION_FLAG_ALL, true, false);
    }

private:
    void InnerTestOnePosting(optionflag_t optionFlag)
    {
        vector<uint32_t> docNums;
        docNums.push_back(888);
        InternalTestMerge(docNums, optionFlag, false, false);
        InternalTestMerge(docNums, optionFlag, true, true);
    }

    void InnerTestSimpleMerge(optionflag_t optionFlag)
    {
        vector<uint32_t> docNums;
        docNums.push_back(20);
        docNums.push_back(40);
        InternalTestMerge(docNums, optionFlag, false, true);
        InternalTestMerge(docNums, optionFlag, true, false);
    }

    void InternalTestMerge(const vector<uint32_t>& docNums, optionflag_t optionFlag, bool isSortByWeight,
                           bool isNullTerm)
    {
        index::DictKeyInfo key = index::DictKeyInfo(0);
        if (isNullTerm) {
            key = index::DictKeyInfo::NULL_TERM;
        }

        // make data
        FOR_EACH(i, IndexTestUtil::deleteFuncs)
        {
            tearDown();
            setUp();

            mIndexConfig->SetOptionFlag(optionFlag);
            IndexFormatOption indexFormatOption;
            indexFormatOption.Init(mIndexConfig);

            PostingListVec postLists;
            SegmentInfos segInfos;
            MakeData(optionFlag, docNums, postLists, segInfos, isNullTerm);

            SegmentMergeInfos segMergeInfos;
            SegmentTermInfos segTermInfos;
            CreateSegmentTermInfos(segInfos, postLists, segTermInfos, segMergeInfos, optionFlag);

            std::shared_ptr<legacy::PostingMergerImpl> merger = CreateMerger(optionFlag);

            // DeletionMapper before merge
            mDeletionMap = CreateDeletionMap(segInfos, IndexTestUtil::deleteFuncs[i]);

            mReclaimMap = ReclaimMapCreator::Create(false, segMergeInfos, mDeletionMap);
            if (isSortByWeight) {
                merger->SortByWeightMerge(segTermInfos, mReclaimMap);
            } else {
                merger->Merge(segTermInfos, mReclaimMap);
            }

            DirectoryPtr directory = GET_PARTITION_DIRECTORY();
            stringstream ss;
            ss << "merged.post." << i;
            string mergedName = ss.str();

            IndexOutputSegmentResources outputSegmentResources;
            IndexOutputSegmentResourcePtr resource(new IndexOutputSegmentResource);
            util::SimplePool simplePool;
            FileWriterPtr mergedSegFile = directory->CreateFileWriter(mergedName);
            resource->_normalIndexDataWriter.reset(new IndexDataWriter());
            resource->_normalIndexDataWriter->postingWriter = mergedSegFile;
            resource->_normalIndexDataWriter->dictWriter.reset(new TieredDictionaryWriter<dictkey_t>(&simplePool));
            resource->_normalIndexDataWriter->dictWriter->Open(directory, mergedName + "_dict");
            outputSegmentResources.push_back(resource);
            merger->Dump(key, outputSegmentResources);
            resource->DestroyIndexDataWriters();

            // make answer
            PostingList answerList;
            if (isSortByWeight) {
                answerList = PostingMaker::SortByWeightMergePostingLists(postLists, *mReclaimMap);
            } else {
                PostingList tmpAnswerList = PostingMaker::MergePostingLists(postLists);
                tmpAnswerList.first = 0;
                PostingMaker::CreateReclaimedPostingList(answerList, tmpAnswerList, *mReclaimMap);
            }
            uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(merger->GetDocFreq(), merger->GetTotalTF(),
                                                                          PFOR_DELTA_COMPRESS_MODE);
            CheckPosting(mDir + mergedName, answerList, compressMode, optionFlag);

            for (uint32_t j = 0; j < segTermInfos.size(); j++) {
                delete segTermInfos[j];
            }
        }
    }

    void MakeData(optionflag_t optionFlag, const vector<uint32_t>& docNums, PostingListVec& postingLists,
                  SegmentInfos& segInfos, bool isNullTerm)
    {
        docid_t maxDocId = 0;
        postingLists.resize(docNums.size());
        for (uint32_t i = 0; i < docNums.size(); i++) {
            stringstream ss;
            ss << "segment_" << i << "_level_0/" << INDEX_REL_PATH;
            string path = ss.str();
            auto segDir = GET_PARTITION_DIRECTORY()->MakeDirectory(path);
            ASSERT_NE(segDir, nullptr);

            MakeOneSegment(segDir, optionFlag, docNums[i], postingLists[i], maxDocId, isNullTerm);

            postingLists[i].first = mMaxDocId;
            mMaxDocId += (maxDocId + 1);

            SegmentInfo segInfo;
            segInfo.docCount = (maxDocId + 1);
            segInfos.push_back(segInfo);
        }
        mSecAttrReader = CreateFakeSectionAttribute();
    }

    void CreateSegmentTermInfos(const SegmentInfos& segInfos, const PostingListVec& postLists,
                                SegmentTermInfos& segTermInfos, SegmentMergeInfos& segMergeInfos,
                                optionflag_t optionFlag)
    {
        for (uint32_t i = 0; i < postLists.size(); i++) {
            stringstream ss;
            ss << "segment_" << i << "_level_0/" << INDEX_REL_PATH;

            DirectoryPtr indexDirectory = GET_PARTITION_DIRECTORY()->GetDirectory(ss.str(), true);
            legacy::OnDiskPackIndexIterator* onDiskIter =
                new legacy::OnDiskPackIndexIterator(mIndexConfig, indexDirectory, optionFlag);
            onDiskIter->Init();

            std::shared_ptr<IndexIterator> indexIter(onDiskIter);

            SegmentTermInfo* segTermInfo =
                new SegmentTermInfo(it_unknown, (segmentid_t)i, postLists[i].first, indexIter, /*patchIter=*/nullptr);
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

    uint32_t ReadVUInt32(const MemFileNodePtr& fileNode, size_t& offset)
    {
        uint8_t byte;
        fileNode->Read((void*)&byte, sizeof(byte), offset++, ReadOption()).GetOrThrow();
        uint32_t value = byte & 0x7F;
        int shift = 7;

        while (byte & 0x80) {
            fileNode->Read((void*)&byte, sizeof(byte), offset++, ReadOption()).GetOrThrow();
            value |= ((byte & 0x7F) << shift);
            shift += 7;
        }
        return value;
    }

    void CheckPosting(const string& mergedPath, const PostingList& answerList, uint8_t compressMode,
                      optionflag_t optionFlag)
    {
        MemFileNodePtr fileNode(MemFileNodeCreator::TEST_Create());
        ASSERT_EQ(file_system::FSEC_OK, fileNode->Open("", mergedPath, FSOT_MEM, -1));
        ASSERT_EQ(file_system::FSEC_OK, fileNode->Populate());
        if (answerList.second.size() == 0) {
            INDEXLIB_TEST_EQUAL(0, fileNode->GetLength());
            return;
        }

        PostingFormatOption postingFormatOption(optionFlag);
        uint32_t postingLen = 0;
        ByteSliceList* sliceList = NULL;
        if (postingFormatOption.IsCompressedPostingHeader()) {
            size_t offset = 0;
            postingLen = ReadVUInt32(fileNode, offset);
            sliceList = fileNode->ReadToByteSliceList(postingLen, offset, ReadOption());
        } else {
            fileNode->Read((void*)(&postingLen), sizeof(uint32_t), 0, ReadOption()).GetOrThrow();
            sliceList = fileNode->ReadToByteSliceList(postingLen, sizeof(uint32_t), ReadOption());
        }
        ASSERT_TRUE(sliceList);

        SegmentPosting segPosting(postingFormatOption);
        segPosting.Init(compressMode, ByteSliceListPtr(sliceList), 0, 0);

        shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
        segPostings->push_back(segPosting);

        std::shared_ptr<BufferedPostingIterator> iter(new BufferedPostingIterator(postingFormatOption, NULL, nullptr));

        iter->Init(segPostings, mSecAttrReader.get(), 1000);

        // begin check
        DocRecordVector docRecordVec = answerList.second;
        uint32_t recordCursor = 0;
        if (docRecordVec.size() == 0) {
            return;
        }

        DocRecord currDocRecord = docRecordVec[0];

        docid_t docId = 0;
        docpayload_t docPayload = 0;
        DocMap::const_iterator inRecordCursor = currDocRecord.begin();

        while ((docId = iter->SeekDoc(docId)) != INVALID_DOCID) {
            if (inRecordCursor == currDocRecord.end()) {
                recordCursor++;
                currDocRecord = docRecordVec[recordCursor];
                inRecordCursor = currDocRecord.begin();
            }
            DocKey docKey = inRecordCursor->first;

            INDEXLIB_TEST_EQUAL(docKey.first, docId);

            if (optionFlag & of_doc_payload) {
                docPayload = iter->GetDocPayload();
                INDEXLIB_TEST_EQUAL(docKey.second, docPayload);
            }

            TermMatchData tmd;
            iter->Unpack(tmd);

            PosValue posValue = inRecordCursor->second;
            if (optionFlag & of_term_frequency) {
                INDEXLIB_TEST_EQUAL(posValue.size(), (size_t)tmd.GetTermFreq());
            } else {
                INDEXLIB_TEST_EQUAL(1, tmd.GetTermFreq());
            }

            if (optionFlag & of_position_list) {
                uint32_t posCursor = 0;
                pos_t pos = 0;
                pospayload_t posPayload = 0;
                while ((pos = iter->SeekPosition(pos)) != INVALID_POSITION) {
                    INDEXLIB_TEST_EQUAL(posValue[posCursor].first, pos);
                    if (optionFlag & of_position_payload) {
                        posPayload = iter->GetPosPayload();
                        INDEXLIB_TEST_EQUAL(posValue[posCursor].second, posPayload);
                    }
                    posCursor++;
                }
            }
            inRecordCursor++;
            tmd.FreeInDocPositionState();
        }
    }

    void InitPool()
    {
        mByteSlicePool.reset();
        mBufferPool.reset();
        mAllocator.reset();

        SimpleAllocator* allocator = new SimpleAllocator;
        mAllocator.reset(allocator);

        Pool* byteSlicePool = new Pool(allocator, DEFAULT_CHUNK_SIZE);
        mByteSlicePool.reset(byteSlicePool);

        RecyclePool* bufferPool = new RecyclePool(allocator, DEFAULT_CHUNK_SIZE);
        mBufferPool.reset(bufferPool);
    }

    std::shared_ptr<legacy::PostingMergerImpl> CreateMerger(optionflag_t flag)
    {
        index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
        index_base::OutputSegmentMergeInfo outputSegMergeInfo;
        outputSegMergeInfo.path = mDir;
        outputSegMergeInfo.directory = GET_PARTITION_DIRECTORY();
        outputSegMergeInfo.targetSegmentId = 100;
        outputSegmentMergeInfos.push_back(outputSegMergeInfo);
        std::shared_ptr<legacy::PostingMergerImpl> merger;
        PostingFormatOption formatOption(flag);
        mWriterResource.reset(
            new PostingWriterResource(&mSimplePool, mByteSlicePool.get(), mBufferPool.get(), formatOption));

        merger.reset(new legacy::PostingMergerImpl(mWriterResource.get(), outputSegmentMergeInfos));
        return merger;
    }

    shared_ptr<PostingWriterImpl> CreateWriter(optionflag_t flag)
    {
        PostingFormatOption formatOption(flag);
        mWriterResource.reset(
            new PostingWriterResource(&mSimplePool, mByteSlicePool.get(), mBufferPool.get(), formatOption));
        shared_ptr<PostingWriterImpl> writer(new PostingWriterImpl(mWriterResource.get()));
        return writer;
    }

    void MakeOneSegment(const file_system::DirectoryPtr& indexDir, optionflag_t optionFlag, uint32_t docNum,
                        PostingList& postList, docid_t& maxDocId, bool isNullTerm)
    {
        shared_ptr<PostingWriterImpl> writer = CreateWriter(optionFlag);
        uint32_t totalTF;
        postList = PostingMaker::MakePositionData(docNum, maxDocId, totalTF);
        legacy::PackPositionMaker::MakeOneSegmentWithWriter(*writer, postList, optionFlag);

        writer->EndSegment();

        TermMeta termMeta(writer->GetDF(), writer->GetTotalTF());
        if (optionFlag & of_term_payload) {
            termMeta.SetPayload(writer->GetTermPayload());
        }

        TermMetaDumper tmDumper;
        uint32_t postingLen = tmDumper.CalculateStoreSize(termMeta) + writer->GetDumpLength();
        IE_LOG(TRACE1, "Writing Posting length: [%u] ", postingLen);

        FileWriterPtr file = indexDir->CreateFileWriter(POSTING_FILE_NAME);
        file->Write((void*)(&postingLen), sizeof(uint32_t)).GetOrThrow();
        tmDumper.Dump(file, termMeta);
        writer->Dump(file);
        ASSERT_EQ(FSEC_OK, file->Close());
        writer.reset();

        SimplePool simplePool;
        TieredDictionaryWriter<dictkey_t> dictWriter(&simplePool);
        dictWriter.Open(indexDir, DICTIONARY_FILE_NAME);

        if (isNullTerm) {
            dictWriter.AddItem(index::DictKeyInfo::NULL_TERM, 0);
        } else {
            dictWriter.AddItem(index::DictKeyInfo(0), 0);
        }
        dictWriter.Close();
    }

    string CreateFakeSectionString(docid_t maxDocId, pos_t maxPos)
    {
        stringstream ss;
        for (docid_t i = 0; i <= maxDocId; i++) {
            pos_t secNum = maxPos / 0x7ff + 1;
            for (uint32_t j = 0; j < secNum; ++j) {
                ss << j << " 2047 1, ";
            }
            ss << ";";
        }
        return ss.str();
    }

    LegacySectionAttributeReaderImplPtr CreateFakeSectionAttribute()
    {
        string sectionStr = CreateFakeSectionString(mMaxDocId, mMaxPos);
        IndexPartitionSchemaPtr schema =
            AttributeTestUtil::MakeSchemaWithPackageIndexConfig(mIndexConfig->GetIndexName(), 32);

        PackageIndexConfigPtr packIndexConfig = DYNAMIC_POINTER_CAST(
            PackageIndexConfig, schema->GetIndexSchema()->GetIndexConfig(mIndexConfig->GetIndexName()));

        SectionDataMaker::BuildOneSegmentSectionData(schema, mIndexConfig->GetIndexName(), sectionStr,
                                                     GET_PARTITION_DIRECTORY(), 0);

        LegacySectionAttributeReaderImplPtr reader(new LegacySectionAttributeReaderImpl);
        PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), 1);
        reader->Open(mIndexConfig, partitionData);
        return reader;
    }

    DeletionMapReaderPtr CreateDeletionMap(const SegmentInfos& segInfos, IndexTestUtil::ToDelete toDelete)
    {
        vector<uint32_t> docCounts;
        for (uint32_t i = 0; i < segInfos.size(); i++) {
            docCounts.push_back((uint32_t)segInfos[i].docCount);
        }

        DeletionMapReaderPtr deletionMap = IndexTestUtil::CreateDeletionMapReader(docCounts);

        for (uint32_t j = 0; j < segInfos.size(); ++j) {
            IndexTestUtil::CreateDeletionMap((segmentid_t)j, segInfos[j].docCount, toDelete, deletionMap);
        }
        return deletionMap;
    }

private:
    string mDir;
    uint32_t mMaxDocId;
    pos_t mMaxPos;

    DeletionMapReaderPtr mDeletionMap;
    ReclaimMapPtr mReclaimMap;
    LegacySectionAttributeReaderImplPtr mSecAttrReader;
    PackageIndexConfigPtr mIndexConfig;

    SimpleAllocatorPtr mAllocator;
    std::shared_ptr<Pool> mByteSlicePool;
    std::shared_ptr<RecyclePool> mBufferPool;
    SimplePool mSimplePool;
    shared_ptr<PostingWriterResource> mWriterResource;
    static const size_t DEFAULT_CHUNK_SIZE = 100 * 1024;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, PackPostingMergerTest);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMergeOnePosting);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMergeOnePostingWithoutTf);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMergeOnePostingWithoutPosition);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMergeOnePostingWithTfBitmapWithoutPayload);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMergeOnePostingWithTfBitmapWithoutPosition);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMergeOnePostingWithTfBitmap);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestSimpleMerge);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestSimpleMergeWithoutTf);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestSimpleMergeWithoutPosition);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestSimpleMergeWithTfBitmapWithoutPayload);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestSimpleMergeWithTfBitmapWithoutPosition);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestSimpleMergeWithTfBitmap);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMultiMergeWithoutPosList);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMultiMergeWithPosList);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMultiMergeWithoutPayloads);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMultiMergeWithoutTf);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMultiMergeWithTfBitmap);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMultiMergeWithTfBitmapWithoutPayload);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestMultiMergeWithTfBitmapWithoutPosition);
INDEXLIB_UNIT_TEST_CASE(PackPostingMergerTest, TestSpecificMerge);
}} // namespace indexlib::index
