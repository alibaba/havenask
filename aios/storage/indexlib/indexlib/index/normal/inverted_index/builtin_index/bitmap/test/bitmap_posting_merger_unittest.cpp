#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingDumper.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/OnDiskBitmapIndexIterator.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/merger_util/reclaim_map/reclaim_map_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/bitmap_posting_maker.h"
#include "indexlib/index/test/fake_reclaim_map.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/MMapAllocator.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace index { namespace legacy {

#define INDEX_NAME_TAG "bitmap"

class BitmapPostingMergerTest : public INDEXLIB_TESTBASE
{
public:
    typedef BitmapPostingMaker::PostingList PostingList;
    typedef vector<PostingList> PostingListVec;

    DECLARE_CLASS_NAME(BitmapPostingMergerTest);
    void CaseSetUp() override
    {
        mDir = GET_TEMP_DATA_PATH();

        mByteSlicePool.reset();

        Pool* byteSlicePool = new Pool(DEFAULT_CHUNK_SIZE);
        mByteSlicePool.reset(byteSlicePool);

        PostingFormatOption tmpFormatOption;
        mPostingFormatOption = tmpFormatOption.GetBitmapPostingFormatOption();
    }

    void CaseTearDown() override {}

    void TestCaseForMergeOnePosting()
    {
        vector<uint32_t> docNums;
        docNums.push_back(88);
        InternalTestMerge(docNums);
        InternalTestMerge(docNums, true);
    }

    void TestCaseForMergeMultiPosting()
    {
        vector<uint32_t> docNums;
        docNums.push_back(88);
        docNums.push_back(180);
        docNums.push_back(50);
        docNums.push_back(600);
        docNums.push_back(22);

        InternalTestMerge(docNums);
        InternalTestMerge(docNums, true);
    }

private:
    void InternalTestMerge(const vector<uint32_t>& docNums, bool isSortByWeight = false)
    {
        tearDown();
        setUp();
        FOR_EACH(i, IndexTestUtil::deleteFuncs)
        {
            CaseSetUp();

            index_base::SegmentInfos segInfos;
            PostingListVec postLists;
            MakeData(docNums, postLists, segInfos);

            SegmentMergeInfos segMergeInfos;
            SegmentTermInfos segTermInfos;
            CreateSegmentTermInfos(segInfos, postLists, segTermInfos, segMergeInfos);

            BitmapPostingMergerPtr merger = CreateMerger();

            DeletionMapReaderPtr deletionMap = CreateDeletionMap(segInfos);
            ReclaimMapPtr reclaimMap = CreateReclaimMap(deletionMap, segMergeInfos, isSortByWeight);
            if (isSortByWeight) {
                merger->SortByWeightMerge(segTermInfos, reclaimMap);
            } else {
                merger->Merge(segTermInfos, reclaimMap);
            }

            stringstream ss;
            ss << "merged.post." << i;
            const file_system::DirectoryPtr directory = test::DirectoryCreator::Create(mDir);
            DumpMergerData(merger, directory, ss.str());

            docid_t maxDocId = postLists.back().first + segInfos.back().docCount;
            vector<docid_t> answer;
            CreateAnswer(postLists, reclaimMap, answer);
            CheckPosting(mDir + ss.str(), maxDocId, answer);

            for (uint32_t j = 0; j < segTermInfos.size(); j++) {
                delete segTermInfos[j];
            }
        }
    }

    void MakeData(const vector<uint32_t>& docNums, PostingListVec& postingLists, index_base::SegmentInfos& segInfos)
    {
        docid_t baseDocId = 0;
        postingLists.resize(docNums.size());
        BitmapPostingMaker maker(GET_PARTITION_DIRECTORY(), INDEX_NAME_TAG, /*enableNullTerm=*/false);

        for (uint32_t i = 0; i < docNums.size(); ++i) {
            PostingList postingList;
            maker.MakeOnePostingList(i, docNums[i], baseDocId, &postingList);
            postingLists[i] = postingList;

            index_base::SegmentInfo segInfo;
            segInfo.docCount = docNums[i];
            segInfos.push_back(segInfo);

            baseDocId += docNums[i];
        }
    }

    void CreateSegmentTermInfos(const index_base::SegmentInfos& segInfos, const PostingListVec& postLists,
                                SegmentTermInfos& segTermInfos, SegmentMergeInfos& segMergeInfos)
    {
        for (uint32_t i = 0; i < postLists.size(); i++) {
            stringstream ss;
            ss << SEGMENT_FILE_NAME_PREFIX << "_" << i << "_level_0/" << INDEX_DIR_NAME << "/" << INDEX_NAME_TAG;

            file_system::DirectoryPtr indexDirectory = GET_PARTITION_DIRECTORY()->GetDirectory(ss.str(), true);

            OnDiskBitmapIndexIterator* onDiskIter = new OnDiskBitmapIndexIterator(indexDirectory);
            onDiskIter->Init();
            std::shared_ptr<IndexIterator> indexIter(onDiskIter);

            SegmentTermInfo* segTermInfo =
                new SegmentTermInfo(it_unknown, (segmentid_t)i, postLists[i].first, indexIter, /*patchIter=*/nullptr);
            segTermInfo->Next();
            segTermInfos.push_back(segTermInfo);

            SegmentMergeInfo segMergeInfo;
            segMergeInfo.segmentId = (segmentid_t)i;
            segMergeInfo.baseDocId = postLists[i].first;
            segMergeInfo.deletedDocCount = 0;
            segMergeInfo.segmentInfo = segInfos[i];

            segMergeInfos.push_back(segMergeInfo);
        }
    }

    BitmapPostingMergerPtr CreateMerger()
    {
        index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
        index_base::OutputSegmentMergeInfo outputSegMergeInfo;
        outputSegMergeInfo.path = mDir;
        outputSegMergeInfo.directory = test::DirectoryCreator::Create(mDir);
        outputSegMergeInfo.targetSegmentId = 100;
        outputSegmentMergeInfos.push_back(outputSegMergeInfo);
        BitmapPostingMergerPtr merger;
        merger.reset(new BitmapPostingMerger(mByteSlicePool.get(), outputSegmentMergeInfos, OPTION_FLAG_ALL));
        return merger;
    }

    // BitmapPostingDumperPtr CreateDumper()
    // {
    //     BitmapPostingDumperPtr dumper;
    //     dumper.reset(new BitmapPostingDumper(mByteSlicePool.get()));
    //     return dumper;
    // }

    DeletionMapReaderPtr CreateDeletionMap(const index_base::SegmentInfos& segInfos)
    {
        vector<uint32_t> docNums;
        for (uint32_t i = 0; i < segInfos.size(); i++) {
            docNums.push_back((uint32_t)segInfos[i].docCount);
        }

        DeletionMapReaderPtr deletionMap = IndexTestUtil::CreateDeletionMapReader(docNums);

        for (uint32_t j = 0; j < segInfos.size(); ++j) {
            IndexTestUtil::CreateDeletionMap((segmentid_t)j, segInfos[j].docCount, IndexTestUtil::deleteFuncs[j],
                                             deletionMap);
        }
        return deletionMap;
    }

    ReclaimMapPtr CreateReclaimMap(const DeletionMapReaderPtr& deletionMap, const SegmentMergeInfos& segMergeInfos,
                                   bool isSortByWeight)
    {
        return ReclaimMapCreator::Create(false, segMergeInfos, deletionMap);
    }

    void DumpMergerData(const BitmapPostingMergerPtr& merger, const file_system::DirectoryPtr& directory,
                        const string& fileName)
    {
        IndexOutputSegmentResources outputSegmentResources;
        IndexOutputSegmentResourcePtr resource(new IndexOutputSegmentResource);
        util::SimplePool simplePool;

        resource->_bitmapIndexDataWriter.reset(new IndexDataWriter());
        resource->_bitmapIndexDataWriter->postingWriter = directory->CreateFileWriter(fileName);
        resource->_bitmapIndexDataWriter->dictWriter.reset(new TieredDictionaryWriter<dictkey_t>(&simplePool));
        resource->_bitmapIndexDataWriter->dictWriter->Open(directory, fileName + "_dict");
        outputSegmentResources.push_back(resource);
        merger->Dump(index::DictKeyInfo(0), outputSegmentResources);
        resource->DestroyIndexDataWriters();
    }

    void CreateAnswer(const PostingListVec& postLists, const ReclaimMapPtr& reclaimMap, vector<docid_t>& answer)
    {
        for (size_t i = 0; i < postLists.size(); ++i) {
            const vector<docid_t>& postList = postLists[i].second;
            for (size_t j = 0; j < postList.size(); ++j) {
                docid_t newId = reclaimMap->GetNewId(postList[j]);
                if (newId != INVALID_DOCID) {
                    answer.push_back(newId);
                }
            }
        }
        sort(answer.begin(), answer.end());
    }

    void CheckPosting(const string& filePath, docid_t maxDocId, const vector<docid_t>& answer)
    {
        file_system::MemFileNodePtr fileReader(file_system::MemFileNodeCreator::TEST_Create());
        ASSERT_EQ(file_system::FSEC_OK, fileReader->Open("", filePath, file_system::FSOT_MEM, -1));
        ASSERT_EQ(file_system::FSEC_OK, fileReader->Populate());
        uint32_t postingLen = 0;
        ASSERT_EQ(file_system::FSEC_OK,
                  fileReader->Read((void*)(&postingLen), sizeof(uint32_t), 0, file_system::ReadOption()).Code());

        ByteSliceList* sliceList =
            fileReader->ReadToByteSliceList(postingLen, sizeof(uint32_t), file_system::ReadOption());
        assert(sliceList);

        SegmentPosting segPosting(mPostingFormatOption);
        segPosting.Init(0, ByteSliceListPtr(sliceList), 0, 0);
        shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
        segPostings->push_back(segPosting);

        std::shared_ptr<BitmapPostingIterator> it(new BitmapPostingIterator);
        it->Init(segPostings, 1000);

        docid_t docIdToSeek = 0;
        for (size_t i = 0; i < answer.size(); ++i) {
            docid_t seekedId = it->SeekDoc(docIdToSeek);
            INDEXLIB_TEST_EQUAL(answer[i], seekedId);
            docIdToSeek = answer[i] + 1;
        }

        docid_t seekedId = it->SeekDoc(docIdToSeek);
        assert(INVALID_DOCID == seekedId);
        INDEXLIB_TEST_EQUAL(INVALID_DOCID, seekedId);
        sliceList->Clear(NULL);
    }

protected:
    string mDir;
    static const size_t DEFAULT_CHUNK_SIZE = 100 * 1024;
    SimpleAllocatorPtr mAllocator;
    std::shared_ptr<Pool> mByteSlicePool;
    PostingFormatOption mPostingFormatOption;
};

INDEXLIB_UNIT_TEST_CASE(BitmapPostingMergerTest, TestCaseForMergeOnePosting);
INDEXLIB_UNIT_TEST_CASE(BitmapPostingMergerTest, TestCaseForMergeMultiPosting);
}}} // namespace indexlib::index::legacy
