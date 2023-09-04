#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingMerger.h"

#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/OnDiskBitmapIndexIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/test/BitmapPostingMaker.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/test/FakeDocMapper.h"
#include "indexlib/index/test/IndexTestUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {
namespace {
using indexlibv2::framework::Segment;
using indexlibv2::framework::SegmentInfo;
using indexlibv2::framework::SegmentMeta;
using indexlibv2::index::DocMapper;
using indexlibv2::index::IIndexMerger;
using indexlibv2::index::IndexTestUtil;
using indexlibv2::index::SrcSegmentInfo;
} // namespace

#define INDEX_NAME_TAG "bitmap"

class BitmapPostingMergerTest : public TESTBASE
{
public:
    using PostingList = BitmapPostingMaker::PostingList;
    using PostingListVec = std::vector<PostingList>;

    void setUp() override;
    void tearDown() override {}

private:
    void caseSetUp();
    void InternalTestMerge(const std::vector<uint32_t>& docNums);
    void MakeData(const std::vector<uint32_t>& docNums, PostingListVec& postingLists,
                  std::vector<SegmentInfo>& segInfos);
    void CreateSegmentTermInfos(const std::vector<SegmentInfo>& segInfos, const PostingListVec& postLists,
                                SegmentTermInfos& segTermInfos, std::vector<IIndexMerger::SourceSegment>& srcSegments);
    std::shared_ptr<BitmapPostingMerger> CreateMerger();
    void DumpMergerData(const std::shared_ptr<BitmapPostingMerger>& merger,
                        const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName);
    void CreateAnswer(const PostingListVec& postLists, const std::shared_ptr<DocMapper>& docMapper,
                      std::vector<docid_t>& answer);
    void CheckPosting(const std::string& filePath, docid_t maxDocId, const std::vector<docid_t>& answer);
    std::set<docid_t> GenDeletedDocs(uint32_t docCount, size_t i);

private:
    static const size_t DEFAULT_CHUNK_SIZE = 100 * 1024;
    std::shared_ptr<autil::mem_pool::Pool> _byteSlicePool;
    PostingFormatOption _postingFormatOption;

    std::shared_ptr<file_system::Directory> _testDir;
    std::shared_ptr<file_system::IFileSystem> _fileSystem;

    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, BitmapPostingMergerTest);

void BitmapPostingMergerTest::caseSetUp()
{
    _byteSlicePool.reset();

    autil::mem_pool::Pool* byteSlicePool = new autil::mem_pool::Pool(DEFAULT_CHUNK_SIZE);
    _byteSlicePool.reset(byteSlicePool);

    PostingFormatOption tmpFormatOption;
    _postingFormatOption = tmpFormatOption.GetBitmapPostingFormatOption();
}

void BitmapPostingMergerTest::setUp()
{
    std::string caseDir = GET_TEMP_DATA_PATH();
    file_system::FslibWrapper::DeleteDirE(caseDir, file_system::DeleteOption::NoFence(true));
    file_system::FslibWrapper::MkDirE(caseDir);

    file_system::FileSystemOptions fileSystemOptions;
    fileSystemOptions.needFlush = true;
    std::shared_ptr<util::MemoryQuotaController> mqc(
        new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    std::shared_ptr<util::PartitionMemoryQuotaController> quotaController(
        new util::PartitionMemoryQuotaController(mqc));
    fileSystemOptions.memoryQuotaController = quotaController;
    _fileSystem = file_system::FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                                         /*rootPath=*/caseDir, fileSystemOptions,
                                                         std::shared_ptr<util::MetricProvider>(),
                                                         /*isOverride=*/false)
                      .GetOrThrow();

    file_system::DirectoryPtr directory =
        file_system::IDirectory::ToLegacyDirectory(std::make_shared<file_system::LocalDirectory>("", _fileSystem));
    _testDir = directory->MakeDirectory("test");
    _fileSystem->Sync(true).GetOrThrow();
}

void BitmapPostingMergerTest::MakeData(const std::vector<uint32_t>& docNums, PostingListVec& postingLists,
                                       std::vector<SegmentInfo>& segInfos)
{
    docid_t baseDocId = 0;
    postingLists.resize(docNums.size());
    BitmapPostingMaker maker(_testDir, INDEX_NAME_TAG, /*enableNullTerm=*/false);

    for (uint32_t i = 0; i < docNums.size(); ++i) {
        PostingList postingList;
        maker.MakeOnePostingList(i, docNums[i], baseDocId, &postingList);
        postingLists[i] = postingList;

        SegmentInfo segInfo;
        segInfo.docCount = docNums[i];
        segInfos.push_back(segInfo);

        baseDocId += docNums[i];
    }
}

void BitmapPostingMergerTest::CreateSegmentTermInfos(const std::vector<SegmentInfo>& segInfos,
                                                     const PostingListVec& postLists, SegmentTermInfos& segTermInfos,
                                                     std::vector<IIndexMerger::SourceSegment>& srcSegments)
{
    for (uint32_t i = 0; i < postLists.size(); i++) {
        std::stringstream ss;
        ss << "segment_" << i << "_level_0/index/" << INDEX_NAME_TAG;

        std::shared_ptr<file_system::Directory> indexDirectory = _testDir->GetDirectory(ss.str(), true);

        auto onDiskIter = new OnDiskBitmapIndexIterator(indexDirectory);
        onDiskIter->Init();
        std::shared_ptr<IndexIterator> indexIter(onDiskIter);

        auto segTermInfo =
            new SegmentTermInfo(it_unknown, (segmentid_t)i, postLists[i].first, indexIter, /*patchIter=*/nullptr);
        segTermInfo->Next();
        segTermInfos.push_back(segTermInfo);

        IIndexMerger::SourceSegment srcSegment;
        SegmentMeta segMeta;
        srcSegment.segment = std::make_shared<indexlibv2::framework::FakeSegment>(Segment::SegmentStatus::ST_BUILT);
        segMeta.segmentId = (segmentid_t)i;
        srcSegment.baseDocid = postLists[i].first;
        *segMeta.segmentInfo = segInfos[i];
        srcSegment.segment->TEST_SetSegmentMeta(segMeta);

        srcSegments.push_back(srcSegment);
    }
}

std::shared_ptr<BitmapPostingMerger> BitmapPostingMergerTest::CreateMerger()
{
    std::vector<std::shared_ptr<SegmentMeta>> targetSegments;
    auto segMeta = std::make_shared<SegmentMeta>();
    segMeta->segmentDir = _testDir;
    segMeta->segmentId = 100;
    targetSegments.push_back(segMeta);
    std::shared_ptr<BitmapPostingMerger> merger;
    merger.reset(new BitmapPostingMerger(_byteSlicePool.get(), targetSegments, OPTION_FLAG_ALL));
    return merger;
}

void BitmapPostingMergerTest::DumpMergerData(const std::shared_ptr<BitmapPostingMerger>& merger,
                                             const std::shared_ptr<file_system::Directory>& directory,
                                             const std::string& fileName)
{
    IndexOutputSegmentResources outputSegmentResources;
    auto resource = std::make_shared<IndexOutputSegmentResource>();
    util::SimplePool simplePool;

    resource->_bitmapIndexDataWriter.reset(new IndexDataWriter());
    resource->_bitmapIndexDataWriter->postingWriter = directory->CreateFileWriter(fileName);
    resource->_bitmapIndexDataWriter->dictWriter.reset(new TieredDictionaryWriter<dictkey_t>(&simplePool));
    resource->_bitmapIndexDataWriter->dictWriter->Open(directory, fileName + "_dict");
    outputSegmentResources.push_back(resource);
    merger->Dump(index::DictKeyInfo(0), outputSegmentResources);
    resource->DestroyIndexDataWriters();
}

std::set<docid_t> BitmapPostingMergerTest::GenDeletedDocs(uint32_t docCount, size_t i)
{
    std::set<docid_t> deletedDocs;
    auto delFunc = IndexTestUtil::deleteFuncs[i % IndexTestUtil::DM_MAX_MODE];
    for (size_t j = 0; j < docCount; ++j) {
        if (delFunc(j)) {
            deletedDocs.insert(j);
        }
    }
    return deletedDocs;
}

void BitmapPostingMergerTest::InternalTestMerge(const std::vector<uint32_t>& docNums)
{
    tearDown();
    setUp();

    for (uint32_t i = 0; i < sizeof(IndexTestUtil::deleteFuncs) / sizeof(IndexTestUtil::deleteFuncs[0]); ++i) {
        setUp();

        std::vector<SegmentInfo> segInfos;
        PostingListVec postLists;
        MakeData(docNums, postLists, segInfos);

        std::vector<IIndexMerger::SourceSegment> srcSegments;
        SegmentTermInfos segTermInfos;
        CreateSegmentTermInfos(segInfos, postLists, segTermInfos, srcSegments);

        std::shared_ptr<BitmapPostingMerger> merger = CreateMerger();

        auto docMapper =
            std::make_shared<indexlibv2::index::FakeDocMapper>("docMapper", index::DocMapper::GetDocMapperType());
        std::vector<SrcSegmentInfo> infos;
        for (size_t i = 0; i < srcSegments.size(); i++) {
            auto& segment = srcSegments[i].segment;
            std::set<docid_t> deletedDocs = GenDeletedDocs(segment->GetSegmentInfo()->docCount, i);
            infos.emplace_back(SrcSegmentInfo(segment->GetSegmentId(), segment->GetSegmentInfo()->docCount,
                                              srcSegments[i].baseDocid, deletedDocs));
        }
        docMapper->Init(infos, 100, 1, true);
        merger->Merge(segTermInfos, docMapper);

        std::stringstream ss;
        ss << "merged.post." << i;
        const std::shared_ptr<file_system::Directory> directory = _testDir;
        DumpMergerData(merger, directory, ss.str());

        docid_t maxDocId = postLists.back().first + segInfos.back().docCount;
        std::vector<docid_t> answer;
        CreateAnswer(postLists, docMapper, answer);
        CheckPosting(directory->GetPhysicalPath("") + "/" + ss.str(), maxDocId, answer);

        for (uint32_t j = 0; j < segTermInfos.size(); j++) {
            delete segTermInfos[j];
        }
    }
}

void BitmapPostingMergerTest::CheckPosting(const std::string& filePath, docid_t maxDocId,
                                           const std::vector<docid_t>& answer)
{
    std::shared_ptr<file_system::MemFileNode> fileReader(file_system::MemFileNodeCreator::TEST_Create());
    ASSERT_EQ(file_system::FSEC_OK, fileReader->Open("", filePath, file_system::FSOT_MEM, -1));
    ASSERT_EQ(file_system::FSEC_OK, fileReader->Populate());
    uint32_t postingLen = 0;
    ASSERT_EQ(file_system::FSEC_OK,
              fileReader->Read((void*)(&postingLen), sizeof(uint32_t), 0, file_system::ReadOption()).Code());

    util::ByteSliceList* sliceList =
        fileReader->ReadToByteSliceList(postingLen, sizeof(uint32_t), file_system::ReadOption());
    assert(sliceList);

    SegmentPosting segPosting(_postingFormatOption);
    segPosting.Init(0, util::ByteSliceListPtr(sliceList), 0, 0);
    std::shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    segPostings->push_back(segPosting);

    std::shared_ptr<BitmapPostingIterator> it(new BitmapPostingIterator);
    it->Init(segPostings, 1000);

    docid_t docIdToSeek = 0;
    for (size_t i = 0; i < answer.size(); ++i) {
        docid_t seekedId = it->SeekDoc(docIdToSeek);
        ASSERT_EQ(answer[i], seekedId);
        docIdToSeek = answer[i] + 1;
    }

    docid_t seekedId = it->SeekDoc(docIdToSeek);
    assert(INVALID_DOCID == seekedId);
    ASSERT_EQ(INVALID_DOCID, seekedId);
    sliceList->Clear(nullptr);
}

void BitmapPostingMergerTest::CreateAnswer(const PostingListVec& postLists, const std::shared_ptr<DocMapper>& docMapper,
                                           std::vector<docid_t>& answer)
{
    for (size_t i = 0; i < postLists.size(); ++i) {
        const std::vector<docid_t>& postList = postLists[i].second;
        for (size_t j = 0; j < postList.size(); ++j) {
            docid_t newId = docMapper->GetNewId(postList[j]);
            if (newId != INVALID_DOCID) {
                answer.push_back(newId);
            }
        }
    }
    sort(answer.begin(), answer.end());
}

TEST_F(BitmapPostingMergerTest, testCaseForMergeOnePosting)
{
    std::vector<uint32_t> docNums;
    docNums.push_back(88);
    InternalTestMerge(docNums);
}

TEST_F(BitmapPostingMergerTest, testCaseForMergeMultiPosting)
{
    std::vector<uint32_t> docNums;
    docNums.push_back(88);
    docNums.push_back(180);
    docNums.push_back(50);
    docNums.push_back(600);
    docNums.push_back(22);

    InternalTestMerge(docNums);
}

} // namespace indexlib::index
