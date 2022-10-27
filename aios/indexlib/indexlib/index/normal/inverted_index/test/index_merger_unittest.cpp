#include "indexlib/index/normal/inverted_index/test/index_merger_unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_data_writer.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/merger/segment_directory.h"

using namespace std;
using namespace autil::mem_pool;

using testing::Return;
using testing::_;
using testing::SetArgReferee;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexMergerTest);

IndexMergerTest::IndexMergerTest()
{
    mByteSlicePool.reset(new autil::mem_pool::Pool(&mAllocator,
                    DEFAULT_CHUNK_SIZE * 1024 * 1024)); 
    mBufferPool.reset(new RecyclePool(
                    &mAllocator, DEFAULT_CHUNK_SIZE * 1024 * 1024, 8));
    mPostingWriterResource.reset(new PostingWriterResource(
            &mSimplePool, mByteSlicePool.get(), mBufferPool.get(),
            PostingFormatOption()));
}

IndexMergerTest::~IndexMergerTest()
{
    mPostingWriterResource.reset();
    mByteSlicePool.reset();
    mBufferPool.reset();
}

void IndexMergerTest::CaseSetUp()
{
}

void IndexMergerTest::CaseTearDown()
{
}

//TODO :add in mutiSegmentPostingWriter
// void IndexMergerTest::TestGetDictValue()
// {
//     IndexMergerMock indexMerger;
//     config::IndexConfigPtr indexConfig(new PackageIndexConfig("test", it_pack));
//     indexMerger.Init(
//         indexConfig, index_base::MergeItemHint(), {}, config::MergeIOConfig());
//     {
//         // mode = NORMAL, not dict inline compress
//         PostingMergerMock* postingMergerMock = new PostingMergerMock(mPostingWriterResource.get());
// //TODO
        
//         // PostingDumperImplPtr postingDumperImpl(new PostingDumperImpl(mPostingWriterResource.get()));
//         // postingMerger->AddPostingDumper(postingDumperImpl);
//         // indexMerger.GetIndexOutputSegmentResources()[0].SetPostingDumper(postingDumperImpl);

//         // EXPECT_CALL(*mockPostingMergerPtr, GetDictInlinePostingValue(_))
//         //     .WillOnce(Return(false));

//           EXPECT_CALL(*postingMergerMock, GetDocFreq())
//             .WillOnce(Return(0));
//           EXPECT_CALL(*postingMergerMock, GetTotalTF())
//             .WillOnce(Return(0));
//           dictvalue_t dictValue = postingMergerMock->mPostingWriter->GetDictValue(345);

//           uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(1, 1, PFOR_DELTA_COMPRESS_MODE);
//           dictvalue_t expectDictValue = ShortListOptimizeUtil::CreateDictValue(compressMode, 345);
//           ASSERT_EQ(expectDictValue, dictValue);
//           delete postingMergerMock;
//     }

//     {
//         uint64_t compressValue = 234;

//         // mode = NORMAL, dict inline compress
//         PostingMergerMock* postingMergerMock = new PostingMergerMock(mPostingWriterResource.get());
//         // EXPECT_CALL(*postingDumperMock, GetDictInlinePostingValue(_))
//         //     .WillOnce(DoAll(SetArgReferee<0>(compressValue), Return(true)));

//         EXPECT_CALL(*postingMergerMock, GetDocFreq())
//             .Times(0);
//         EXPECT_CALL(*postingMergerMock, GetTotalTF())
//             .Times(0);
//         dictvalue_t dictValue = postingMergerMock->mPostingWriter->GetDictValue(345);        

//         dictvalue_t expectDictValue = 
//             ShortListOptimizeUtil::CreateDictInlineValue(compressValue);

//         ASSERT_EQ(expectDictValue, dictValue);
//         delete postingMergerMock;
//     }
// }

//TODO:
// void IndexMergerTest::TestDumpPosting()
// {
//     IndexMergerMock indexMerger;
//     indexMerger.Init(
//         config::IndexConfigPtr(), index_base::MergeItemHint(), {}, config::MergeIOConfig());

//     {
//         // normal posting
//         IndexDataWriterPtr indexDataWriter(new IndexDataWriter());
//         DictionaryWriterMock *dictWriter = new DictionaryWriterMock;
//         indexDataWriter->dictWriter.reset(dictWriter);
//         EXPECT_CALL(*dictWriter, AddItem(_,_));

//         PostingMergerMock* mockPostingMergerPtr = 
//             new PostingMergerMock(mPostingWriterResource.get());
//         PostingMergerPtr postingMerger(mockPostingMergerPtr);

//         PostingDumperMock* mockPostingDumper = 
//             new PostingDumperMock(mPostingWriterResource.get());
        
//         uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(1, 1, PFOR_DELTA_COMPRESS_MODE);
//         dictvalue_t dictValue = ShortListOptimizeUtil::CreateDictValue(compressMode, 345);
//         indexMerger.DumpPosting(0, dictValue, postingMerger->GetPostingDumpers()[0], indexDataWriter);
//     }

//     {
//         // normal posting

//         IndexDataWriterPtr indexDataWriter(new IndexDataWriter());
//         DictionaryWriterMock *dictWriter = new DictionaryWriterMock;
//         indexDataWriter->dictWriter.reset(dictWriter);
//         EXPECT_CALL(*dictWriter, AddItem(_,_));

//         PostingMergerMock* mockPostingMergerPtr = 
//             new PostingMergerMock(mPostingWriterResource.get());
//         PostingMergerPtr postingMerger(mockPostingMergerPtr);


//         PostingDumperMock* mockPostingDumper = 
//             new PostingDumperMock(mPostingWriterResource.get());
//         PostingDumperPtr postingDumperPtr(mockPostingDumper);
//         postingMerger->AddPostingDumper(postingDumperPtr);
//         indexMerger.GetIndexOutputSegmentResources()[0].SetPostingDumper(postingDumperPtr);

//         EXPECT_CALL(*mockPostingDumper, Dump(_)).Times(0);
//         dictvalue_t dictValue = ShortListOptimizeUtil::CreateDictInlineValue(345);
//         indexMerger.DumpPosting(0, dictValue, postingMerger->GetPostingDumpers()[0], indexDataWriter);
//     }
// }

void IndexMergerTest::TestGetMaxLengthOfPostingWithDictInline()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "string", SFP_INDEX);
    provider.Build("1,2,3", SFP_OFFLINE);
    IndexConfigPtr indexConfig = provider.GetIndexConfig();
    IndexMergerPtr indexMerger( 
            IndexMergerFactory::GetInstance()->CreateIndexMerger(
                    indexConfig->GetIndexType(),
                    plugin::PluginManagerPtr()));
    indexMerger->Init(indexConfig);
    SegmentInfo segmentInfo;
    segmentInfo.docCount = 3;
    SegmentMergeInfos mergeInfos;
    mergeInfos.push_back(SegmentMergeInfo(0, segmentInfo, 0, 0));
    Version version;
    VersionLoader::GetVersion(GET_TEST_DATA_PATH(), version, INVALID_VERSION);
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_TEST_DATA_PATH(), version));
    segDir->Init(false);
    ASSERT_EQ((int64_t)0, indexMerger->GetMaxLengthOfPosting(segDir, mergeInfos));
}

void IndexMergerTest::TestMergeTermPoolReset()
{
    IndexMergerMock indexMerger;

    IndexConfigPtr indexConfig(
            new SingleFieldIndexConfig("test", it_string));
    indexMerger.Init(indexConfig);
    SegmentTermInfos termInfos;
    SegmentTermInfo::TermIndexMode mode = SegmentTermInfo::TM_NORMAL;
    IndexTermExtenderPtr indexTermExetender;

    Pool* byteSlicePool = indexMerger.mByteSlicePool;
    index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
    index::MergerResource resource;
    indexMerger.MergeTerm(0, termInfos, mode, resource, outputSegmentMergeInfos);
    size_t memAfter = byteSlicePool->getUsedBytes();
    ASSERT_EQ(0u, memAfter);
}

void IndexMergerTest::TestGetMergedDir()
{
    IndexMergerMock indexMerger;
    MergeItemHint hint(1, 1.0, 1, 2);
    MergeTaskResourceVector resources;

    IndexConfigPtr indexConfig(
            new SingleFieldIndexConfig("test", it_string));
    indexMerger.Init(indexConfig, hint, resources, config::MergeIOConfig());

    string instPath = indexMerger.GetMergedDir(GET_TEST_DATA_PATH());
    string expectPath = GET_TEST_DATA_PATH() + "test/inst_2_1";
    ASSERT_EQ(expectPath, instPath);
}

IE_NAMESPACE_END(index);

