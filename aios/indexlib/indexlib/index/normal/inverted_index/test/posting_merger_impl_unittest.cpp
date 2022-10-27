#include "indexlib/index/normal/inverted_index/test/posting_merger_impl_unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer.h"
#include <autil/ConstString.h>
#include "indexlib/config/impl/index_config_impl.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/buffered_file_writer.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingMergerImplTest);

PostingMergerImplTest::PostingMergerImplTest()
{
    mByteSlicePool = new Pool(&mAllocator, 10240);
    mBufferPool = new RecyclePool(&mAllocator, 10240);
}

PostingMergerImplTest::~PostingMergerImplTest()
{
    delete mByteSlicePool;
    delete mBufferPool;
}

void PostingMergerImplTest::CaseSetUp()
{
    mRootPath = GET_TEST_DATA_PATH();
    mRootDirectory = DirectoryCreator::Create(mRootPath);
}

void PostingMergerImplTest::CaseTearDown()
{
}

void PostingMergerImplTest::TestDump()
{
    DirectoryPtr singleIndexDirectory = mRootDirectory->MakeDirectory("index1");

    PostingFormatOption formatOption;
    PostingWriterResource postingWriterResource(
            &mSimplePool, mByteSlicePool, mBufferPool, formatOption);


    index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
    index_base::OutputSegmentMergeInfo outputSegmentMergeInfo;
    outputSegmentMergeInfo.path = mRootPath;
    outputSegmentMergeInfo.directory = DirectoryCreator::Create(mRootPath);
    outputSegmentMergeInfos.push_back(outputSegmentMergeInfo);

    PostingMergerImpl merger(&postingWriterResource, outputSegmentMergeInfos);
    IndexOutputSegmentResources indexOutputSegmentResources;
    config::IndexConfigPtr indexConfig(new PackageIndexConfig("test", it_pack));

    IndexOutputSegmentResourcePtr indexOutputSegmentResource(new IndexOutputSegmentResource());
    indexOutputSegmentResource->Init(
            singleIndexDirectory, indexConfig, config::MergeIOConfig(), &mSimplePool, false);
    indexOutputSegmentResources.push_back(indexOutputSegmentResource);
    merger.mPostingWriter->GetSegmentPostingWriter(0)->EndDocument(1, 1);
    merger.Dump(1, indexOutputSegmentResources);
    indexOutputSegmentResources[0]->Reset();
    ASSERT_TRUE(singleIndexDirectory->IsExist("posting"));
    ASSERT_EQ(merger.mPostingWriter->GetDumpLength(
                    merger.mPostingWriter->GetSegmentPostingWriter(0),0), 
              singleIndexDirectory->GetFileLength("posting"));
}

void PostingMergerImplTest::InitFiles()
{
    string postingFile = mRootPath + "index1/posting";
    
    if (storage::FileSystemWrapper::IsExist(postingFile))
    {
        storage::FileSystemWrapper::DeleteFile(postingFile);
    }

    mPostingFileWriter.reset(new BufferedFileWriter);
    mPostingFileWriter->Open(postingFile);
}

void PostingMergerImplTest::CloseFiles()
{

    mPostingFileWriter->Close();
}

IE_NAMESPACE_END(index);

