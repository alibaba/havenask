#include "indexlib/index/normal/inverted_index/test/posting_merger_impl_unittest.h"

#include "autil/ConstString.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/test/directory_creator.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index;

namespace indexlib { namespace index { namespace legacy {
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
    mRootPath = GET_TEMP_DATA_PATH();
    mRootDirectory = test::DirectoryCreator::Create(mRootPath);
}

void PostingMergerImplTest::CaseTearDown() {}

void PostingMergerImplTest::TestDump()
{
    DirectoryPtr singleIndexDirectory = mRootDirectory->MakeDirectory("index1");

    PostingFormatOption formatOption;
    PostingWriterResource postingWriterResource(&mSimplePool, mByteSlicePool, mBufferPool, formatOption);

    index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
    index_base::OutputSegmentMergeInfo outputSegmentMergeInfo;
    outputSegmentMergeInfo.path = mRootPath;
    outputSegmentMergeInfo.directory = test::DirectoryCreator::Create(mRootPath);
    outputSegmentMergeInfos.push_back(outputSegmentMergeInfo);

    PostingMergerImpl merger(&postingWriterResource, outputSegmentMergeInfos);
    IndexOutputSegmentResources indexOutputSegmentResources;
    config::IndexConfigPtr indexConfig(new PackageIndexConfig("test", it_pack));

    IndexOutputSegmentResourcePtr indexOutputSegmentResource(new IndexOutputSegmentResource());
    indexOutputSegmentResource->Init(singleIndexDirectory, indexConfig, file_system::IOConfig(), "", &mSimplePool,
                                     false);
    indexOutputSegmentResources.push_back(indexOutputSegmentResource);
    merger.mPostingWriter->GetSegmentPostingWriter(0)->EndDocument(1, 1);
    merger.Dump(index::DictKeyInfo(1), indexOutputSegmentResources);
    indexOutputSegmentResources[0]->Reset();
    ASSERT_TRUE(singleIndexDirectory->IsExist("posting"));
    ASSERT_EQ(merger.mPostingWriter->GetDumpLength(merger.mPostingWriter->GetSegmentPostingWriter(0), 0),
              singleIndexDirectory->GetFileLength("posting"));
}

void PostingMergerImplTest::InitFiles()
{
    string postingFile = mRootPath + "index1/posting";

    if (file_system::FslibWrapper::IsExist(postingFile).GetOrThrow()) {
        file_system::FslibWrapper::DeleteFileE(postingFile, DeleteOption::NoFence(false));
    }

    mPostingFileWriter.reset(new BufferedFileWriter);
    ASSERT_EQ(file_system::FSEC_OK, mPostingFileWriter->Open(postingFile, postingFile));
}

void PostingMergerImplTest::CloseFiles() { mPostingFileWriter->Close().GetOrThrow(); }
}}} // namespace indexlib::index::legacy
