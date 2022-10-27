#include "indexlib/index/normal/inverted_index/test/segment_posting_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SegmentPostingTest);

SegmentPostingTest::SegmentPostingTest()
{
    mByteSlicePool = new Pool(&mAllocator, 10240);
    mBufferPool = new RecyclePool(&mAllocator, 10240);
}

SegmentPostingTest::~SegmentPostingTest()
{
    delete mByteSlicePool;
    delete mBufferPool;
}

void SegmentPostingTest::SetUp()
{
    mRootDir = PathUtil::JoinPath(TEST_DATA_PATH, "segment_posting_test");
    if (storage::FileSystemWrapper::IsExist(mRootDir))
    {
        storage::FileSystemWrapper::DeleteDir(mRootDir);
    }
    storage::FileSystemWrapper::MkDir(mRootDir);
}

void SegmentPostingTest::TearDown()
{
    if (storage::FileSystemWrapper::IsExist(mRootDir))
    {
        storage::FileSystemWrapper::DeleteDir(mRootDir);
    }
}

void SegmentPostingTest::TestEqual()
{
    TermMeta termMeta(1, 2, 3);
    PostingFormatOption postingFormatOption(OPTION_FLAG_ALL);
    SegmentPosting segmentPosting(postingFormatOption);
    SegmentInfo segmentInfo;

    std::string postingFileName = PathUtil::JoinPath(mRootDir, POSTING_FILE_NAME);
    DumpTermMeta(termMeta, postingFileName);
    
    file_system::InMemFileNodePtr reader(file_system::InMemFileNodeCreator::Create());
    reader->Open(postingFileName, file_system::FSOT_IN_MEM);
    reader->Populate();

    ByteSliceList* sliceList = reader->Read(reader->GetLength(), 0);
    segmentPosting.Init(0, ByteSliceListPtr(sliceList), 0, segmentInfo);
    
    {
        SegmentPosting segmentPosting1(segmentPosting);
        ASSERT_TRUE(segmentPosting1 == segmentPosting);
    }

    {
        SegmentPosting segmentPosting1(segmentPosting);
        segmentPosting1.mSliceListPtr.reset();
        ASSERT_TRUE(!(segmentPosting1 == segmentPosting));
    }

    {
        SegmentPosting segmentPosting1(segmentPosting);
        segmentPosting1.mBaseDocId = 10;
        ASSERT_TRUE(!(segmentPosting1 == segmentPosting));
    }

    {
        SegmentPosting segmentPosting1(segmentPosting);
        segmentPosting1.mDocCount = 10;
        ASSERT_TRUE(!(segmentPosting1 == segmentPosting));
    }
    
    {
        SegmentPosting segmentPosting1(segmentPosting);
        segmentPosting1.mCompressMode = 1;
        ASSERT_TRUE(!(segmentPosting1 == segmentPosting));
    }
}

void SegmentPostingTest::TestInitWithRealtime()
{
    PostingFormatOption option;
    SegmentPosting segPosting(option);
    PostingWriterResource writerResource(
            &mSimplePool, mByteSlicePool, mBufferPool, option);
    PostingWriterImpl postingWriter(&writerResource);
    postingWriter.SetTermPayload(100);

    segPosting.Init(0, 0, &postingWriter);

    TermMeta tm = segPosting.GetCurrentTermMeta();
    TermMeta mainChainTm = segPosting.GetMainChainTermMeta();

    INDEXLIB_TEST_EQUAL((df_t)0, tm.GetDocFreq());
    INDEXLIB_TEST_EQUAL(tm, mainChainTm);
}

void SegmentPostingTest::DumpTermMeta(TermMeta& termMeta, string fileName)
{
    FileWriterPtr postingFileWriter(new BufferedFileWriter());
    postingFileWriter->Open(fileName);
    
    TermMetaDumper tmDumper;
    tmDumper.Dump(postingFileWriter, termMeta);
    postingFileWriter->Close();
}

IE_NAMESPACE_END(index);

