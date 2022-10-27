
#include "indexlib/index/normal/inverted_index/test/doc_list_encoder_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/common/numeric_compress/new_pfordelta_compressor.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"
#include "indexlib/file_system/buffered_file_writer.h"

using testing::Return;
using testing::_;
using testing::SetArgReferee;
using testing::InSequence;
using testing::Sequence;
using testing::DoAll;
using namespace std;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocListEncoderTest);

DocListEncoderTest::DocListEncoderTest()
{
    mByteSlicePool = new Pool(&mAllocator, 10240);
    mBufferPool = new RecyclePool(&mAllocator, 10240);
}

DocListEncoderTest::~DocListEncoderTest()
{
    delete mByteSlicePool;
    delete mBufferPool;
}

void DocListEncoderTest::SetUp()
{
    mRootDir = storage::FileSystemWrapper::JoinPath(TEST_DATA_PATH, "doc_list_encoder_test");
    if (storage::FileSystemWrapper::IsExist(mRootDir))
    {
        storage::FileSystemWrapper::DeleteDir(mRootDir);
    }
    storage::FileSystemWrapper::MkDir(mRootDir);
    mDocListFile = storage::FileSystemWrapper::JoinPath(mRootDir, "docListFile");
}

void DocListEncoderTest::TearDown()
{
    if (storage::FileSystemWrapper::IsExist(mRootDir))
    {
        storage::FileSystemWrapper::DeleteDir(mRootDir);
    }
}

void DocListEncoderTest::TestSimpleProcess()
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(docListFormatOption);
    DocListEncoder docListEncoder(docListFormatOption, &mSimplePool,
                                  mByteSlicePool, mBufferPool, &docListFormat);

    for (uint32_t i = 0; i <= MAX_DOC_PER_RECORD + 1; i++)
    {
        //TODO here field_map may not so large 
        docListEncoder.AddPosition(i % 8);
    }
    docListEncoder.EndDocument(1, 1);

    InMemDocListDecoder* inMemDocListDecoder = 
        docListEncoder.GetInMemDocListDecoder(mByteSlicePool);
    
    INDEXLIB_TEST_TRUE(inMemDocListDecoder);

    file_system::FileWriterPtr docListFile(new BufferedFileWriter());
    docListFile->Open(mDocListFile);
    docListEncoder.Dump(docListFile);
    docListFile->Close();
    INDEXLIB_TEST_TRUE(storage::FileSystemWrapper::IsExist(mDocListFile));
    INDEXLIB_TEST_TRUE(storage::FileSystemWrapper::GetFileLength(mDocListFile) > 0);

    IE_POOL_COMPATIBLE_DELETE_CLASS(mByteSlicePool, inMemDocListDecoder);
}

void DocListEncoderTest::TestAddPosition()
{
    {
        DocListFormatOption docListFormatOption;
        docListFormatOption.mHasFieldMap = false;

        DocListFormat docListFormat(docListFormatOption);
        DocListEncoder docListEncoder(docListFormatOption, &mSimplePool,
                mByteSlicePool, mBufferPool, &docListFormat);
        docListEncoder.AddPosition(3);
        ASSERT_EQ(docListEncoder.mCurrentTF, 1);
        ASSERT_EQ(docListEncoder.mTotalTF, 1);
        ASSERT_EQ(docListEncoder.mFieldMap, 0);
        ASSERT_EQ(docListEncoder.mDF, 0);

        docListEncoder.AddPosition(4);
        ASSERT_EQ(docListEncoder.mCurrentTF, 2);
        ASSERT_EQ(docListEncoder.mTotalTF, 2);
        ASSERT_EQ(docListEncoder.mFieldMap, 0);
        ASSERT_EQ(docListEncoder.mDF, 0);
    }
    {
        DocListFormatOption docListFormatOption;
        docListFormatOption.mHasFieldMap = true;
        DocListFormat docListFormat(docListFormatOption);
        DocListEncoder docListEncoder(docListFormatOption, &mSimplePool,
                mByteSlicePool, mBufferPool, &docListFormat);
        docListEncoder.AddPosition(3);
        ASSERT_EQ(docListEncoder.mCurrentTF, 1);
        ASSERT_EQ(docListEncoder.mTotalTF, 1);
        ASSERT_EQ(docListEncoder.mFieldMap, (1 << 3));
        ASSERT_EQ(docListEncoder.mDF, 0);

        docListEncoder.AddPosition(4);
        ASSERT_EQ(docListEncoder.mCurrentTF, 2);
        ASSERT_EQ(docListEncoder.mTotalTF, 2);
        ASSERT_EQ(docListEncoder.mFieldMap, (1 << 3) | (1 << 4));
        ASSERT_EQ(docListEncoder.mDF, 0);
    }
}

void DocListEncoderTest::TestEndDocument()
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(docListFormatOption);
    DocListEncoder docListEncoder(docListFormatOption, &mSimplePool,
                                  mByteSlicePool, mBufferPool, &docListFormat);
    docListEncoder.mDF = 5;
    docListEncoder.mTotalTF = 18;
    docListEncoder.mCurrentTF = 3;

    MockPositionBitmapWriter* mockTfBitmapWriter =
        IE_POOL_COMPATIBLE_NEW_CLASS(mByteSlicePool, MockPositionBitmapWriter);
    EXPECT_CALL(*mockTfBitmapWriter, Set(18 - 3)).Times(1);
    EXPECT_CALL(*mockTfBitmapWriter, EndDocument((5+1), 18)).Times(1);

    docListEncoder.SetPositionBitmapWriter(mockTfBitmapWriter);
    docListEncoder.EndDocument(30, 100);
    ASSERT_EQ(6, docListEncoder.mDF);
    ASSERT_EQ(30, docListEncoder.mLastDocId);
    ASSERT_EQ(0, docListEncoder.mCurrentTF);
    ASSERT_EQ(0, docListEncoder.mFieldMap);
}

void DocListEncoderTest::TestFlush()
{
    DocListFormatOption docListFormatOption;
    DocListFormat docListFormat(docListFormatOption);
    DocListEncoder docListEncoder(docListFormatOption, &mSimplePool,
                                  mByteSlicePool, mBufferPool, &docListFormat);
    docListEncoder.mDF = 5;
    docListEncoder.mTotalTF = 18;

    MockPositionBitmapWriter* mockTfBitmapWriter =
        IE_POOL_COMPATIBLE_NEW_CLASS(mByteSlicePool, MockPositionBitmapWriter);
    MockSkipListWriter* mockSkipListWriter = new MockSkipListWriter(
            mByteSlicePool, mBufferPool);
    unique_ptr<MockSkipListWriter> mockSkipListWriterPtr(mockSkipListWriter);

    EXPECT_CALL(*mockTfBitmapWriter, Resize(18)).Times(1);
    EXPECT_CALL(*mockSkipListWriter, Flush(_)).Times(1);

    docListEncoder.SetPositionBitmapWriter(mockTfBitmapWriter);
    docListEncoder.SetDocSkipListWriter(mockSkipListWriter);
    docListEncoder.Flush();
    docListEncoder.SetDocSkipListWriter(NULL);
}

void DocListEncoderTest::TestDump()
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(docListFormatOption);
    DocListEncoder docListEncoder(docListFormatOption, &mSimplePool,
                                  mByteSlicePool, mBufferPool, &docListFormat);
    docListEncoder.mDF = 5;
    docListEncoder.mTotalTF = 18;
    MockPositionBitmapWriter* mockTfBitmapWriter =
        IE_POOL_COMPATIBLE_NEW_CLASS(mByteSlicePool, MockPositionBitmapWriter);
    MockSkipListWriter* mockSkipListWriter = new MockSkipListWriter(
            mByteSlicePool, mBufferPool);
    unique_ptr<MockSkipListWriter> mockSkipListWriterPtr(mockSkipListWriter);

    Sequence s;
    EXPECT_CALL(*mockSkipListWriter, Flush(_)).Times(1);
    EXPECT_CALL(*mockTfBitmapWriter, Resize(18)).Times(1);
    EXPECT_CALL(*mockSkipListWriter, Dump(_))
        .Times(1)
        .InSequence(s);
    EXPECT_CALL(*mockTfBitmapWriter, Dump(_,_))
        .Times(1)
        .InSequence(s);

    docListEncoder.SetPositionBitmapWriter(mockTfBitmapWriter);
    docListEncoder.SetDocSkipListWriter(mockSkipListWriter);

    file_system::FileWriterPtr docListFile(new BufferedFileWriter());
    docListFile->Open(mDocListFile);
    docListEncoder.Dump(docListFile);
    docListFile->Close();
    docListEncoder.SetDocSkipListWriter(NULL);
}

void DocListEncoderTest::TestGetDumpLength()
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(docListFormatOption);
    DocListEncoder docListEncoder(docListFormatOption, &mSimplePool,
                                  mByteSlicePool, mBufferPool, &docListFormat);
    MockPositionBitmapWriter* mockTfBitmapWriter =
        IE_POOL_COMPATIBLE_NEW_CLASS(mByteSlicePool, MockPositionBitmapWriter);
    EXPECT_CALL(*mockTfBitmapWriter, GetDumpLength(_))
        .WillOnce(Return(6000));
    docListEncoder.SetPositionBitmapWriter(mockTfBitmapWriter);
    uint32_t length = docListEncoder.GetDumpLength();
    // vint32Length(0) + vint32length(0) + 0 + 0 + 6000
    ASSERT_EQ((uint32_t)6002, length);
}

void DocListEncoderTest::TestFlushDocListBuffer()
{
    // compressMode == SHORT_LIST_COMPRESS_MODE, no skip list
    InnerTestFlushDocListBuffer(SHORT_LIST_COMPRESS_MODE, false);
    InnerTestFlushDocListBuffer(PFOR_DELTA_COMPRESS_MODE, true);
}

void DocListEncoderTest::InnerTestFlushDocListBuffer(
        uint8_t compressMode, bool hasSkipListWriter)
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(docListFormatOption);
    DocListEncoder docListEncoder(docListFormatOption, &mSimplePool,
                                  mByteSlicePool, mBufferPool, &docListFormat);
    docListEncoder.AddPosition(1);
    docListEncoder.EndDocument(1, 1);
    docListEncoder.FlushDocListBuffer(compressMode);
    ASSERT_EQ(hasSkipListWriter, docListEncoder.mDocSkipListWriter != NULL);
}

IE_NAMESPACE_END(index);

