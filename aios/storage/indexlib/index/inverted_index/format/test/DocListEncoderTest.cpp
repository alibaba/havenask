#include "indexlib/index/inverted_index/format/DocListEncoder.h"

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/index/common/numeric_compress/NewPfordeltaCompressor.h"
#include "indexlib/index/inverted_index/format/PositionBitmapWriter.h"
#include "indexlib/index/inverted_index/format/skiplist/BufferedSkipListWriter.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class MockSkipListWriter : public BufferedSkipListWriter
{
public:
    MockSkipListWriter(autil::mem_pool::Pool* byteSlicePool, autil::mem_pool::RecyclePool* bufferPool)
        : BufferedSkipListWriter(byteSlicePool, bufferPool)
    {
    }

public:
    MOCK_METHOD(size_t, Flush, (uint8_t compressMode), (override));
    MOCK_METHOD(void, Dump, (const file_system::FileWriterPtr& file), (override));
};

class MockPositionBitmapWriter : public PositionBitmapWriter
{
public:
    MockPositionBitmapWriter() : PositionBitmapWriter(nullptr) {}

public:
    MOCK_METHOD(void, Set, (uint32_t index), (override));
    MOCK_METHOD(void, Resize, (uint32_t size), (override));
    MOCK_METHOD(void, EndDocument, (uint32_t df, uint32_t totalPosCount), (override));
    MOCK_METHOD(void, Dump, (const file_system::FileWriterPtr& file, uint32_t totalPosCount), (override));
    MOCK_METHOD(uint32_t, GetDumpLength, (uint32_t bitCount), (const, override));
};

class DocListEncoderTest : public TESTBASE
{
public:
    DocListEncoderTest()
    {
        _byteSlicePool = new autil::mem_pool::Pool(&_allocator, 10240);
        _bufferPool = new autil::mem_pool::RecyclePool(&_allocator, 10240);
    }
    ~DocListEncoderTest()
    {
        delete _byteSlicePool;
        delete _bufferPool;
    }

    void setUp() override
    {
        _rootDir = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "doc_list_encoder_test");
        file_system::FslibWrapper::MkDirE(_rootDir);
        _docListFile = util::PathUtil::JoinPath(_rootDir, "docListFile");
    }
    void tearDown() override {}

private:
    void InnerTestFlushDocListBuffer(uint8_t compressMode, bool hasSkipListWriter)
    {
        DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
        DocListFormat docListFormat(docListFormatOption);
        DocListEncoder docListEncoder(docListFormatOption, &_simplePool, _byteSlicePool, _bufferPool, &docListFormat);
        docListEncoder.AddPosition(1);
        docListEncoder.EndDocument(1, 1);
        docListEncoder.FlushDocListBuffer(compressMode);
        ASSERT_EQ(hasSkipListWriter, docListEncoder._docSkipListWriter != nullptr);
    }

    autil::mem_pool::Pool* _byteSlicePool;
    autil::mem_pool::RecyclePool* _bufferPool;
    util::SimplePool _simplePool;
    autil::mem_pool::SimpleAllocator _allocator;
    std::string _rootDir;
    std::string _docListFile;
};

TEST_F(DocListEncoderTest, testSimpleProcess)
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(docListFormatOption);
    DocListEncoder docListEncoder(docListFormatOption, &_simplePool, _byteSlicePool, _bufferPool, &docListFormat);

    for (uint32_t i = 0; i <= MAX_DOC_PER_RECORD + 1; i++) {
        // TODO here field_std::map may not so large
        docListEncoder.AddPosition(i % 8);
    }
    docListEncoder.EndDocument(1, 1);

    InMemDocListDecoder* inMemDocListDecoder = docListEncoder.GetInMemDocListDecoder(_byteSlicePool);

    ASSERT_TRUE(inMemDocListDecoder);

    file_system::FileWriterPtr docListFile(new file_system::BufferedFileWriter());
    ASSERT_EQ(file_system::FSEC_OK, docListFile->Open(_docListFile, _docListFile));
    docListEncoder.Dump(docListFile);
    ASSERT_EQ(file_system::FSEC_OK, docListFile->Close());
    ASSERT_TRUE(file_system::FslibWrapper::IsExist(_docListFile).GetOrThrow());
    ASSERT_TRUE(file_system::FslibWrapper::GetFileLength(_docListFile).GetOrThrow() > 0);

    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool, inMemDocListDecoder);
}

TEST_F(DocListEncoderTest, testAddPosition)
{
    {
        DocListFormatOption docListFormatOption;
        docListFormatOption._hasFieldMap = false;

        DocListFormat docListFormat(docListFormatOption);
        DocListEncoder docListEncoder(docListFormatOption, &_simplePool, _byteSlicePool, _bufferPool, &docListFormat);
        docListEncoder.AddPosition(3);
        ASSERT_EQ(docListEncoder._currentTF, 1);
        ASSERT_EQ(docListEncoder._totalTF, 1);
        ASSERT_EQ(docListEncoder._fieldMap, 0);
        ASSERT_EQ(docListEncoder._df, 0);

        docListEncoder.AddPosition(4);
        ASSERT_EQ(docListEncoder._currentTF, 2);
        ASSERT_EQ(docListEncoder._totalTF, 2);
        ASSERT_EQ(docListEncoder._fieldMap, 0);
        ASSERT_EQ(docListEncoder._df, 0);
    }
    {
        DocListFormatOption docListFormatOption;
        docListFormatOption._hasFieldMap = true;
        DocListFormat docListFormat(docListFormatOption);
        DocListEncoder docListEncoder(docListFormatOption, &_simplePool, _byteSlicePool, _bufferPool, &docListFormat);
        docListEncoder.AddPosition(3);
        ASSERT_EQ(docListEncoder._currentTF, 1);
        ASSERT_EQ(docListEncoder._totalTF, 1);
        ASSERT_EQ(docListEncoder._fieldMap, (1 << 3));
        ASSERT_EQ(docListEncoder._df, 0);

        docListEncoder.AddPosition(4);
        ASSERT_EQ(docListEncoder._currentTF, 2);
        ASSERT_EQ(docListEncoder._totalTF, 2);
        ASSERT_EQ(docListEncoder._fieldMap, (1 << 3) | (1 << 4));
        ASSERT_EQ(docListEncoder._df, 0);
    }
}

TEST_F(DocListEncoderTest, testEndDocument)
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(docListFormatOption);
    DocListEncoder docListEncoder(docListFormatOption, &_simplePool, _byteSlicePool, _bufferPool, &docListFormat);
    docListEncoder._df = 5;
    docListEncoder._totalTF = 18;
    docListEncoder._currentTF = 3;

    MockPositionBitmapWriter* mockTfBitmapWriter =
        IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool, MockPositionBitmapWriter);
    EXPECT_CALL(*mockTfBitmapWriter, Set(18 - 3)).Times(1);
    EXPECT_CALL(*mockTfBitmapWriter, EndDocument((5 + 1), 18)).Times(1);

    docListEncoder.TEST_SetPositionBitmapWriter(mockTfBitmapWriter);
    docListEncoder.EndDocument(30, 100);
    ASSERT_EQ(6, docListEncoder._df);
    ASSERT_EQ(30, docListEncoder._lastDocId);
    ASSERT_EQ(0, docListEncoder._currentTF);
    ASSERT_EQ(0, docListEncoder._fieldMap);
}

TEST_F(DocListEncoderTest, testFlush)
{
    DocListFormatOption docListFormatOption;
    DocListFormat docListFormat(docListFormatOption);
    DocListEncoder docListEncoder(docListFormatOption, &_simplePool, _byteSlicePool, _bufferPool, &docListFormat);
    docListEncoder._df = 5;
    docListEncoder._totalTF = 18;

    MockPositionBitmapWriter* mockTfBitmapWriter =
        IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool, MockPositionBitmapWriter);
    MockSkipListWriter* mockSkipListWriter = new MockSkipListWriter(_byteSlicePool, _bufferPool);
    std::unique_ptr<MockSkipListWriter> mockSkipListWriterPtr(mockSkipListWriter);

    EXPECT_CALL(*mockTfBitmapWriter, Resize(18)).Times(1);
    EXPECT_CALL(*mockSkipListWriter, Flush(_)).Times(1);

    docListEncoder.TEST_SetPositionBitmapWriter(mockTfBitmapWriter);
    docListEncoder.TEST_SetDocSkipListWriter(mockSkipListWriter);
    docListEncoder.Flush();
    docListEncoder.TEST_SetDocSkipListWriter(nullptr);
}

TEST_F(DocListEncoderTest, testDump)
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(docListFormatOption);
    DocListEncoder docListEncoder(docListFormatOption, &_simplePool, _byteSlicePool, _bufferPool, &docListFormat);
    docListEncoder._df = 5;
    docListEncoder._totalTF = 18;
    MockPositionBitmapWriter* mockTfBitmapWriter =
        IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool, MockPositionBitmapWriter);
    MockSkipListWriter* mockSkipListWriter = new MockSkipListWriter(_byteSlicePool, _bufferPool);
    std::unique_ptr<MockSkipListWriter> mockSkipListWriterPtr(mockSkipListWriter);

    testing::Sequence s;
    EXPECT_CALL(*mockSkipListWriter, Flush(_)).Times(1);
    EXPECT_CALL(*mockTfBitmapWriter, Resize(18)).Times(1);
    EXPECT_CALL(*mockSkipListWriter, Dump(_)).Times(1).InSequence(s);
    EXPECT_CALL(*mockTfBitmapWriter, Dump(_, _)).Times(1).InSequence(s);

    docListEncoder.TEST_SetPositionBitmapWriter(mockTfBitmapWriter);
    docListEncoder.TEST_SetDocSkipListWriter(mockSkipListWriter);

    file_system::FileWriterPtr docListFile(new file_system::BufferedFileWriter());
    ASSERT_EQ(file_system::FSEC_OK, docListFile->Open(_docListFile, _docListFile));
    docListEncoder.Dump(docListFile);
    ASSERT_EQ(file_system::FSEC_OK, docListFile->Close());
    docListEncoder.TEST_SetDocSkipListWriter(nullptr);
}

TEST_F(DocListEncoderTest, testGetDumpLength)
{
    DocListFormatOption docListFormatOption(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(docListFormatOption);
    DocListEncoder docListEncoder(docListFormatOption, &_simplePool, _byteSlicePool, _bufferPool, &docListFormat);
    MockPositionBitmapWriter* mockTfBitmapWriter =
        IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool, MockPositionBitmapWriter);
    EXPECT_CALL(*mockTfBitmapWriter, GetDumpLength(_)).WillOnce(testing::Return(6000));
    docListEncoder.TEST_SetPositionBitmapWriter(mockTfBitmapWriter);
    uint32_t length = docListEncoder.GetDumpLength();
    // vint32Length(0) + vint32length(0) + 0 + 0 + 6000
    ASSERT_EQ((uint32_t)6002, length);
}

TEST_F(DocListEncoderTest, testFlushDocListBuffer)
{
    // compressMode == SHORT_LIST_COMPRESS_MODE, no skip list
    InnerTestFlushDocListBuffer(SHORT_LIST_COMPRESS_MODE, false);
    InnerTestFlushDocListBuffer(PFOR_DELTA_COMPRESS_MODE, true);
}

} // namespace indexlib::index
