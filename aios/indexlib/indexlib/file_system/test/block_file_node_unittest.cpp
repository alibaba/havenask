#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/common_file_wrapper.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/test/block_file_node_unittest.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/util/cache/lru_cache.h"
#include "indexlib/util/cache/cache.h"
#include "indexlib/util/cache/block.h"

using namespace std;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BlockFileNodeTest);

BlockFileNodeTest::BlockFileNodeTest()
    : mBlockCache(new BlockCache)
{
}

BlockFileNodeTest::~BlockFileNodeTest()
{
}

void BlockFileNodeTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mFileName = mRootDir + "block_file";
    mBlockSize = 4;
}

void BlockFileNodeTest::CaseTearDown()
{
}

#ifndef CHECK_FILE_NODE_WITH_BUFFER
#define CHECK_FILE_NODE_WITH_BUFFER(expectData, offset, length)         \
{                                                                       \
    const uint32_t fileSize = 9;                                        \
    GenerateFile(mFileName, fileSize, mBlockSize);                      \
    BlockFileNodePtr blockFileNode = CreateFileNode();                  \
    blockFileNode->Open(mFileName, FSOT_CACHE);                         \
    char buffer[length];                                                \
    blockFileNode->Read(buffer, length, offset);                        \
    const string curData(buffer, length);                               \
    ASSERT_EQ(expectData, curData.substr(0, length));                   \
}
#endif

void BlockFileNodeTest::TestCaseForReadWithBuffer()
{
    CHECK_FILE_NODE_WITH_BUFFER(string(3, 'a'), 0, 3);
    CHECK_FILE_NODE_WITH_BUFFER(string(4, 'a'), 0, 4);
    CHECK_FILE_NODE_WITH_BUFFER(string(4, 'a') + string(1, 'b'), 0, 5);

    CHECK_FILE_NODE_WITH_BUFFER(string(1, 'a'), 2, 1);
    CHECK_FILE_NODE_WITH_BUFFER(string(2, 'a'), 2, 2);
    CHECK_FILE_NODE_WITH_BUFFER(string(2, 'a') + string(2, 'b'), 2, 4);
    CHECK_FILE_NODE_WITH_BUFFER(string(2, 'a') + string(4, 'b'), 2, 6);

    CHECK_FILE_NODE_WITH_BUFFER(string(1, 'a'), 3, 1);
    CHECK_FILE_NODE_WITH_BUFFER(string(1, 'a') + string(1, 'b'), 3, 2);
    CHECK_FILE_NODE_WITH_BUFFER(string(1, 'a') + string(4, 'b'), 3, 5);
    CHECK_FILE_NODE_WITH_BUFFER(string(1, 'a') + string(4, 'b') + string(1, 'c'), 3, 6);
}
#undef CHECK_FILE_NODE_WITH_BUFFER

#ifndef CHECK_FILE_NODE_WITH_DIRECT_IO
#define CHECK_FILE_NODE_WITH_DIRECT_IO(expectData, offset, length)      \
{                                                                       \
    const uint32_t fileSize = 10 * 1024 + 1;                            \
    mBlockSize = 4 * 1024;                                              \
    GenerateFile(mFileName, fileSize, mBlockSize);                      \
    BlockFileNodePtr blockFileNode = CreateFileNode(true);              \
    blockFileNode->Open(mFileName, FSOT_CACHE);                         \
    char buffer[length];                                                \
    blockFileNode->Read(buffer, length, offset);                        \
    const string curData(buffer, length);                               \
    ASSERT_EQ(expectData, curData.substr(0, length));         \
}
#endif

void BlockFileNodeTest::TestCaseForReadWithDirectIO()
{
    CHECK_FILE_NODE_WITH_DIRECT_IO(string("aaa"), 0, 3);
    CHECK_FILE_NODE_WITH_DIRECT_IO(string(4096, 'a'), 0, 4096);
    CHECK_FILE_NODE_WITH_DIRECT_IO(string(4095, 'a') + "bbb", 1, 4098);
    CHECK_FILE_NODE_WITH_DIRECT_IO(string("bbb"), 5000, 3);        
    CHECK_FILE_NODE_WITH_DIRECT_IO("a"+ string(4096, 'b') + "ccc", 4095, 4100);
}
#undef CHECK_FILE_NODE_WITH_DIRECT_IO

void BlockFileNodeTest::TestCaseForReadWithByteSliceList()
{
    auto CheckFileNodeWithByteSliceReader = [&](
            const string& expectData, uint32_t offset, uint32_t length)
    {
        const uint32_t fileSize = 9; 
        GenerateFile(mFileName, fileSize, mBlockSize); 
        BlockFileNodePtr blockFileNode = CreateFileNode(); 
        blockFileNode->Open(mFileName, FSOT_CACHE); 
        ByteSliceList* byteSliceList = blockFileNode->Read(length, offset);
        ByteSliceReader reader(byteSliceList);
        uint32_t dataLen = expectData.size();
        vector<char> dataBuffer;
        dataBuffer.resize(dataLen);
        char* data = (char*)dataBuffer.data();
        ASSERT_EQ(dataLen, reader.Read(data, dataLen));
        ASSERT_EQ(expectData, string(data, dataLen)); 
        delete byteSliceList; 
    };
    CheckFileNodeWithByteSliceReader(string(3, 'a'), 0, 3);
    CheckFileNodeWithByteSliceReader(string(3, 'a'), 0, 3);
    CheckFileNodeWithByteSliceReader(string(4, 'a'), 0, 4);
    CheckFileNodeWithByteSliceReader(string(4, 'a') + string(1, 'b'), 0, 5);

    CheckFileNodeWithByteSliceReader(string(1, 'a'), 2, 1);
    CheckFileNodeWithByteSliceReader(string(2, 'a'), 2, 2);
    CheckFileNodeWithByteSliceReader(string(2, 'a') + string(2, 'b'), 2, 4);
    CheckFileNodeWithByteSliceReader(string(2, 'a') + string(4, 'b'), 2, 6);

    CheckFileNodeWithByteSliceReader(string(1, 'a'), 3, 1);
    CheckFileNodeWithByteSliceReader(string(1, 'a') + string(1, 'b'), 3, 2);
    CheckFileNodeWithByteSliceReader(string(1, 'a') + string(4, 'b'), 3, 5);
    CheckFileNodeWithByteSliceReader(string(1, 'a') + string(4, 'b') + string(1, 'c'), 3, 6);
}

void BlockFileNodeTest::TestCaseForReadFileOutOfRangeWithBuffer()
{
    const uint32_t fileSize = 9;
    const uint32_t offset = 0;
    const uint32_t len = 10;
    GenerateFile(mFileName, fileSize, mBlockSize);
    BlockFileNodePtr blockFileNode = CreateFileNode();
    blockFileNode->Open(mFileName, FSOT_CACHE);
    char buffer[len];
    ASSERT_THROW(blockFileNode->Read(buffer, len, offset), OutOfRangeException);
}

void BlockFileNodeTest::TestCaseForReadFileOutOfRangeWithByteSliceList()
{
    const uint32_t offset = 0;
    const uint32_t len = 10;
    const uint32_t fileSize = 9;
    GenerateFile(mFileName, fileSize, mBlockSize);
    BlockFileNodePtr blockFileNode = CreateFileNode();
    blockFileNode->Open(mFileName, FSOT_CACHE);
    ByteSliceList* byteSliceList = NULL;
    ASSERT_THROW(byteSliceList = blockFileNode-> Read(len, offset),
                 OutOfRangeException);
    delete byteSliceList;
}

void BlockFileNodeTest::TestCaseForReadEmptyFileWithCache()
{
    GenerateFile(mFileName, 0, mBlockSize);
    BlockFileNodePtr blockFileNode = CreateFileNode();
    blockFileNode->Open(mFileName, FSOT_CACHE);
    const uint64_t len = 100;
    char buffer[len];
    ASSERT_THROW(blockFileNode->Read(buffer, len, 0), OutOfRangeException);
}

void BlockFileNodeTest::TestCaseForGetBlocks()
{
    GenerateFile(mFileName, 20 * mBlockSize + 2, mBlockSize);
    mBlockCache->Init(100 * mBlockSize, mBlockSize, 1);
    BlockFileNodePtr blockFileNode(new BlockFileNode(mBlockCache.get(), false));
    blockFileNode->Open(mFileName, FSOT_CACHE);
    ReadOption option;
    {
        future_lite::Future<std::vector<util::BlockHandle>> f = blockFileNode->mAccessor.GetBlocks(0, 5, option);
        auto handles = std::move(f).get();
        for (size_t i = 0; i < 5; i ++)
        {
            ASSERT_TRUE(handles[i].GetBlock() != NULL);
            const string expectData(mBlockSize, 'a' + i);
            const char* addr = reinterpret_cast<const char*>(handles[i].GetBlock()->data);
            ASSERT_EQ(expectData, string(addr, mBlockSize));
        }
    }
    {
        ASSERT_EQ(5, blockFileNode->GetBlockCache()->GetBlockCount());
        future_lite::Future<std::vector<util::BlockHandle>> f = blockFileNode->mAccessor.GetBlocks(3, 2, option);
        auto handles = std::move(f).get();
        for (size_t i = 0; i < 2; i ++)
        {
            ASSERT_TRUE(handles[i].GetBlock() != NULL);
            const string expectData(mBlockSize, 'a' + i + 3);
            const char* addr = reinterpret_cast<const char*>(handles[i].GetBlock()->data);
            ASSERT_EQ(expectData, string(addr, mBlockSize));
        }
    }
    {
        ASSERT_EQ(5, blockFileNode->GetBlockCache()->GetBlockCount());
        future_lite::Future<std::vector<util::BlockHandle>> f = blockFileNode->mAccessor.GetBlocks(3, 5, option);
        auto handles = std::move(f).get();
        for (size_t i = 0; i < 5; i ++)
        {
            ASSERT_TRUE(handles[i].GetBlock() != NULL);
            const string expectData(mBlockSize, 'a' + i + 3);
            const char* addr = reinterpret_cast<const char*>(handles[i].GetBlock()->data);
            ASSERT_EQ(expectData, string(addr, mBlockSize));
        }
        ASSERT_EQ(8, blockFileNode->GetBlockCache()->GetBlockCount());
    }
    {
        BlockHandle blockHandle;
        blockFileNode->mAccessor.GetBlock(10 * mBlockSize, blockHandle);
        const string expectData(mBlockSize, 'a' + 10);
        const char* addr1 = reinterpret_cast<const char*>(blockHandle.GetBlock()->data);
        EXPECT_EQ(expectData, string(addr1, mBlockSize));        
        ASSERT_EQ(9, blockFileNode->GetBlockCache()->GetBlockCount());
        future_lite::Future<std::vector<util::BlockHandle>> f = blockFileNode->mAccessor.GetBlocks(8, 5, option);
        auto handles = std::move(f).get();
        ASSERT_EQ((size_t)5, handles.size());
        for (size_t i = 0; i < 5; i ++)
        {
            ASSERT_TRUE(handles[i].GetBlock() != NULL);
            const string expectData(mBlockSize, 'a' + i + 8);
            const char* addr = reinterpret_cast<const char*>(handles[i].GetBlock()->data);
            EXPECT_EQ(expectData, string(addr, mBlockSize)) << "not equal " << i << endl;
        }
        ASSERT_EQ(13, blockFileNode->GetBlockCache()->GetBlockCount());
    }
}

void BlockFileNodeTest::TestCaseForGetBlock()
{
    GenerateFile(mFileName, 2 * mBlockSize + 2, mBlockSize);
    BlockFileNodePtr blockFileNode = CreateFileNode();
    blockFileNode->Open(mFileName, FSOT_CACHE);
    size_t offset = mBlockSize / 2; // 1st block
    blockid_t blockId(blockFileNode->mAccessor.GetFileId(),
                      blockFileNode->mAccessor.GetBlockOffset(offset) / mBlockSize);

    BlockHandle blockHandle;
    blockFileNode->mAccessor.GetBlock(offset, blockHandle);
    ASSERT_TRUE(blockHandle.GetBlock() != NULL);
    Cache::Handle* handle = blockHandle.StealHandle();

    const string expectData(mBlockSize, 'a');
    const string curData((char*)blockHandle.GetData(), mBlockSize);
        
    ASSERT_EQ(expectData, curData.substr(0, mBlockSize));
    ASSERT_EQ(blockId, blockHandle.GetBlock()->id);
    ASSERT_EQ(2U, blockFileNode->GetBlockCache()->TEST_GetRefCount(handle));

    BlockHandle blockHandle2;
    blockFileNode->mAccessor.GetBlock(offset, blockHandle2);
    ASSERT_TRUE(blockHandle2.GetBlock() != NULL);
    Cache::Handle* handle2 = blockHandle2.StealHandle();
    ASSERT_EQ(3U, blockFileNode->GetBlockCache()->TEST_GetRefCount(handle2));
        
    blockFileNode->GetBlockCache()->ReleaseHandle(handle);
    blockFileNode->GetBlockCache()->ReleaseHandle(handle2);

    ASSERT_EQ(1U, blockFileNode->GetBlockCache()->TEST_GetRefCount(handle));
    ASSERT_EQ(1U, blockFileNode->GetBlockCache()->TEST_GetRefCount(handle2));
}

void BlockFileNodeTest::TestCaseForPutSameBlockToCache()
{
    GenerateFile(mFileName, 100, mBlockSize);
    BlockFileNodePtr blockFileNode = CreateFileNode();
    BlockAllocatorPtr allocator = mBlockCache->GetBlockAllocator();
    blockFileNode->Open(mFileName, FSOT_CACHE);
    Block* block = allocator->AllocBlock();
    block->id = blockid_t(0, 1);
    Cache::Handle* handle = NULL;
    blockFileNode->mAccessor.ReadBlockFromFileToCache(block, 0, &handle);
    blockFileNode->GetBlockCache()->ReleaseHandle(handle);
        
    ASSERT_EQ((size_t)1, allocator->TEST_GetAllocatedCount());
        
    Block* block2 = allocator->AllocBlock();
    block2->id = blockid_t(0, 1);
    ASSERT_EQ((size_t)2, allocator->TEST_GetAllocatedCount());
        
    blockFileNode->mAccessor.ReadBlockFromFileToCache(block2, 0, &handle);
    blockFileNode->GetBlockCache()->ReleaseHandle(handle);

    ASSERT_EQ((size_t)1, allocator->TEST_GetAllocatedCount());
}

void BlockFileNodeTest::TestCaseForPutBlockToFullCache()
{
    GenerateFile(mFileName, 100, mBlockSize);

    mBlockCache->Init(3 * mBlockSize, mBlockSize, 0);
    BlockFileNodePtr blockFileNode(new BlockFileNode(mBlockCache.get(), false));
    ASSERT_EQ(3UL, blockFileNode->GetBlockCache()->GetMaxBlockCount());

    blockFileNode->Open(mFileName, FSOT_CACHE);
    BlockAllocatorPtr allocator = mBlockCache->GetBlockAllocator();
    Block* block = allocator->AllocBlock();
    block->id = blockid_t(0, 1);
    Cache::Handle* handle;
    blockFileNode->mAccessor.ReadBlockFromFileToCache(block, 0, &handle);
    blockFileNode->GetBlockCache()->ReleaseHandle(handle);
    ASSERT_EQ(1UL, blockFileNode->GetBlockCache()->GetBlockCount());

    Block* block2 = allocator->AllocBlock();
    block2->id = blockid_t(0, 2);
    blockFileNode->mAccessor.ReadBlockFromFileToCache(block2, 0, &handle);
    blockFileNode->GetBlockCache()->ReleaseHandle(handle);
    ASSERT_EQ(2UL, blockFileNode->GetBlockCache()->GetBlockCount());

    Block* block3 = allocator->AllocBlock();
    block3->id = blockid_t(0, 3);
    blockFileNode->mAccessor.ReadBlockFromFileToCache(block3, 0, &handle);
    blockFileNode->GetBlockCache()->ReleaseHandle(handle);
    ASSERT_EQ(3UL, blockFileNode->GetBlockCache()->GetBlockCount());
        
    Block* block4 = allocator->AllocBlock();
    block4->id = blockid_t(0, 4);
    blockFileNode->mAccessor.ReadBlockFromFileToCache(block4, 0, &handle);
    blockFileNode->GetBlockCache()->ReleaseHandle(handle);
    ASSERT_EQ(3UL, blockFileNode->GetBlockCache()->GetBlockCount());
}

void BlockFileNodeTest::TestCaseForGetBaseAddressException()
{
    GenerateFile(mFileName, 100, mBlockSize);
    BlockFileNodePtr blockFileNode = CreateFileNode();
    blockFileNode->Open(mFileName, FSOT_CACHE);

    ASSERT_FALSE(blockFileNode->GetBaseAddress());
}

void BlockFileNodeTest::TestCaseForWriteException()
{
    GenerateFile(mFileName, 100, mBlockSize);
    BlockFileNodePtr blockFileNode = CreateFileNode();
    blockFileNode->Open(mFileName, FSOT_CACHE);

    uint8_t buffer[] = "abc";
    ASSERT_THROW(blockFileNode->Write(buffer, 3), UnSupportedException);
}

class FakeCommonFileWrapper : public storage::CommonFileWrapper
{
public:
    FakeCommonFileWrapper(const string& path)
        : CommonFileWrapper(fslib::fs::FileSystem::openFile(path, fslib::READ))
    {}
    size_t PRead(void* buffer, size_t length, off_t offset) override
    {
        if (count == 1)
        {
            INDEXLIB_THROW(misc::FileIOException, "%s", "");
        }
        ++count;
        return length;
    }

    future_lite::Future<size_t> PReadAsync(void* buffer, size_t length, off_t offset, int advice) override
    {
        future_lite::Promise<size_t> p;
        p.setValue(PRead(buffer, length, offset));
        return p.getFuture();
    }

    int count = 0;
};

void BlockFileNodeTest::TestCaseForReadWithByteSliceListException()
{
    GenerateFile(mFileName, 4 * mBlockSize, mBlockSize);
    FakeCommonFileWrapper* fakeFileWrapper = new FakeCommonFileWrapper(mFileName);
    FileWrapperPtr fileWrapper(fakeFileWrapper);
    mBlockCache->Init(4 * mBlockSize, mBlockSize, 1);
    BlockFileNode blockFileNode(mBlockCache.get(), false);
    blockFileNode.Open(mFileName, FSOT_CACHE);
    blockFileNode.mAccessor.Close();
    blockFileNode.mAccessor.TEST_SetFile(fileWrapper);

    char* buffer = new char[mBlockSize * 4];
    
    ByteSliceList* byteSliceList = NULL;
    ASSERT_NO_THROW(byteSliceList = blockFileNode.Read(3*mBlockSize, 0));
    ASSERT_EQ(0, mBlockCache->GetBlockCount());
    ASSERT_EQ(0, fakeFileWrapper->count);
    
    ByteSliceReader reader(byteSliceList);
    ASSERT_EQ(1, mBlockCache->GetBlockCount());
    ASSERT_EQ(1, fakeFileWrapper->count);
    
    ASSERT_THROW(reader.Read(buffer, mBlockSize * 3), FileIOException);
    ASSERT_EQ(1, fakeFileWrapper->count);
    ASSERT_EQ(1, mBlockCache->GetBlockCount());

    delete byteSliceList;
    delete[] buffer;
}

void BlockFileNodeTest::GenerateFile(const std::string& fileName, 
                                     size_t size, size_t blockSize)
{
    if (FileSystemWrapper::IsExist(fileName))
    {
        FileSystemWrapper::DeleteFile(fileName);
    }
    FileWrapper* file = FileSystemWrapper::OpenFile(fileName, fslib::WRITE);
    assert(file != NULL);
    char* buffer = new char[blockSize];
    char content = 0;
    size_t n = size / blockSize;
    size_t tail = size % blockSize;
    for (size_t i = 0; i < n; ++i)
    {
        memset(buffer, 'a' + content, blockSize);
        file->Write((void *)buffer, blockSize);
        content = (content + 1) % 26;
    }
    if (tail != 0)
    {
        memset(buffer, 'a' + content, tail);
        file->Write((void *)buffer, tail);
    }
    file->Close();
    delete file;
    delete[] buffer;
    buffer = NULL;
}

void BlockFileNodeTest::GenerateFile(const std::string& fileName,  const std::string& content, size_t blockSize)
{
    if (FileSystemWrapper::IsExist(fileName))
    {
        FileSystemWrapper::DeleteFile(fileName);
    }
    FileWrapper* file = FileSystemWrapper::OpenFile(fileName, fslib::WRITE);
    assert(file != NULL);
    file->Write(content.data(), content.size());
    file->Close();
    delete file;
}


BlockFileNodePtr BlockFileNodeTest::CreateFileNode(bool useDirectIO)
{
    mBlockCache->Init(4 * mBlockSize, mBlockSize, 1);
    BlockFileNodePtr blockFileNode(new BlockFileNode(mBlockCache.get(), useDirectIO));
    return blockFileNode;
}

void BlockFileNodeTest::TestCaseForPrefetch()
{
    GenerateFile(mFileName, 10 * mBlockSize + 1, mBlockSize);
    mBlockCache->Init(100 * mBlockSize, mBlockSize, 1);
    ReadOption option;
    option.blockCounter = new BlockAccessCounter();
    BlockFileNodePtr blockFileNode(new BlockFileNode(mBlockCache.get(), false));
    blockFileNode->Open(mFileName, FSOT_CACHE);
    const uint64_t len = 100;
    char buffer[len];
    ASSERT_EQ(0, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(0, option.blockCounter->blockCacheMissCount);

    ASSERT_EQ(20, blockFileNode->Prefetch(20, 0, option));
    ASSERT_EQ(0, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(5, option.blockCounter->blockCacheMissCount);

    ASSERT_EQ(20, blockFileNode->Read(buffer, 20, 0, option));
    ASSERT_EQ(5, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(5, option.blockCounter->blockCacheMissCount);

    ASSERT_EQ(4, blockFileNode->Read(buffer + 20, 4, 20, option));
    ASSERT_EQ(5, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(6, option.blockCounter->blockCacheMissCount);

    ASSERT_EQ(15, blockFileNode->Prefetch(15, 20, option));
    ASSERT_EQ(6, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(9, option.blockCounter->blockCacheMissCount);

    ASSERT_EQ(16, blockFileNode->Read(buffer + 24, 16, 24, option));
    ASSERT_EQ(9, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(10, option.blockCounter->blockCacheMissCount);
    ASSERT_EQ(string("aaaabbbbccccddddeeeeffffgggghhhhiiiijjjj"), string(buffer, 40));
    delete option.blockCounter;
}

void BlockFileNodeTest::TestCaseForReadUInt32Async()
{
    constexpr size_t blockSize = 2;
    constexpr uint8_t size = 21;
    string content(size, '\0');
    for (uint8_t i = 0; i < size; ++i)
    {
        content[i] = i;
    }
    GenerateFile(mFileName, content, blockSize);
    mBlockCache->Init(100 * blockSize, blockSize, 0);
    ReadOption option;
    option.blockCounter = new BlockAccessCounter();
    BlockFileNodePtr blockFileNode(new BlockFileNode(mBlockCache.get(), false));
    blockFileNode->Open(mFileName, FSOT_CACHE);

    auto check = [this, option, blockFileNode, &content](size_t offset) mutable {
        uint32_t result = blockFileNode->ReadUInt32Async(offset, option).get();
        uint32_t expected = *reinterpret_cast<uint32_t*>(const_cast<char*>(content.c_str()) + offset);
        ASSERT_EQ(expected, result) << "check " << offset << " failed";
    };
    check(0);    
    check(5);
    check(13);
    check(17);

    delete option.blockCounter;
}

void BlockFileNodeTest::TestCaseForReadMultipleBlocks()
{
    constexpr uint32_t fileSize = 20000 * 1024 + 1;
    mBlockSize = 512;
    GenerateFile(mFileName, fileSize, mBlockSize);
    BlockFileNodePtr blockFileNode = CreateFileNode(true);
    blockFileNode->Open(mFileName, FSOT_CACHE);
    char stack[8388534];
    constexpr size_t length = fileSize;
    size_t sum = 0;
    for (size_t i = 0; i < 8388534; ++i)
    {
        sum += stack[i];
    }
    cout << sum << endl;
    auto buffer = new char[length];
    auto readSize = blockFileNode->Read(buffer, length, 0);
    ASSERT_EQ(fileSize, readSize);
    delete[] buffer;
}

IE_NAMESPACE_END(file_system);

