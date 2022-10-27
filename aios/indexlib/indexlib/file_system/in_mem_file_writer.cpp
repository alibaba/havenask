#include "indexlib/file_system/in_mem_file_writer.h"
#include "indexlib/file_system/in_mem_package_file_writer.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/mmap_pool.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemFileWriter);

InMemFileWriter::InMemFileWriter(const util::BlockMemoryQuotaControllerPtr& memController,
    InMemStorage* inMemStorage, const FSWriterParam& param)
    : FileWriter(param)
    , mInMemStorage(inMemStorage)
    , mSliceArray(NULL)
    , mMemController(memController)
    , mLength(0)
    , mIsClosed(true)
{
    assert(inMemStorage);
}

InMemFileWriter::InMemFileWriter(const util::BlockMemoryQuotaControllerPtr& memController,
    InMemPackageFileWriter* packageFileWriter, const FSWriterParam& param)
    : FileWriter(param)
    , mInMemStorage(NULL)
    , mPackageFileWriter(packageFileWriter)
    , mSliceArray(NULL)
    , mMemController(memController)
    , mLength(0)
    , mIsClosed(true)
{
    assert(packageFileWriter);
}

InMemFileWriter::~InMemFileWriter() 
{
    if (!mIsClosed)
    {
        IE_LOG(ERROR, "file [%s] not CLOSED in destructor. Exception flag [%d]",
               mPath.c_str(),
               misc::IoExceptionController::HasFileIOException());
        assert(misc::IoExceptionController::HasFileIOException());
    }
    DELETE_AND_SET_NULL(mSliceArray);    
}

void InMemFileWriter::Open(const string& path)
{
    mPath = path;
    DELETE_AND_SET_NULL(mSliceArray);
    mSliceArray = new SliceArray(SLICE_LEN, SLICE_NUM, NULL, new MmapPool, true);
    mInMemFileNode.reset();
    mLength = 0;
    mIsClosed = false;
}

void InMemFileWriter::ReserveFileNode(size_t reservedSize)
{
    if (mPath.empty() || mLength > 0 || reservedSize == 0)
    {
        // slice array already have data, not use mInMemFileNode
        return;
    }

    DELETE_AND_SET_NULL(mSliceArray);
    mInMemFileNode.reset(new InMemFileNode(reservedSize, false, LoadConfig(), mMemController));
    mInMemFileNode->Open(mPath, FSOT_IN_MEM);
    mInMemFileNode->SetDirty(true);
}

size_t InMemFileWriter::Write(const void* buffer, size_t length)
{
    if (mInMemFileNode)
    {
        mInMemFileNode->DoWrite(buffer, length);
        mLength += length;
        assert(mLength == mInMemFileNode->GetLength());
        return length;
    }

    assert(mSliceArray);
    mSliceArray->SetList(mLength, (const char* )buffer, length);
    mLength += length;
    assert((int64_t)mLength == mSliceArray->GetDataSize());
    return length;
}

size_t InMemFileWriter::GetLength() const
{
    return mLength;
}

const std::string& InMemFileWriter::GetPath() const
{
    return mPath;
}

void InMemFileWriter::Close()
{
    if (mIsClosed)
    {
        return;
    }
    mIsClosed = true;
    if (!mInMemFileNode)
    {
        mInMemFileNode = CreateFileNodeFromSliceArray();
    }

    if (!mInMemFileNode)
    {
        return;
    }

    assert(!mSliceArray);
    if (mInMemStorage)
    {
        // type(FSWriterParam) == type(FSDumpParam) for now
        mInMemStorage->StoreFile(mInMemFileNode, mWriterParam);
    }
    else
    {
        assert(mPackageFileWriter);
        mPackageFileWriter->StoreFile(mInMemFileNode);
    }
    assert(mInMemFileNode.use_count() > 1);
    mInMemFileNode.reset();
}

InMemFileNodePtr InMemFileWriter::CreateFileNodeFromSliceArray()
{
    if (!mSliceArray)
    {
        return InMemFileNodePtr();
    }

    InMemFileNodePtr fileNode(new InMemFileNode(mLength, false, LoadConfig(), mMemController));
    fileNode->Open(mPath, FSOT_IN_MEM);
    fileNode->SetDirty(true);

    assert((int64_t)mLength == mSliceArray->GetDataSize());
    for (size_t i = 0; i < mSliceArray->GetSliceNum(); ++i)
    {
        fileNode->DoWrite(mSliceArray->GetSlice(i),
                          mSliceArray->GetSliceDataLen(i));
    }
    DELETE_AND_SET_NULL(mSliceArray);
    return fileNode;
}

IE_NAMESPACE_END(file_system);

