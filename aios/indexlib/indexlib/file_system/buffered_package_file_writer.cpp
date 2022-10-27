#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/buffered_package_file_writer.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/buffered_file_node.h"
#include "indexlib/file_system/file_work_item.h"
#include "indexlib/file_system/package_storage.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/file_buffer.h"
#include "indexlib/util/thread_pool.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BufferedPackageFileWriter);

BufferedPackageFileWriter::BufferedPackageFileWriter(
        PackageStorage* packageStorage, size_t packageUnitId,
        const FSWriterParam& param)
    : BufferedFileWriter(param)
    , mStorage(packageStorage)
    , mUnitId(packageUnitId)
    , mPhysicalFileId(-1)
{
}

BufferedPackageFileWriter::~BufferedPackageFileWriter() 
{
    if (!mIsClosed)
    {
        IE_LOG(ERROR, "file [%s] not CLOSED in destructor. Exception flag [%d]",
               mFilePath.c_str(),
               misc::IoExceptionController::HasFileIOException());
        assert(misc::IoExceptionController::HasFileIOException());
    }
    else
    {
        assert(!mStream);
        assert(!mBuffer);
        assert(!mSwitchBuffer);
    }
}

void BufferedPackageFileWriter::Open(const string& path)
{
    mFilePath = path;
    mIsClosed = false;
}

void BufferedPackageFileWriter::Close()
{
    if (mIsClosed)
    {
        return;
    }
    mIsClosed = true;
    if (!mStream)
    {
        // not flushed yet, will flush in PackageStorage.Flush()
        mStorage->StoreLogicalFile(mUnitId, mFilePath, mBuffer);
        mBuffer = NULL;
        DELETE_AND_SET_NULL(mSwitchBuffer);
    }
    else
    {
        DumpBuffer();
        if (mSwitchBuffer)
        {
            mSwitchBuffer->Wait();
        }
        if (mThreadPool)
        {
            mThreadPool->CheckException();
        }
        mStorage->StoreLogicalFile(mUnitId, mFilePath, mPhysicalFileId, mLength);
        mStream.reset();
        DELETE_AND_SET_NULL(mBuffer);
        DELETE_AND_SET_NULL(mSwitchBuffer);
    }
}

void BufferedPackageFileWriter::DumpBuffer() 
{
    if (!mStream)
    {
        auto ret = mStorage->AllocatePhysicalStream(mUnitId, GetPath());
        mPhysicalFileId = ret.first;
        mStream = ret.second;
    }
    
    BufferedFileWriter::DumpBuffer();
}

IE_NAMESPACE_END(file_system);

