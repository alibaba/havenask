#include <autil/StringUtil.h>
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/buffered_file_reader.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;

IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, MergeTaskResource);

#define RESOURCE_FILE_PREFIX "task_resource."

MergeTaskResource::MergeTaskResource()
    : mId(INVALID_MERGE_RESOURCEID)
    , mDataLen(0)
    , mData(NULL)
{
}


MergeTaskResource::MergeTaskResource(const MergeTaskResource& other)
    : mRootDir(other.mRootDir)
    , mRootPath(other.mRootPath)
    , mId(other.mId)
    , mDataLen(other.mDataLen)
    , mData(NULL)
    , mDescription(other.mDescription)
{
}

MergeTaskResource::MergeTaskResource(
        const DirectoryPtr& rootDir, resourceid_t id)
    : mRootDir(rootDir)
    , mRootPath(rootDir->GetPath())
    , mId(id)
    , mDataLen(0)
    , mData(NULL)
{
}

void MergeTaskResource::SetRoot(const DirectoryPtr& rootDir)
{
    mRootDir = rootDir;
}

MergeTaskResource::~MergeTaskResource() 
{
    ARRAY_DELETE_AND_SET_NULL(mData);
}

void MergeTaskResource::Store(const char* data, size_t dataLen)
{
    if (!IsValid())
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "Invalid resource can not store data!");
    }

    if (dataLen == 0)
    {
        return;
    }

    string fileName = GetResourceFileName(mId);
    IE_LOG(INFO, "Begin store merge task resource, filePath [%s/%s]",
           mRootPath.c_str(), fileName.c_str());
    mRootDir->Store(fileName, string(data, dataLen), false);
    mDataLen = dataLen;
    IE_LOG(INFO, "end store merge task resource, filePath [%s/%s]",
           mRootPath.c_str(), fileName.c_str());
}

size_t MergeTaskResource::Read(char* buffer, size_t bufferSize)
{
    if (!IsValid())
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "Invalid resource can not load data!");
    }

    if (mDataLen == 0)
    {
        return 0;
    }
    
    string filePath = GetResourceFileName(mId);
    size_t fileLen = mRootDir->GetFileLength(filePath);
    if (fileLen != mDataLen)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "fileLength [%lu] not match with dataLen [%lu], filePath [%s/%s]",
                             fileLen, mDataLen, mRootDir->GetPath().c_str(), filePath.c_str());
    }

    if (buffer == NULL || bufferSize < mDataLen)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "invalid buffer parameter, buffer size: %lu, data len: %lu!",
                             bufferSize, mDataLen);
    }

    IE_LOG(INFO, "Begin load merge task resource, filePath [%s/%s]",
           mRootPath.c_str(), filePath.c_str());
    assert(mRootDir);
    FileReaderPtr fileReader = mRootDir->CreateFileReader(filePath, FSOpenType::FSOT_BUFFERED);
    fileReader->Open();
    if ((size_t)fileReader->Read(buffer, mDataLen) != mDataLen)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "read file [%s] fail!", filePath.c_str());
    }
    fileReader->Close();
    IE_LOG(INFO, "End load merge task resource, filePath [%s/%s]",
           mRootPath.c_str(), filePath.c_str());
    return mDataLen;
}

char* MergeTaskResource::GetData()
{
    if (!IsValid() || mDataLen == 0)
    {
        return NULL;
    }

    if (!mData)
    {
        LoadInnerDataBuffer();
    }
    return mData;
}
    
void MergeTaskResource::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("root_path", mRootPath, mRootPath);
    json.Jsonize("resource_id", mId, mId);
    json.Jsonize("data_length", mDataLen, mDataLen);
    json.Jsonize("description", mDescription, mDescription);
}
    
string MergeTaskResource::GetResourceFileName(resourceid_t resId)
{
    return RESOURCE_FILE_PREFIX + StringUtil::toString(resId);
}

void MergeTaskResource::LoadInnerDataBuffer()
{
    ScopedLock lock(mLock);
    mData = new char[mDataLen];
    Read(mData, mDataLen);
}

bool MergeTaskResource::IsValid() const
{
    return mId != INVALID_MERGE_RESOURCEID;
}

void MergeTaskResource::operator=(const MergeTaskResource& other)
{
    mRootDir = other.mRootDir;
    mId = other.mId;
    mDataLen = other.mDataLen;
    mData = NULL;
    mDescription = other.mDescription;
    mRootPath = other.mRootPath;
}

const string& MergeTaskResource::GetRootPath() const
{
    return mRootPath;
}

IE_NAMESPACE_END(index_base);

