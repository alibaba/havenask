#include "indexlib/file_system/file_node.h"
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileNode);

FileNode::FileNode()
    : mOpenType(FSOT_UNKNOWN)
    , mOriginalOpenType(FSOT_UNKNOWN)
    , mDirty(false)
    , mInPackage(false)
{
}

FileNode::~FileNode() 
{
}

FileNode::FileNode(const FileNode& other)
    : mPath(other.mPath)
    , mOpenType(other.mOpenType)
    , mOriginalOpenType(other.mOriginalOpenType)
    , mDirty(other.mDirty)
    , mInPackage(other.mInPackage)
{
}

void FileNode::Open(const std::string& path,
                    FSOpenType openType)
{
    mPath = PathUtil::NormalizePath(path);
    mOpenType = openType;
    DoOpen(PathUtil::NormalizePath(path), mOpenType);
}

void FileNode::Open(const PackageOpenMeta& packageOpenMeta, FSOpenType openType)
{
    mPath = PathUtil::NormalizePath(packageOpenMeta.GetLogicalFilePath());
    mOpenType = openType;
    mInPackage = true;
    DoOpen(packageOpenMeta, openType);
}

void FileNode::Open(const string& path,
                    const fslib::FileMeta& fileMeta, 
                    FSOpenType openType)
{
    mPath = PathUtil::NormalizePath(path);
    mOpenType = openType;
    DoOpen(mPath, fileMeta, openType);
}

void FileNode::Open(const string& path, const string& realPath,
                    const fslib::FileMeta& fileMeta, FSOpenType openType)
{
    mPath = PathUtil::NormalizePath(path);
    mOpenType = openType;
    if (fileMeta.fileLength < 0)
    {
        DoOpen(PathUtil::NormalizePath(realPath), mOpenType);
    }
    else
    {
        DoOpen(PathUtil::NormalizePath(realPath), fileMeta, mOpenType);
    }
}

FileNode* FileNode::Clone() const
{
    // only in_mem_file_node support clone currently
    // TODO: other file node type
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "FileNode [openType:%d] not support Clone!",
                         mOpenType);
    return NULL;
}

future_lite::Future<uint32_t> FileNode::ReadVUInt32Async(size_t offset, ReadOption option)
{
    auto fileLen = GetLength();
    if (unlikely(offset >= fileLen))
    {
        INDEXLIB_FATAL_ERROR(OutOfRange,
            "read file out of range, offset: [%lu], "
            "file length: [%lu]",
            offset, fileLen);
    }
    auto bufferPtr = std::make_unique<uint64_t>(0);
    auto buffer = static_cast<void*>(bufferPtr.get());
    size_t bufferSize = std::min(sizeof(uint64_t), fileLen - offset);
    return ReadAsync(buffer, bufferSize, offset, option)
        .thenValue([ptr = std::move(bufferPtr)](size_t readSize) {
            uint8_t* byte = reinterpret_cast<uint8_t*>(ptr.get());
            uint32_t value = (*byte) & 0x7f;
            int shift = 7;
            while ((*byte) & 0x80)
            {
                ++byte;
                value |= ((*byte & 0x7F) << shift);
                shift += 7;
            }
            return value;
        });
}

IE_NAMESPACE_END(file_system);

