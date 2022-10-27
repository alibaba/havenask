#include "indexlib/misc/exception.h"
#include "indexlib/file_system/directory_file_node.h"

using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, DirectoryFileNode);

DirectoryFileNode::DirectoryFileNode() 
{
}

DirectoryFileNode::~DirectoryFileNode() 
{
}

void DirectoryFileNode::DoOpen(const string& path, FSOpenType openType)
{
}

void DirectoryFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "Not support DoOpen with PackageOpenMeta");
}

void DirectoryFileNode::DoOpen(const string& path, const fslib::FileMeta& fileMeta, 
                               FSOpenType openType)
{
}

FSFileType DirectoryFileNode::GetType() const
{
    return FSFT_DIRECTORY;
}

size_t DirectoryFileNode::GetLength() const
{
    return 0;
}

void* DirectoryFileNode::GetBaseAddress() const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "Not support GetBaseAddress");
    return NULL;
}

size_t DirectoryFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "Not support GetBaseAddress");
    return 0;
}

ByteSliceList* DirectoryFileNode::Read(size_t length, size_t offset, ReadOption option)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "Not support GetBaseAddress");
    return NULL;
}

size_t DirectoryFileNode::Write(const void* buffer, size_t length)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "Not support GetBaseAddress");
    return 0;
}

void DirectoryFileNode::Close()
{
}


IE_NAMESPACE_END(file_system);

