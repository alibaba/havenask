#ifndef __INDEXLIB_DIRECTORY_FILE_NODE_H
#define __INDEXLIB_DIRECTORY_FILE_NODE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class DirectoryFileNode : public FileNode
{
public:
    DirectoryFileNode();
    ~DirectoryFileNode();

public:
    FSFileType GetType() const override;
    size_t GetLength() const override;
    void* GetBaseAddress() const override;
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override;
    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) override;
    size_t Write(const void* buffer, size_t length) override;
    void Close() override;
    bool ReadOnly() const override { return true; }

private:
    void DoOpen(const std::string& path, FSOpenType openType) override;
    void DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) override;
    void DoOpen(const std::string& path, const fslib::FileMeta& fileMeta,
                FSOpenType openType) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DirectoryFileNode);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DIRECTORY_FILE_NODE_H
