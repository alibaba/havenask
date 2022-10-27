#ifndef __INDEXLIB_FAKE_FILE_NODE_H
#define __INDEXLIB_FAKE_FILE_NODE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class FakeFileNode : public FileNode
{
public:
    FakeFileNode(FSFileType type = FSFT_IN_MEM);
    ~FakeFileNode();

public:
    FSFileType GetType() const { return mType; }
    FSOpenType GetOpenType() const { return FSOT_UNKNOWN; }
    size_t GetLength() const { return 0; }
    void* GetBaseAddress() const { return NULL; }
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override
    {
        return 0;
    }
    util::ByteSliceList* Read(
            size_t length, size_t offset, ReadOption option = ReadOption()) override
    {
        return NULL;
    }

    size_t Write(const void* buffer, size_t length) { return 0; }
    void Close() {}
    bool ReadOnly() const override { return false; }

private:
    void DoOpen(const std::string& path, FSOpenType type) {}
    void DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) {}
    void DoOpen(const std::string& path, const fslib::FileMeta& fileMeta, FSOpenType openType) {}

private:
    FSFileType mType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeFileNode);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FAKE_FILE_NODE_H
