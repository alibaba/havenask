#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/ReadOption.h"

namespace indexlib {
namespace file_system {
class PackageOpenMeta;
} // namespace file_system
namespace util {
class ByteSliceList;
} // namespace util
} // namespace indexlib

namespace indexlib { namespace file_system {

class FakeFileNode : public FileNode
{
public:
    FakeFileNode(FSFileType type = FSFT_MEM) : _type(type) {}
    ~FakeFileNode() {}

public:
    FSFileType GetType() const noexcept override { return _type; }
    FSOpenType GetOpenType() const noexcept { return FSOT_UNKNOWN; }
    size_t GetLength() const noexcept override { return 0; }
    void* GetBaseAddress() const noexcept override { return NULL; }
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset,
                          ReadOption option = ReadOption()) noexcept override
    {
        return {FSEC_OK, 0};
    }
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset,
                                             ReadOption option = ReadOption()) noexcept override
    {
        return NULL;
    }
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override { return {FSEC_OK, 0}; }
    FSResult<void> Close() noexcept override { return FSEC_OK; }
    bool ReadOnly() const noexcept override { return false; }

private:
    ErrorCode DoOpen(const std::string& path, FSOpenType type, int64_t fileLength) noexcept override { return FSEC_OK; }
    ErrorCode DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept override { return FSEC_OK; }

private:
    FSFileType _type;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FakeFileNode> FakeFileNodePtr;
}} // namespace indexlib::file_system
