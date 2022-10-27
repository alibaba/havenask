#ifndef __INDEXLIB_SLICE_FILE_READER_H
#define __INDEXLIB_SLICE_FILE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/slice_file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class SliceFileReader : public FileReader
{
public:
    SliceFileReader(const SliceFileNodePtr& sliceFileNode);
    ~SliceFileReader();

public:
    void Open() override { }
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override;
    size_t Read(void* buffer, size_t length, ReadOption option = ReadOption()) override;
    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) override;
    void* GetBaseAddress() const override;
    size_t GetLength() const override;
    const std::string& GetPath() const override;
    FSOpenType GetOpenType() override;
    void Close() override;
    FileNodePtr GetFileNode() const override
    { return mSliceFileNode; }

public:
    const util::BytesAlignedSliceArrayPtr& GetBytesAlignedSliceArray() const
    { assert(mSliceFileNode); return mSliceFileNode->GetBytesAlignedSliceArray(); }

    // inline interface for efficient
    size_t GetSliceFileLength() const
    { assert(mSliceFileNode); return mSliceFileNode->GetSliceFileLength(); }

private:
    SliceFileNodePtr mSliceFileNode;
    size_t mOffset;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SliceFileReader);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SLICE_FILE_READER_H
