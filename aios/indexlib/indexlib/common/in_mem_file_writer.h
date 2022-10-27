#ifndef __INDEXLIB_IN_MEM_FILE_WRITER_H
#define __INDEXLIB_IN_MEM_FILE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"

IE_NAMESPACE_BEGIN(common);

class InMemFileWriter final : public file_system::FileWriter
{
public:
    InMemFileWriter();
    ~InMemFileWriter();

public:
    void Open(const std::string& path) override {}
    size_t Write(const void* buffer, size_t length) override;
    size_t GetLength() const override { return mCursor; }
    const std::string& GetPath() const override;
    void Close() override {}

public:
    void Init(uint64_t length);
    // FileWriter owned buffer, so it'll be released when FileWriter released 
    // write will cause ByteSliceListPtr invalid
    util::ByteSliceListPtr GetByteSliceList(bool isVIntHeader) const;
    // write will cause BaseAddress invalid
    void* GetBaseAddress() { return mBuf; }

private:
    size_t CalculateSize(size_t needLength);
    void Extend(size_t extendSize);

public:
    // for ut
    util::ByteSliceListPtr CopyToByteSliceList(bool isVIntHeader) ;

private:
    uint8_t *mBuf;
    uint64_t mLength;
    uint64_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemFileWriter);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_IN_MEM_FILE_WRITER_H
