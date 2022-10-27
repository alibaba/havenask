#ifndef __INDEXLIB_SLICE_FILE_WRITER_H
#define __INDEXLIB_SLICE_FILE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_writer.h"

DECLARE_REFERENCE_CLASS(file_system, SliceFileNode);

IE_NAMESPACE_BEGIN(file_system);

class SliceFileWriter : public FileWriter
{
public:
    SliceFileWriter(const SliceFileNodePtr& sliceFileNode);
    ~SliceFileWriter();

public:
    void Open(const std::string& path) override;
    size_t Write(const void* buffer, size_t length) override;
    size_t GetLength() const override;
    const std::string& GetPath() const override;
    void Close() override;

private:
    SliceFileNodePtr mSliceFileNode;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SliceFileWriter);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SLICE_FILE_WRITER_H
