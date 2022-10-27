#ifndef __INDEXLIB_FS_IN_MEM_FILE_WRITER_H
#define __INDEXLIB_FS_IN_MEM_FILE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/in_mem_storage.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/in_mem_package_file_writer.h"
#include "indexlib/util/slice_array/aligned_slice_array.h"

IE_NAMESPACE_BEGIN(file_system);

class InMemFileWriter : public FileWriter
{
private:
    typedef util::AlignedSliceArray<char> SliceArray;
public:
    // for not packaged file
    InMemFileWriter(const util::BlockMemoryQuotaControllerPtr& memController,
            InMemStorage* inMemStorage,
            const FSWriterParam& param = FSWriterParam());

    // for inner file writer in package file
    InMemFileWriter(const util::BlockMemoryQuotaControllerPtr& memController,
            InMemPackageFileWriter* packageFileWriter,
            const FSWriterParam& param = FSWriterParam());

    ~InMemFileWriter();

public:
    void Open(const std::string& path) override;
    size_t Write(const void* buffer, size_t length) override;
    size_t GetLength() const override;
    const std::string& GetPath() const override;
    void Close() override;

    // todo: add ut for useBuffer = false
    void ReserveFileNode(size_t reserveSize) override;

protected:
    InMemFileNodePtr CreateFileNodeFromSliceArray();

private:
    const static uint64_t SLICE_LEN = 32 * 1024 * 1024;
    const static uint32_t SLICE_NUM = 32 * 1024;
    
protected:
    std::string mPath;
    InMemStorage* mInMemStorage;
    // must ensure packageFileWriter live longer
    InMemPackageFileWriter* mPackageFileWriter;
    SliceArray* mSliceArray;
    InMemFileNodePtr mInMemFileNode;
    util::BlockMemoryQuotaControllerPtr mMemController;
    size_t mLength;
    bool mIsClosed;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemFileWriter);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FS_IN_MEM_FILE_WRITER_H
