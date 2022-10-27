#ifndef __INDEXLIB_BUFFERED_PACKAGE_FILE_WRITER_H
#define __INDEXLIB_BUFFERED_PACKAGE_FILE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/buffered_file_writer.h"

DECLARE_REFERENCE_CLASS(storage, FileBuffer);
DECLARE_REFERENCE_CLASS(file_system, FileNode);
DECLARE_REFERENCE_CLASS(file_system, PackageStorage);
DECLARE_REFERENCE_CLASS(util, ThreadPool);

IE_NAMESPACE_BEGIN(file_system);

class BufferedPackageFileWriter : public BufferedFileWriter
{
public:
    BufferedPackageFileWriter(PackageStorage* packageStorage, size_t packageUnitId,
                              const FSWriterParam& param = FSWriterParam());
    ~BufferedPackageFileWriter();

public:
    void Open(const std::string& path) override;
    void Close() override;

private:
    void DumpBuffer() override;

private:
    PackageStorage* mStorage;
    size_t mUnitId;
    uint32_t mPhysicalFileId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BufferedPackageFileWriter);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BUFFERED_PACKAGE_FILE_WRITER_H
