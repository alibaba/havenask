#ifndef __INDEXLIB_FS_BUFFERED_FILE_WRITER_H
#define __INDEXLIB_FS_BUFFERED_FILE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/buffered_file_output_stream.h"


DECLARE_REFERENCE_CLASS(storage, FileBuffer);
DECLARE_REFERENCE_CLASS(util, ThreadPool);
DECLARE_REFERENCE_CLASS(file_system, FileNode);

IE_NAMESPACE_BEGIN(file_system);

class BufferedFileWriter : public FileWriter
{
public:
    BufferedFileWriter(const FSWriterParam& param = FSWriterParam(true, false));
    ~BufferedFileWriter();

public:
    void ResetBufferParam(size_t bufferSize, bool asyncWrite);

    void Open(const std::string& path) override;
    void OpenWithoutCheck(const std::string& path);
    size_t Write(const void* buffer, size_t length) override;
    size_t GetLength() const override;
    const std::string& GetPath() const override;
    void Close() override;

protected:
    virtual void DumpBuffer();
    
private:
    std::string GetDumpPath() const;

protected:
    BufferedFileOutputStreamPtr mStream;
    int64_t mLength;
    bool mIsClosed;
    std::string mFilePath;
    storage::FileBuffer *mBuffer;
    storage::FileBuffer *mSwitchBuffer;
    util::ThreadPoolPtr mThreadPool;

private:
    IE_LOG_DECLARE();
    friend class BufferedFileWriterTest;
    friend class PartitionResourceProviderInteTest;
};

DEFINE_SHARED_PTR(BufferedFileWriter);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FS_BUFFERED_FILE_WRITER_H
