#ifndef __INDEXLIB_PACKAGE_FILE_WRITER_H
#define __INDEXLIB_PACKAGE_FILE_WRITER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/file_system_define.h"

IE_NAMESPACE_BEGIN(file_system);

class PackageFileWriter
{
public:
    PackageFileWriter(const std::string& filePath)
        : mFilePath(filePath)
        , mIsClosed(false)
    {}

    virtual ~PackageFileWriter() {}

public:
    virtual FileWriterPtr CreateInnerFileWriter(
        const std::string& filePath,
        const FSWriterParam& param = FSWriterParam()) = 0;

    virtual void MakeInnerDir(const std::string& dirPath) = 0;

    virtual void GetPhysicalFiles(std::vector<std::string>& files) const = 0;

    void Close()
    {
        autil::ScopedLock scopeLock(mLock);
        if (!mIsClosed)
        {
            DoClose();
            mIsClosed = true;
        }
    }

    bool IsClosed() const { return mIsClosed; }

    const std::string& GetFilePath() const { return mFilePath; }

protected:
    virtual void DoClose() = 0;

protected:
    autil::RecursiveThreadMutex mLock;    
    std::string mFilePath;
    bool mIsClosed;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageFileWriter);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKAGE_FILE_WRITER_H
