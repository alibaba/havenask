#ifndef __INDEXLIB_ARCHIVE_FOLDER_H
#define __INDEXLIB_ARCHIVE_FOLDER_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/storage/archive_file.h"
#include "indexlib/storage/log_file.h"

IE_NAMESPACE_BEGIN(storage);

class ArchiveFolder
{
public:
    ArchiveFolder(bool legacyMode);
    ~ArchiveFolder();
public:
    void Open(const std::string& dirPath, 
              const std::string& logFileSuffix = "");
    FileWrapperPtr GetInnerFile(const std::string& fileName, fslib::Flag openMode);
    void Close();
    //static void GenerateCatalogue(const std::string& dirPath);
    std::string GetFolderPath() const
    {
        return mDirPath;
    }

public:
    static const std::string LOG_FILE_NAME;
    //static const std::string LOG_FILE_HINT_NAME;

private:
    LogFilePtr GetLogFileForWrite(const std::string& logFileSuffix);
    void GetLogFiles(const fslib::FileList& fileList, fslib::FileList& logFiles,
                     std::vector<int64_t>& logFileVersions);
    void InitLogFiles();
    ArchiveFilePtr CreateInnerFile(const std::string fileName);
    FileWrapperPtr CreateNormalFile(
            const std::string& filePath, fslib::Flag openMode);

private:
    struct ReadLogFile {
        int64_t version;
        LogFilePtr logFile;
    };
    struct ReadLogCompare {
        bool operator() (const ReadLogFile& left,
                         const ReadLogFile& right) {
            return left.version > right.version;
        }
    };

private:
    std::string mDirPath;
    std::vector<ReadLogFile> mReadLogFiles;
    LogFilePtr mWriteLogFile;
    autil::ThreadMutex mLock;
    std::string mLogFileSuffix;
    int64_t mLogFileVersion;
    int64_t mFileCreateCount;
    bool mLegacyMode;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ArchiveFolder);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_ARCHIVE_FOLDER_H
