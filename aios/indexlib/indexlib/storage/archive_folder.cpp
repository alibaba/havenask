#include "indexlib/storage/archive_folder.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/file_wrapper.h"
#include "indexlib/util/counter/counter_map.h"


using namespace std;
using namespace autil;
using namespace autil::legacy;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, ArchiveFolder);

const std::string ArchiveFolder::LOG_FILE_NAME = "INDEXLIB_ARCHIVE_FILES";
//const std::string ArchiveFolder::LOG_FILE_HINT_NAME = "INDEXLIB_ARCHIVE_HINT_FILE";

ArchiveFolder::ArchiveFolder(bool legacyMode) 
    : mLogFileVersion(-1)
    , mFileCreateCount(0)
    , mLegacyMode(legacyMode)
{
}

ArchiveFolder::~ArchiveFolder() 
{
}

void ArchiveFolder::Open(const string& dirPath, 
                         const std::string& logFileSuffix)
{
    ScopedLock lock(mLock);
    mDirPath = dirPath;
    mLogFileSuffix = logFileSuffix;
    mReadLogFiles.clear();
    InitLogFiles();
}

void ArchiveFolder::InitLogFiles()
{
    fslib::FileList fileList;
    FileSystemWrapper::ListDir(mDirPath, fileList);
    fslib::FileList logFiles;
    vector<int64_t> logFileVersions;
    GetLogFiles(fileList, logFiles, logFileVersions);
    int64_t maxLogFileVersion = -1;
    for (unsigned int i = 0; i < logFiles.size(); i++)
    {
        ReadLogFile readLogFile;
        readLogFile.logFile.reset(new LogFile());
        string logFilePath = FileSystemWrapper::JoinPath(mDirPath,
                logFiles[i]);
        readLogFile.logFile->Open(logFilePath, fslib::READ, true);
        readLogFile.version = logFileVersions[i];
        mReadLogFiles.push_back(readLogFile);
        if (maxLogFileVersion < readLogFile.version)
        {
            maxLogFileVersion = readLogFile.version;
        }
    }
    sort(mReadLogFiles.begin(), mReadLogFiles.end(), ReadLogCompare());
    mLogFileVersion = maxLogFileVersion + 1;
}

void ArchiveFolder::GetLogFiles(const fslib::FileList& fileList,
                                fslib::FileList& logFiles,
                                vector<int64_t>& logFileVersions)
{
    fslib::FileList tmpFiles;
    string suffix = mLogFileSuffix.empty() ? "" : string("_") + mLogFileSuffix;
    string logFileName = LOG_FILE_NAME + suffix;
    for(unsigned int i = 0; i < fileList.size(); ++i)
    {    
        if (fileList[i].find(logFileName) != string::npos)
        {
            tmpFiles.push_back(fileList[i]);
        }
    }
    LogFile::GetLogFiles(tmpFiles, logFiles);
    for (auto& logFile : logFiles)
    {
        string version = logFile.substr(logFile.rfind("_") + 1, logFile.size());
        int64_t tmpVersion;
        if (!StringUtil::fromString(version, tmpVersion))
        {
            assert(false);
            INDEXLIB_FATAL_ERROR(UnSupported, "invalid archive folder log file name[%s]", 
                    logFile.c_str());
        }
        logFileVersions.push_back(tmpVersion);
    }
}

LogFilePtr ArchiveFolder::GetLogFileForWrite(const string& logFileSuffix)
{
    if (mWriteLogFile)
    {
        return mWriteLogFile;
    }
    string suffix = logFileSuffix.empty() ? "" : string("_") + logFileSuffix;
    string logFileName = LOG_FILE_NAME + suffix + "_" +
                         StringUtil::toString(mLogFileVersion);
    string logFilePath = storage::FileSystemWrapper::JoinPath(mDirPath, logFileName);
    LogFilePtr logFile(new LogFile());
    logFile->Open(logFilePath, fslib::APPEND, true);
    mWriteLogFile = logFile;
    return logFile;
}


ArchiveFilePtr ArchiveFolder::CreateInnerFile(const std::string fileName)
{
    string signature = StringUtil::toString(mLogFileVersion) + "_"
                       + StringUtil::toString(mFileCreateCount);
    ArchiveFilePtr archiveFile(new ArchiveFile(false, signature));
    archiveFile->Open(fileName, GetLogFileForWrite(mLogFileSuffix));
    mFileCreateCount++;
    return archiveFile;
}

FileWrapperPtr ArchiveFolder::CreateNormalFile(
        const std::string& filePath, fslib::Flag openMode)
{
    FileSystemWrapper::DeleteIfExist(filePath);
    FileWrapperPtr fileWrapper;
    fileWrapper.reset(FileSystemWrapper::OpenFile(filePath, openMode));
    return fileWrapper;
}

FileWrapperPtr ArchiveFolder::GetInnerFile(const std::string& fileName, fslib::Flag openMode)
{
    ScopedLock lock(mLock);
    if (openMode == fslib::WRITE)
    {
        if (mLegacyMode) {
            string filePath = FileSystemWrapper::JoinPath(mDirPath, fileName);
            return CreateNormalFile(filePath, openMode);
        }
        return CreateInnerFile(fileName);
    }
    assert(openMode == fslib::READ);
    ArchiveFilePtr archiveFile;
    for (size_t i = 0; i < mReadLogFiles.size(); i++)
    {
        if (ArchiveFile::IsExist(fileName, mReadLogFiles[i].logFile))
        {
            archiveFile.reset(new ArchiveFile(true, ""));
            archiveFile->Open(fileName, mReadLogFiles[i].logFile);
            return archiveFile;
        }        
    }

    string filePath = FileSystemWrapper::JoinPath(mDirPath, fileName);
    try 
    {
        FileWrapperPtr file(FileSystemWrapper::OpenFile(filePath, fslib::READ));
        return file;
    } catch (const misc::NonExistException& e)
    {
        return FileWrapperPtr();
    }
    catch (...)
    {
        throw;
    }
    return archiveFile;
}

void ArchiveFolder::Close()
{
    ScopedLock lock(mLock);
    for (size_t i = 0; i < mReadLogFiles.size(); i++)
    {
        mReadLogFiles[i].logFile->Close();
    }
    mReadLogFiles.clear();
    if (mWriteLogFile)
    {
        mWriteLogFile->Close();
        mWriteLogFile.reset();
    }

}

// void ArchiveFolder::GenerateCatalogue(const std::string& dirPath)
// {
//     fslib::FileList fileList;
//     FileSystemWrapper::ListDir(dirPath, fileList);
//     string hintFileName = FileSystemWrapper::JoinPath(dirPath, LOG_FILE_HINT_NAME);
//     FileWrapperPtr hintFile(FileSystemWrapper::OpenFile(hintFileName, fslib::WRITE));
//     string fileInfos = ToJsonString(fileList);
//     hintFile->Write(fileInfos.c_str(), fileInfos.size());
//     hintFile->Close();
// }

IE_NAMESPACE_END(storage);

