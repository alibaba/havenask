#include <autil/Lock.h>
#include "indexlib/misc/exception.h"
#include "indexlib/storage/log_file.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/common_file_wrapper.h"
#include "indexlib/storage/line_reader.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::legacy;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, LogFile);

const std::string LogFile::META_FILE_SUFFIX = ".lfm";
const std::string LogFile::DATA_FILE_SUFFIX = ".lfd";


LogFile::~LogFile() 
{
}

void LogFile::Open(const std::string& filePath, fslib::Flag openMode, bool useDirectIO)
{
    ScopedLock lock(mFileLock);
    mFilePath = filePath;
    mOpenMode = openMode;
    if (openMode != READ && openMode != APPEND) 
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "log file [%s] open only support read or append", 
                             filePath.c_str());
    }
    string metaFile = filePath + META_FILE_SUFFIX;
    string dataFile = filePath + DATA_FILE_SUFFIX;
    LoadLogFile(metaFile, dataFile, openMode);
}

bool LogFile::HasKey(const std::string& key) 
{
    ScopedLock lock(mFileLock);
    return mMetaInfos.find(key) != mMetaInfos.end();
}

bool LogFile::Parse(std::string& lineStr, std::string& key, RecordInfo& record)
{
    vector<string> recordInfos = StringUtil::split(lineStr, "\t");
    if (recordInfos.size() != 4)
    {
        return false;
    }
    int64_t keyLength;
    if (!StringUtil::numberFromString(recordInfos[0], keyLength)) 
    {
        return false;
    }
    if (keyLength != (int64_t)recordInfos[3].length())
    {
        return false;
    }
    key = recordInfos[3];
    if (!StringUtil::numberFromString(recordInfos[1], record.valueOffset) || 
        !StringUtil::numberFromString(recordInfos[2], record.valueLength))
    {
        return false;
    }
    return true;
}

void LogFile::LoadLogFile(const std::string& metaFile, 
                          const std::string& dataFile, Flag openMode) 
{
    if (openMode == fslib::READ)
    {
        LineReader lineReader;
        lineReader.Open(metaFile);
        string line;
        while (lineReader.NextLine(line))
        {
            string key;
            RecordInfo recordInfo;
            if (Parse(line, key, recordInfo))
            {
                mMetaInfos[key] = recordInfo;
            }
            else 
            {
                IE_LOG(DEBUG, "log file [%s] record is illegal format", mFilePath.c_str());
            }
        }        
    }
    mDataFile.reset(FileSystemWrapper::OpenFile(dataFile, openMode));
    if (openMode == fslib::APPEND)
    {
        mMetaFile.reset(FileSystemWrapper::OpenFile(metaFile, openMode));
        char endLine[1] = {'\n'};
        mMetaFile->Write(endLine, 1);
    }
}

void LogFile::Write(const std::string& key, const char* value, int64_t valueSize) 
{
    ScopedLock lock(mFileLock);
    RecordInfo recordInfo;
    recordInfo.valueOffset = mDataFile->Tell();
    recordInfo.valueLength = valueSize;
    mDataFile->Write(value, valueSize);
    mDataFile->Flush();    
    WriteMeta(key, recordInfo);
    mMetaInfos[key] = recordInfo;
}

void LogFile::WriteMeta(const std::string& key, const RecordInfo& recordInfo)
{
    // totalLegth, valueOffset, valueLength is int64_t and "\n"
    string keyLengthStr = StringUtil::toString(key.size()) + "\t";
    mMetaFile->Write((void*)(keyLengthStr.c_str()), keyLengthStr.length());
    string valueOffsetStr = StringUtil::toString(recordInfo.valueOffset) + "\t";
    mMetaFile->Write((void*)valueOffsetStr.c_str(), valueOffsetStr.length());
    string valueLengthStr = StringUtil::toString(recordInfo.valueLength) + "\t";
    mMetaFile->Write((char*)(valueLengthStr.c_str()), valueLengthStr.length());
    mMetaFile->Write(key.c_str(), key.size());
    char endLine[1] = {'\n'};
    mMetaFile->Write(endLine, 1);
}

void LogFile::Flush()
{
    ScopedLock lock(mFileLock);
    mDataFile->Flush();
    mMetaFile->Flush();
}

size_t LogFile::Read(const std::string& key, char* readBuffer, int64_t bufferSize)
{
    ScopedLock lock(mFileLock);
    auto iter = mMetaInfos.find(key);
    if (iter == mMetaInfos.end()) 
    {
        return 0;
    }
    if (bufferSize < iter->second.valueLength) {
        assert(false);
        INDEXLIB_FATAL_ERROR(FileIO, "read log file failed, "
                             "buffer size [%ld] is smaller "
                             "than value size [%ld], key[%s]",
                             bufferSize, iter->second.valueLength,
                             key.c_str());
    }
    ssize_t readLength = mDataFile->PRead(readBuffer,
            iter->second.valueLength,
            iter->second.valueOffset);
    if (readLength != iter->second.valueLength)
    {
        assert(false);
        INDEXLIB_FATAL_ERROR(FileIO, "read log file failed, "
                             "read size [%ld] is not equal "
                             "with value size [%ld], key[%s]",
                             readLength, iter->second.valueLength,
                             key.c_str());
    }
    return readLength;
}

void LogFile::Close() {
    ScopedLock lock(mFileLock);
    if (mDataFile)
    {
        mDataFile->Close();
    }
    if (mMetaFile)
    {
        mMetaFile->Close();
    }

}

bool LogFile::IsClose(const std::string& filePath)
{    
    return FileSystemWrapper::IsExist(filePath);
}

int64_t LogFile::GetValueLength(const std::string& key)
{
    ScopedLock lock(mFileLock);
    auto iter = mMetaInfos.find(key);
    if (iter == mMetaInfos.end())
    {
        return 0;
    }
    return iter->second.valueLength;
}

void LogFile::GetLogFiles(const fslib::FileList& fileList,
                          fslib::FileList& logFiles)
{
    for(size_t i = 0 ; i < fileList.size(); ++i)
    {
        if (fileList[i].find(META_FILE_SUFFIX) == string::npos)
        {
            continue;
        }
        string logFileName = fileList[i].substr(0, fileList[i].find(META_FILE_SUFFIX));
        logFiles.push_back(logFileName);
    }
}

IE_NAMESPACE_END(storage);

