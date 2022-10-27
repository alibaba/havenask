#ifndef __INDEXLIB_LOG_FILE_H
#define __INDEXLIB_LOG_FILE_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include <autil/legacy/jsonizable.h>
#include "indexlib/storage/file_wrapper.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(storage);

class LogFile 
{
public:
    LogFile()
        : mOpenMode(fslib::APPEND)
    {
    }
    ~LogFile();

public:
    void Open(const std::string& filePath, fslib::Flag openMode, bool useDirectIO); //flag only support write, read
    void Write(const std::string& key, const char* value, int64_t valueSize);
    void Flush();
    size_t Read(const std::string& key, char* readBuffer, int64_t bufferSize);
    void Close();
    bool HasKey(const std::string& key);
    int64_t GetValueLength(const std::string& key);

    static bool IsClose(const std::string& filePath);
    static void GetLogFiles(const fslib::FileList& fileList,
                            fslib::FileList& logFileInfos);


private:
    struct RecordInfo : public autil::legacy::Jsonizable
    {
        int64_t valueOffset;
        int64_t valueLength;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) 
        {
            json.Jsonize("value_offset", valueOffset, valueOffset);
            json.Jsonize("value_length", valueLength, valueLength);
        }
    };
    void WriteMeta(const std::string& key, const RecordInfo& recordInfo);
    void LoadLogFile(const std::string& dataFile);
    void LoadLogFile(const std::string& metaFile, const std::string& dataFile, fslib::Flag openMode);
    bool Parse(std::string& lineStr, std::string& key, RecordInfo& record);

private:
    std::string mFilePath;
    storage::FileWrapperPtr mMetaFile;
    storage::FileWrapperPtr mDataFile;
    autil::ThreadMutex mFileLock;
    fslib::Flag mOpenMode;
    std::map<std::string, RecordInfo> mMetaInfos; 
    static const std::string META_FILE_SUFFIX;
    static const std::string DATA_FILE_SUFFIX;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LogFile);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_LOG_FILE_H
