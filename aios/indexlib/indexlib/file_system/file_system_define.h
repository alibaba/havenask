#ifndef __INDEXLIB_INDEXLIB_FILE_SYSTEM_DEFINE_H
#define __INDEXLIB_INDEXLIB_FILE_SYSTEM_DEFINE_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/storage/raid_config.h"
#include <autil/legacy/jsonizable.h>

IE_NAMESPACE_BEGIN(file_system);

struct FSDumpParam
{
    bool atomicDump;     // perform atomic execution when flush
    bool copyOnDump;  // if true, make cloned file node when flush 
    bool prohibitInMemDump; // if true, must use buffer file writer
    storage::RaidConfigPtr raidConfig;

    FSDumpParam(bool atomicDump_ = false, bool copyOnDump_ = false, 
                bool prohibitInMemDump_ = false)
        : atomicDump(atomicDump_)
        , copyOnDump(copyOnDump_)
        , prohibitInMemDump(prohibitInMemDump_)
    {}
};

struct FSWriterParam : public FSDumpParam
{
    bool asyncDump;
    uint32_t bufferSize;
    static const uint32_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024; //2M, for BufferedFileWriter

    FSWriterParam(bool atomicDump_ = false,
                  bool copyOnDump_ = false,
                  bool prohibitInMemDump_ = false,
                  bool asyncDump_ = false,
                  uint32_t bufferSize_ = DEFAULT_BUFFER_SIZE)
        : FSDumpParam(atomicDump_, copyOnDump_, prohibitInMemDump_)
        , asyncDump(asyncDump_)
        , bufferSize(bufferSize_) {}
    FSWriterParam(uint32_t bufferSize_, bool asyncDump_)
        : FSWriterParam(true, false, false, asyncDump_, bufferSize_) {}
};

struct FSReaderParam
{
    static const uint32_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024; //2M, for BufferedFileReader
};

enum FSStorageType
{ 
    FSST_IN_MEM = 0,
    FSST_LOCAL,
    FSST_PACKAGE,
    FSST_UNKNOWN,
};

enum FSOpenType
{
    FSOT_IN_MEM = 0,
    FSOT_MMAP,       // mmap private read+write
    FSOT_CACHE,         // block cache
    FSOT_BUFFERED,
    FSOT_LOAD_CONFIG,
    FSOT_SLICE,         // CreateFileReader not support it
    FSOT_UNKNOWN,    
};

enum FSFileType
{
    FSFT_IN_MEM = 0,
    FSFT_MMAP,
    FSFT_MMAP_LOCK,
    FSFT_BLOCK,         // block cache
    FSFT_BUFFERED,
    FSFT_SLICE,
    FSFT_DIRECTORY,
    FSFT_UNKNOWN,       // must be the last one for StorageMetrics
};

struct FileStat
{
    FSStorageType storageType;
    FSFileType fileType; // valid when inCache == true
    FSOpenType openType; // valid when inCache == true
    size_t fileLength;
    bool inCache;
    bool onDisk;
    bool inPackage;
    bool isDirectory;
};

struct FileInfo: public autil::legacy::Jsonizable
{
public:
    FileInfo()
        : fileLength(-1)
        , modifyTime((uint64_t)-1)
    {};
    
    FileInfo(const std::string& _filePath,
             int64_t _fileLength = -1,
             uint64_t _modifyTime = (uint64_t)-1)
        : filePath(_filePath)
        , fileLength(_fileLength)
        , modifyTime(_modifyTime)
    {}
    
    ~FileInfo() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("path", filePath, filePath);
        json.Jsonize("file_length", fileLength, fileLength);
        json.Jsonize("modify_time", modifyTime, modifyTime);
    }

public:
    bool operator<(const FileInfo& other) const { return filePath < other.filePath; }
    bool isValid() const { return isDirectory() || fileLength != -1; }
    bool isDirectory() const { return *(filePath.rbegin()) == '/'; }
    bool isFile() const { return !isDirectory(); }

public:
    std::string filePath;
    int64_t fileLength;
    uint64_t modifyTime;
};

enum FSMetricPreference
{
    FSMP_ALL = 0,
    FSMP_OFFLINE = 1,
    FSMP_ONLINE = FSMP_ALL,
};

#define TEMP_FILE_SUFFIX ".__fs_tmp__"
#define PACKAGE_FILE_PREFIX "package_file"
#define PACKAGE_FILE_META_SUFFIX ".__meta__"
#define PACKAGE_FILE_DATA_SUFFIX ".__data__"
#define COMPRESS_FILE_INFO_SUFFIX ".__compress_info__"
#define FILE_SYSTEM_ROOT_LINK_NAME "__indexlib_fs_root_link__"

#define DISABLE_FIELDS_LOAD_CONFIG_NAME "__DISABLE_FIELDS__"
#define NOT_READY_OR_DELETE_FIELDS_CONFIG_NAME "__NOT_READY_OR_DELETE_FIELDS__"
#define DEPLOY_META_LOCK_SUFFIX "._ilock_"

IE_NAMESPACE_END(file_system);

#endif // __INDEXLIB_INDEXLIB_FILE_SYSTEM_DEFINE_H
