/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef FSLIB_COMMON_TYPE_H_
#define FSLIB_COMMON_TYPE_H_

#include <string>
#include <vector>
#include <map>
#include <unistd.h>
#include <cstddef>
#include <sys/types.h>
#include <stdint.h>
#include <string.h>
#include <cstdio>

namespace fslib { 

enum ErrorCode {
    EC_OK = 0,
    EC_AGAIN = 1,
    EC_BADF = 2,
    EC_BADARGS = 3,
    EC_BUSY = 4,
    EC_CONNECTIONLOSS = 5,
    EC_EXIST = 6,
    EC_FALSE = 7,
    EC_ISDIR = 8,
    EC_KUAFU = 9,
    EC_NOENT = 10,
    EC_NOTDIR = 11,
    EC_NOTEMPTY = 12,
    EC_NOTSUP = 13,
    EC_PARSEFAIL = 14,
    EC_PERMISSION = 15,
    EC_TRUE = 16,
    EC_OPERATIONTIMEOUT = 17,
    EC_XDEV = 18,
    EC_INIT_ZOOKEEPER_FAIL = 19,
    EC_INVALIDSTATE = 20,
    EC_SESSIONEXPIRED = 21,
    EC_PANGU_FILE_LOCK = 22,
    EC_UNKNOWN = 23,
    EC_LOCAL_DISK_NO_SPACE = 24,
    EC_LOCAL_DISK_IO_ERROR = 25,
    EC_PANGU_MASTER_MODE = 26,
    EC_PANGU_CLIENT_SESSION = 27,
    EC_PANGU_FILE_LOCK_BY_SAME_PROCESS = 28,
    EC_PANGU_FLOW_CONTROL = 29,
    EC_PANGU_USER_DENIED = 30, // old name: EC_PANGU_PERMISSION_EXCEPTION
    EC_PANGU_NOT_ENOUGH_CHUNKSERVER = 31,
    EC_PANGU_STREAM_CORRUPTED = 32,
    EC_PANGU_INTERNAL_SERVER_ERROR = 33,
    EC_NO_SPACE = 34,
    EC_IO_ERROR = 35,
    EC_READ_ONLY_FILE_SYSTEM = 36,
    EC_ACCESS_DENY = 37,
};

enum Flag {
    READ = 1,
    WRITE = 1 << 1,
    APPEND = 1 << 2,
    FLAG_CMD_MASK = 0xFFFF,
    USE_DIRECTIO = 1 << 16,
    FLAG_CMD_OPTION_MASK = 0xFFFF0000,
    FLAG_MASK = 0xFFFFFFFF
};

enum class IOCtlRequest {
    AdvisePrefetch = 1,
    SetPrefetchParams,
    GetPrefetchParams,
};

enum FileSystemCapability {
    FSC_DISTINCT_FILE_DIR = 0x01,
    FSC_BUILTIN_RENAME_SUPPORT = 0x02,
    FSC_ALL_CAPABILITY = 0xFFFFFFFF
};

typedef std::string FsType;

typedef std::vector<std::string> FileList;
typedef std::vector<std::string>::iterator FileListIterator;
typedef uint64_t FileChecksum;
typedef uint64_t DateTime;

enum SeekFlag {
    FILE_SEEK_SET,  /** seek from file beginning */
    FILE_SEEK_CUR,  /** seek from current stream position */
    FILE_SEEK_END   /** seek from the end of file */
};

struct FileMeta
{
    int64_t fileLength;
    DateTime createTime;
    DateTime lastModifyTime;
};

struct PathMeta
{
public:
    PathMeta(int64_t _len = 0, DateTime _createTs = -1,
             DateTime _modifyTs = -1, bool _isFile = false)
        : isFile(_isFile)
        , length(_len)
        , createTime(_createTs)
        , lastModifyTime(_modifyTs)
    {}
    bool isFile;
    int64_t length;
    DateTime createTime;
    DateTime lastModifyTime;
};

struct EntryMeta
{
    bool isDir;
    std::string entryName;
};
typedef std::vector<EntryMeta> EntryList;

struct EntryInfo {
    std::string entryName;
    int64_t length;
};
typedef std::map<std::string, EntryInfo> EntryInfoMap;

struct RichFileMeta {
    std::string path;
    int64_t size = 0;
    int64_t physicalSize = 0;
    DateTime createTime = 0;
    DateTime lastModifyTime = 0;
    bool isDir = false;
};
typedef std::vector<RichFileMeta> RichFileList;

}
#endif
