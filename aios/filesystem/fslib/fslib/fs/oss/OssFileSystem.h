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
#ifndef FSLIB_PLUGIN_OSSFILESYSTEM_H
#define FSLIB_PLUGIN_OSSFILESYSTEM_H

#include <curl/curl.h>
#include <fslib/common.h>

#include "fslib/fs/AbstractFileSystem.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(oss);
using namespace fslib;
using namespace fslib::fs;

class OssFileSystem : public AbstractFileSystem {
public:
    static const std::string CONFIG_SEPARATOR;
    static const std::string KV_SEPARATOR;
    static const std::string KV_PAIR_SEPARATOR;
    static const std::string OSS_PREFIX;

    static const std::string ENV_DEFAULT_OSS_ACCESS_ID;
    static const std::string ENV_DEFAULT_OSS_ACCESS_KEY;
    static const std::string ENV_DEFAULT_OSS_ENDPOINT;
    static const std::string ENV_OSS_CONFIG_PATH;
    static const std::string DEFAULT_OSS_CONFIG_PATH;
    static const std::string OSS_ACCESS_ID;
    static const std::string OSS_ACCESS_KEY;
    static const std::string OSS_ENDPOINT;

public:
    OssFileSystem() { curl_global_init(CURL_GLOBAL_DEFAULT); }
    ~OssFileSystem() { curl_global_cleanup(); }

public:
    /*override*/ File *openFile(const std::string &fileName, Flag flag);

    /*override*/ MMapFile *mmapFile(
        const std::string &fileName, Flag openMode, char *start, size_t length, int prot, int mapFlag, off_t offset);

    /*override*/ ErrorCode rename(const std::string &oldPath, const std::string &newPath);

    /*override*/ ErrorCode link(const std::string &oldPath, const std::string &newPath);

    /*override*/ ErrorCode getFileMeta(const std::string &fileName, FileMeta &fileMeta);

    /*override*/ ErrorCode isFile(const std::string &path);

    /*override*/ FileChecksum getFileChecksum(const std::string &fileName);

    /*override*/ ErrorCode mkDir(const std::string &dirName, bool recursive = false);

    /*override*/ ErrorCode listDir(const std::string &dirName, FileList &fileList);

    /*ovverride*/ ErrorCode isDirectory(const std::string &path);

    /*override*/ ErrorCode remove(const std::string &pathName);

    /*override*/ ErrorCode isExist(const std::string &pathName);

    /*override*/ ErrorCode listDir(const std::string &dirName, EntryList &entryList);

    /*override*/ FileReadWriteLock *createFileReadWriteLock(const std::string &fileName) { return NULL; }

    /*override*/ FileLock *createFileLock(const std::string &fileName) { return NULL; }

    /*override*/ FileSystemCapability getCapability() const {
        return (FileSystemCapability)((~FSC_DISTINCT_FILE_DIR) & (~FSC_BUILTIN_RENAME_SUPPORT));
    }

    static bool getOssConfigFromEnv(std::string &accessKeyId, std::string &accessKeySecret, std::string &endpoint);

    static bool
    parseBucketAndObject(const std::string &fullPathWithOutConfigs, std::string &bucket, std::string &object);

    static bool parseOssConfig(const std::string &configs,
                               std::string &accessKeyId,
                               std::string &accessKeySecret,
                               std::string &endpoint);

    static bool parseOssConfigFromFile(std::string &accessKeyId, std::string &accessKeySecret, std::string &endpoint);

    static bool parseConfigStr(const std::string &fullPath, std::string &configs, std::string &path);

    static bool parsePath(const std::string &fullPath,
                          std::string &bucket,
                          std::string &object,
                          std::string &accessKeyId,
                          std::string &accessKeySecret,
                          std::string &endpoint);

    static ErrorCode listObjects(const std::string &accessKeyId,
                                 const std::string &accessKeySecret,
                                 const std::string &endpoint,
                                 const std::string &bucketStr,
                                 const std::string &prefix,
                                 const std::string &delimiter,
                                 int32_t maxRet,
                                 std::vector<std::string> &contents,
                                 std::vector<std::string> &commonPrefixs);

    static ErrorCode deleteObject(const std::string &accessKeyId,
                                  const std::string &accessKeySecret,
                                  const std::string &endpoint,
                                  const std::string &bucketStr,
                                  const std::string &object);

    static ErrorCode deleteObjects(const std::string &accessKeyId,
                                   const std::string &accessKeySecret,
                                   const std::string &endpoint,
                                   const std::string &bucketStr,
                                   const std::vector<std::string> &objects);

    static ErrorCode mkDirObject(const std::string &accessKeyId,
                                 const std::string &accessKeySecret,
                                 const std::string &endpoint,
                                 const std::string &bucketStr,
                                 const std::string &object);

    static ErrorCode copyObject(const std::string &accessKeyId,
                                const std::string &accessKeySecret,
                                const std::string &endpoint,
                                const std::string &sourceBucketStr,
                                const std::string &dstBucketStr,
                                const std::string &sourceObjectStr,
                                const std::string &dstObjectStr);

    static ErrorCode renameObject(const std::string &accessKeyId,
                                  const std::string &accessKeySecret,
                                  const std::string &endpoint,
                                  const std::string &sourceBucketStr,
                                  const std::string &dstBucketStr,
                                  const std::string &sourceObjectStr,
                                  const std::string &dstObjectStr,
                                  bool hasSubFile);

    static ErrorCode renameObjectRecursive(const std::string &accessKeyId,
                                           const std::string &accessKeySecret,
                                           const std::string &endpoint,
                                           const std::string &sourceBucketStr,
                                           const std::string &dstBucketStr,
                                           const std::string &sourceObjectStr,
                                           const std::string &dstObjectStr,
                                           bool objectIsFile);

    static std::string normalizePath(const std::string &path);
};

typedef std::shared_ptr<OssFileSystem> OssFileSystemPtr;

FSLIB_PLUGIN_END_NAMESPACE(oss);

#endif // FSLIB_PLUGIN_OSSFILESYSTEM_H
