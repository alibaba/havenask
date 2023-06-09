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

#pragma once

#include <map>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "fslib/common/common_define.h"
#include "fslib/fslib.h"

#define HOBBIT_PREFIX "HOBBIT_PREFIX"

FSLIB_BEGIN_NAMESPACE(util);

class FileUtil {
public:
    static std::string readFile(const std::string &file);
    static bool readFile(const std::string &file, std::string &content);
    static bool readFile(const std::string &file, std::string &content, bool *isNotExist);
    static bool writeFile(const std::string &file, const std::string &content);

    static bool mkDir(const std::string &dirPath, bool recursive = false);
    static bool createPath(const std::string &dirPath);
    static bool createDir(const std::string &dirPath);
    static bool removeFile(const std::string &path, bool force = false);
    static bool removePath(const std::string &filePath);
    static bool remove(const std::string &path, bool force);
    static bool remove(const std::string &path);
    static bool removeIfExist(const std::string &path);
    static std::string dirName(const std::string &path);
    static int64_t getCgroupValue(const std::string &path);
    static bool mv(const std::string &srcPath, const std::string &dstPath);
    static bool listDir(const std::string &path, std::vector<std::string> &fileList);
    static bool listDirRecursive(const std::string &path, std::vector<std::string> &entryVec, bool fileOnly);
    static bool listDirWithAbsolutePath(const std::string &path, std::vector<std::string> &entryVec, bool fileOnly);
    static bool
    listDirRecursiveWithAbsolutePath(const std::string &path, std::vector<std::string> &entryVec, bool fileOnly);
    static bool writeFileLocalSafe(const std::string &filePath, const std::string &content);
    static bool isDir(const std::string &path, bool &dirExist);
    static bool isDir(const std::string &path);

    static bool isFile(const std::string &path, bool &fileExist);
    static bool isFile(const std::string &path);

    static bool isExist(const std::string &path, bool &exist);
    static bool isExist(const std::string &path);

    static bool isFileExist(const std::string &filePath);
    static bool isPathExist(const std::string &filePath);

    static bool makeSureParentDirExist(const std::string &fileName);
    static std::string joinFilePath(const std::string &dir, const std::string &file);
    static std::string joinPath(const std::string &path, const std::string &subPath);
    static bool pathJoin(const std::string &basePath, const std::string &fileName, std::string &fullFileName);
    static std::string normalizeDir(const std::string &dirName);
    static bool normalizeFile(const std::string &srcFileName, std::string &dstFileName);
    static std::string normalizePath(const std::string &path);
    static std::string normalizePath(std::string &path);
    static void splitFileName(const std::string &input, std::string &folder, std::string &fileName);
    static std::string getParentDir(const std::string &dir);
    static std::string parentPath(const std::string &path);
    // zookeeper
    static std::string getHostFromZkPath(const std::string &zkPath);
    static std::string getPathFromZkPath(const std::string &zkPath);

    static bool getFileLength(const std::string &filePath, int64_t &fileLength, bool needPrintLog = true);

    static bool readWithBak(const std::string filepath, std::string &content);
    static bool writeWithBak(const std::string filepath, const std::string &content);
    static bool atomicCopy(const std::string &src, const std::string &file);
    static bool listDir(const std::string &path, std::vector<std::string> &entryVec, bool fileOnly, bool recursive);
    static std::string firstLineFromShell(const std::string &cmd);
    static bool changeCurrentPath(const std::string &path);
    static bool getCurrentPath(std::string &path);
    static std::string baseName(const std::string &path);
    static bool
    getFileNames(const std::string &path, const std::string &extension, std::vector<std::string> &fileNames);
    static bool getDirMeta(const std::string &pathName, uint32_t &fileCount, uint32_t &dirCount, uint64_t &size);
    static bool getDirNames(const std::string &path, std::vector<std::string> &dirNames);

    static bool localLinkCopy(const std::string &src, const std::string &dest);
    static bool getFsUsage(const std::string &path, uint32_t *usage);
    static std::string getFileMd5(const std::string &filePath);
    static bool getRealPath(const std::string &path, std::string &real_path);

    static int32_t call(const std::string &command, std::string *out, int32_t timeout = 300);
    static void readStream(FILE *stream, std::string *pattern, int32_t timeout);
    static std::string getTmpFilePath(const std::string &filePath);
    static bool isLocalFile(const std::string &basePath);
    static bool isHttpFile(const std::string &basePath);
    static bool isHdfsFile(const std::string &basePath);
    static bool isZfsFile(const std::string &basePath);
    static bool isPanguFile(const std::string &basePath);
    static bool isOssFile(const std::string &basePath);
    static bool isDfsFile(const std::string &basePath);
    static bool isDcacheFile(const std::string &basePath);
    static bool renameFile(const std::string &s_old_file, const std::string &s_new_file);
    static bool makeSureNotExist(const std::string &path);
    static bool makeSureNotExistLocal(const std::string &path);
    static bool listSubDirs(const std::string &dir, std::vector<std::string> *subDirs);

    static bool listSubFiles(const std::string &dir, std::map<std::string, std::string> &subFiles);

    static bool resolveSymbolLink(const std::string &path, std::string &realPath);

    static bool readLinkRecursivly(std::string &path);

    static bool isLink(const std::string &path);

    static std::string readLink(const std::string &linkPath);

    static bool listLinks(const std::string &linkDir, std::vector<std::string> *linkNames);
    static bool matchPattern(const std::string &path, const std::string &pattern);

    static bool createLink(const std::string &srcFilePath, const std::string &target, bool isSymlink = false);
    static bool recoverFromFile(const std::string &filePath, std::string *content);
    // diskSize in MiB
    static bool getDiskSize(const std::string &path, int32_t *diskSize);

    static bool isSubDir(const std::string &parent, const std::string &child);
    static std::string getVolumePath(const std::string &appWorkDir, const int32_t slotId);
    static bool copyAnyFileToLocal(const std::string &src, const std::string &dest);
    static uint32_t getQuotaId(const std::string &quotaFile);
    static bool isFileContentDifference(const std::string &srcFile, const std::string &destFile);

private:
    static const std::string ZFS_PREFIX;
    static const std::size_t ZFS_PREFIX_LEN;
    static const std::string TMP_SUFFIX;
    static const std::string BAK_SUFFIX;

private:
    FileUtil();
    ~FileUtil();

private:
    AUTIL_LOG_DECLARE();
};
FSLIB_END_NAMESPACE(util);
