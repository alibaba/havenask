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
#ifndef FSLIB_FSUTIL_H
#define FSLIB_FSUTIL_H

#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/cipher/AESCipherCommon.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileReadWriteLock.h"

FSLIB_BEGIN_NAMESPACE(tools);

class FsUtil {
public:
    static std::string GETMETA_CMD;
    static std::string COPY_CMD;
    static std::string MD5_SUM_CMD;
    static std::string ENCRYPT_CMD;
    static std::string DECRYPT_CMD;
    static std::string DECRYPT_CAT_CMD;
    static std::string DECRYPT_ZCAT_CMD;
    static std::string MOVE_CMD;
    static std::string MKDIR_CMD;
    static std::string LISTDIR_CMD;
    static std::string REMOVE_CMD;
    static std::string CAT_CMD;
    static std::string ZCAT_CMD;
    static std::string FLOCK_CMD;
    static std::string ISEXIST_CMD;
    static std::string RENAME_CMD;
    static std::string FORWARD_CMD;
    static const int32_t DEFAULT_THREAD_NUM;

public:
    ~FsUtil();
    static void closeFileSystem();

public:
    static ErrorCode runCopy(const char *srcPath, const char *dstPath, bool recursive);
    static ErrorCode runMove(const char *srcPath, const char *dstPath);
    static ErrorCode runRename(const char *srcPath, const char *dstPath);
    static ErrorCode runMkDir(const char *dirname, bool recursive);
    static ErrorCode runListDir(const char *dirname);
    static ErrorCode runListDirWithRecursive(const char *dirname, const char *threadNumStr);
    static ErrorCode runRemove(const char *path);
    static ErrorCode runMd5Sum(const char *filename);
    static ErrorCode runCat(const char *filename);
    static ErrorCode runZCat(const char *filename);
    static ErrorCode runGetMeta(const char *pathname);
    static ErrorCode runIsExist(const char *pathname);
    static ErrorCode runFlock(const char *lockname, const char *script);
    static ErrorCode runForward(const char *command, const char *pathname, const char *args);

    static ErrorCode runEncrypt(const char *srcPath, const char *dstPath, const char *option, bool optionUseBase64);
    static ErrorCode runDecrypt(const char *srcPath, const char *dstPath, const char *option, bool optionUseBase64);
    static ErrorCode runDecryptCat(const char *srcPath, const char *option, bool optionUseBase64);
    static ErrorCode runDecryptZCat(const char *srcPath, const char *option, bool optionUseBase64);

private:
    static void timeToStr(uint64_t time, char *timestr);
    static ErrorCode getDirMeta(const std::string &dirname, uint32_t &fileCount, uint32_t &dirCount, uint64_t &size);
    static ErrorCode getPathMeta(const std::string &pathname, FileMeta &meta);
    static ErrorCode getZookeeperNodeMeta(const std::string &pathName, uint32_t &totalNodeCount, uint64_t &size);
    static bool getCipherOption(autil::cipher::CipherOption &option);

    template <typename T>
    static bool getUserConsoleInput(const std::string &hintStr,
                                    const std::set<std::string> &validInputs,
                                    const std::string &defaultValue,
                                    size_t maxInputTimes,
                                    T &value);
};

FSLIB_TYPEDEF_AUTO_PTR(FsUtil);

FSLIB_END_NAMESPACE(tools);

#endif // FSLIB_FSUTIL_H
