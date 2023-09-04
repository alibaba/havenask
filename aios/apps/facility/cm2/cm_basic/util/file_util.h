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
#ifndef __CM_BASIC_FILE_UTIL_H_
#define __CM_BASIC_FILE_UTIL_H_

#include <string>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/common/common.h"
#include "autil/Log.h"

namespace cm_basic {

class FileUtil
{
public:
    FileUtil();
    ~FileUtil();

public:
    static std::string readFile(const std::string& filePath);
    static bool writeFile(const std::string& filePath, const std::string& content);

    static bool fileExist(const std::string& filePath);
    static bool removeFile(const std::string& filePath);
    static bool copyFile(const std::string& desFilePath, const std::string& srcFilePath);
    static bool renameFile(const std::string& s_old_file, const std::string& s_new_file);

    static bool dirExist(const std::string& dirPath);
    static bool makeDir(const std::string& dirPath, bool recursive = false);
    static bool removeDir(const std::string& dirPath, bool removeNoneEmptyDir = false);

    static std::string joinFilePath(const std::string& dir, const std::string& file);
    static std::string normalizeDir(const std::string& dirName);

    static bool appendFile(const std::string& filePath, const std::string& content);

    static bool listDir(const std::string& dirPath, std::vector<std::string>& entryVec, bool fileOnly = false);
    static bool listDir(const std::string& dirPath, std::vector<std::string>& fileVec, std::string& substr);
    static bool listDirByPattern(const std::string& dirPath, std::vector<std::string>& entryVec,
                                 const std::string& pattern, bool fileOnly = false);
    static bool getCurrentPath(std::string& path);
    // for galaxy hash function
    static bool readFromFile(const std::string& srcFileName, std::stringstream& ss);
    static bool readFromFile(const std::string& srcFileName, std::string& readStr);
    static std::string readFileWithLoyalty(const std::string& filePath);
    static void splitFileName(const std::string& input, std::string& folder, std::string& fileName);
    static bool recursiveMakeDir(const std::string& dirPath);
    static bool recursiveRemoveDir(const std::string& dirPath);

    static std::string getParentDir(const std::string& currentDir);
    static char* getRealPath(const char* path, char* rpath);
    static std::string getRealPath(const char* path);

protected:
    static int splitPath(char* path, char** pathes);

private:
    static const char DIR_DELIM = '/';

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_basic

#endif // ISEARCH_FILEUTIL_H
