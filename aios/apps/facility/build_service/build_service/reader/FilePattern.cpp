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
#include "build_service/reader/FilePattern.h"

#include "autil/StringUtil.h"
#include "build_service/util/RangeUtil.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::util;
namespace build_service { namespace reader {

BS_LOG_SETUP(reader, FilePattern);

FilePattern::FilePattern(const string& dirPath, const string& prefix, const string& suffix, uint32_t fileIdCount)
    : FilePatternBase(dirPath)
    , _prefix(prefix)
    , _suffix(suffix)
    , _fileCount(fileIdCount)
{
}

FilePattern::~FilePattern() {}

CollectResults FilePattern::calculateHashIds(const CollectResults& fileList) const
{
    CollectResults results;
    for (size_t i = 0; i < fileList.size(); ++i) {
        vector<string> dirs = StringUtil::split(fileList[i]._fileName, "/", true);
        string fileName = dirs.back();

        uint32_t fileId;
        if (!extractFileId(fileName, fileId)) {
            continue;
        }
        if (fileId >= _fileCount) {
            continue;
        }

        // match file pattern
        dirs.pop_back();
        string pathDir = StringUtil::toString(dirs, "/");
        string expectedPathDir = fslib::util::FileUtil::joinFilePath(_rootPath, _relativePath);
        dirs = StringUtil::split(expectedPathDir, "/", true);
        expectedPathDir = StringUtil::toString(dirs, "/");
        if (expectedPathDir != pathDir) {
            continue;
        }

        CollectResult now = fileList[i];
        now._rangeId = RangeUtil::getRangeId(fileId, _fileCount);
        results.push_back(now);
    }

    return results;
}

bool FilePattern::operator==(const FilePattern& other) const
{
    return _relativePath == other._relativePath && _prefix == other._prefix && _suffix == other._suffix &&
           _fileCount == other._fileCount && _rootPath == other._rootPath;
}

bool FilePattern::extractFileId(const string& fileName, uint32_t& fileId) const
{
    size_t prefixLen = _prefix.length();
    size_t suffixLen = _suffix.length();
    size_t len = fileName.length();
    if (len <= prefixLen + suffixLen) {
        return false;
    }
    if (fileName.substr(0, prefixLen) != _prefix || fileName.substr(len - suffixLen, suffixLen) != _suffix) {
        return false;
    }
    string idStr = fileName.substr(prefixLen, len - suffixLen - prefixLen);
    if (!StringUtil::strToUInt32(idStr.c_str(), fileId)) {
        return false;
    }
    return true;
}

}} // namespace build_service::reader
