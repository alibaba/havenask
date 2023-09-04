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
#include "autil/FileRecorder.h"

#include <limits.h>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"

using namespace std;
using namespace fslib::fs;

namespace autil {

const size_t FileRecorder::_maxCount = 200;

void FileRecorder::newRecord(const string &content, const string &dirname, const string &suffix, size_t id) {
    char cwdPath[PATH_MAX];
    char *ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        return;
    }
    auto path = string(cwdPath);
    if ('/' != *(path.rbegin())) {
        path += "/";
    }

    string dir = FileSystem::joinFilePath(path, dirname);
    if (fslib::EC_TRUE != FileSystem::isExist(dir)) {
        if (fslib::EC_OK != FileSystem::mkDir(dir)) {
            return;
        }
    }
    vector<string> allfiles;
    if (fslib::EC_OK == FileSystem::listDir(dir, allfiles)) {
        vector<string> records;
        for (size_t i = 0; i < allfiles.size(); i++) {
            if (hasSuffix(allfiles[i], suffix)) {
                records.push_back(allfiles[i]);
            }
        }
        if (records.size() > _maxCount) {
            sort(records.begin(), records.end());
            for (size_t i = 0; i < records.size() - _maxCount; i++) {
                string removeFile = FileSystem::joinFilePath(dir, records[i]);
                FileSystem::remove(removeFile);
            }
        }
    }
    string filename =
        TimeUtility::currentTimeString("%Y-%m-%d-%H-%M-%S") + "_" + StringUtil::toString(id) + string("_") + suffix;
    string finalPath = FileSystem::joinFilePath(dir, filename);
    string formatContent = content;
    FileSystem::writeFile(finalPath, formatContent);
}

bool FileRecorder::hasSuffix(const string &str, const string &suffix) {
    return str.size() >= suffix.size() && str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

} // namespace autil
