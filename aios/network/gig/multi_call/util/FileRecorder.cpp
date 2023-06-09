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
#include "aios/network/gig/multi_call/util/FileRecorder.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/jsonizable.h"
#include <dirent.h>
#include <fstream>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, FileRecorder);

FileRecorder::FileRecorder() {}

FileRecorder::~FileRecorder() {}

void FileRecorder::recordSnapshot(const string &content, size_t logCount,
                                  const string &prefix) {
    newRecord(content, logCount, prefix, "snapshot");
}

bool FileRecorder::checkAndCreateDir(const string &dir) {
    struct stat buf;
    if (stat(dir.c_str(), &buf) == 0) {
        if ((buf.st_mode & S_IFDIR) == S_IFDIR) {
            return true;
        } else if (!removeFile(dir)) {
            return false;
        }
    } else if (errno != ENOENT) {
        AUTIL_LOG(ERROR, "Failed to stat [%s], error [%s]", dir.c_str(),
                  strerror(errno));
        return false;
    }
    AUTIL_LOG(DEBUG, "mkdir [%s]", dir.c_str());
    if (mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IRWXO)) {
        AUTIL_LOG(ERROR, "Failed to mkdir [%s], error [%s]", dir.c_str(),
                  strerror(errno));
        return false;
    }
    return true;
}

void FileRecorder::newRecord(const string &content, size_t logCount,
                             const string &dirname, const string &suffix) {
    string cwd;
    if (!getCurrentPath(cwd)) {
        return;
    }

    string dir = cwd;
    auto subDirs = autil::StringUtil::split(dirname, "/");
    for (const auto &subDir : subDirs) {
        dir = dir + "/" + subDir;
        if (!checkAndCreateDir(dir)) {
            return;
        }
    }

    vector<string> allfiles;
    if (listDir(dir, allfiles)) {
        vector<string> records;
        for (size_t i = 0; i < allfiles.size(); i++) {
            if (hasSuffix(allfiles[i], suffix)) {
                records.push_back(allfiles[i]);
            }
        }
        if (records.size() > logCount) {
            sort(records.begin(), records.end());
            for (size_t i = 0; i < records.size() - logCount; i++) {
                string removeFileName = join(dir, records[i]);
                removeFile(removeFileName);
            }
        }
    }
    int64_t usec = autil::TimeUtility::currentTime() % 1000000;
    string filename =
        autil::TimeUtility::currentTimeString("%Y-%m-%d-%H-%M-%S") + "." +
        autil::StringUtil::toString(usec) + "_" + suffix;
    string path = join(dir, filename);
    writeFile(path, content);
}

bool FileRecorder::hasSuffix(const string &str, const string &suffix) {
    return str.size() >= suffix.size() &&
           str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool FileRecorder::getCurrentPath(string &path) {
    char cwdPath[PATH_MAX];
    char *ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        AUTIL_LOG(ERROR, "Failed to get current work directory");
        return false;
    }
    path = string(cwdPath);
    if ('/' != *(path.rbegin())) {
        path += "/";
    }
    return true;
}

string FileRecorder::join(const string &path, const string &file) {
    if (path.empty()) {
        return file;
    }

    if (path[path.length() - 1] == '/') {
        return path + file;
    }

    return path + '/' + file;
}

bool FileRecorder::listDir(const string &dirName, vector<string> &fileList) {
    fileList.clear();

    DIR *dp;
    struct dirent *ep;
    dp = opendir(dirName.c_str());
    if (dp == NULL) {
        AUTIL_LOG(ERROR, "open dir %s fail, %s", dirName.c_str(),
                  strerror(errno));
        return false;
    }

    while ((ep = readdir(dp)) != NULL) {
        if (strcmp(ep->d_name, ".") == 0 || strcmp(ep->d_name, "..") == 0) {
            continue;
        }
        fileList.push_back(ep->d_name);
    }
    if (closedir(dp) < 0) {
        AUTIL_LOG(ERROR, "close dir %s fail, %s", dirName.c_str(),
                  strerror(errno));
        return false;
    }

    return true;
}

bool FileRecorder::removeFile(const string &path) {
    if (remove(path.c_str()) != 0) {
        AUTIL_LOG(ERROR, "remove file [%s] failed, error [%s]", path.c_str(),
                  strerror(errno));
        return false;
    }
    return true;
}

bool FileRecorder::writeFile(const string &srcFileName, const string &content) {
    ofstream ofile(srcFileName);
    if (!ofile.good()) {
        AUTIL_LOG(ERROR, "write [%s] to file [%s] failed", content.c_str(),
                  srcFileName.c_str());
        return false;
    }
    ofile << content;
    ofile.close();
    return true;
}

} // namespace multi_call
