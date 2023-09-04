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
#include "common/LocalState.h"
#include "util/PathUtil.h"
#include "fslib/util/FileUtil.h"
#include "autil/TimeUtility.h"
#include "autil/StringUtil.h"
#include <algorithm>
#include <fstream>

using namespace std;
using namespace autil;
USE_HIPPO_NAMESPACE(util);


BEGIN_HIPPO_NAMESPACE(common);

HIPPO_LOG_SETUP(common, LocalState);

void LocalStateReclaimItem::process() {
    vector<string> entries;
    if (!fslib::util::FileUtil::listDir(_basePath, entries)) {
        return;
    }
    auto keepCount = _fileName == RESOURCE_MANAGER_SERIALIZE_FILE_NEW ? 1000 : 50;
    vector<string> targets;
    string pattern = _fileName + DOT_SEPARATOR;
    for (auto e : entries) {
        if (e.find(pattern) == 0 ) {
            targets.push_back(e);
        }
    }
    if (targets.size() < (size_t)keepCount) {
        return;
    }
    std::sort(begin(targets), end(targets));
    int32_t end = targets.size() - keepCount;
    for (auto i = 0; i < end; ++i) {
        fslib::util::FileUtil::removeFile(fslib::util::FileUtil::joinPath(_basePath, targets.at(i)));
    }
}

LocalState::LocalState(const string &basePath)
    : _basePath(basePath)
{
    _bgReclaimThreadPool = autil::ThreadPoolPtr(new autil::ThreadPool(1, 100));
    _bgReclaimThreadPool->start();
}

LocalState::~LocalState() {
    _bgReclaimThreadPool->stop();
}

bool LocalState::read(const string &fileName, string &content) const {
    string fullPath = fslib::util::FileUtil::joinPath(_basePath, fileName);
    ifstream ifs(fullPath.c_str(), ios::binary);
    if (!ifs) {
        HIPPO_LOG(ERROR, "open file:%s failed", fullPath.c_str());
        return false;
    }
    
    ifs.seekg(0, ifs.end);
    uint64_t fileLength = ifs.tellg();
    ifs.seekg(0, ifs.beg);
    char *buffer = new char[fileLength];
    ifs.read(buffer, fileLength);
    if (!ifs) {
        HIPPO_LOG(ERROR, "read file:%s failed", fullPath.c_str());
        ifs.close();
        delete[] buffer;
        return false;
    }
    
    content.assign(buffer, fileLength);
    delete[] buffer;
    return true;
}
    
bool LocalState::write(const string &fileName, const string &content) {
    string fullPath = fslib::util::FileUtil::joinPath(_basePath, fileName);
    if (fslib::util::FileUtil::isExist(fullPath)) {
        string backupPath = fullPath + DOT_SEPARATOR +
                            StringUtil::toString(TimeUtility::currentTime());
        fslib::util::FileUtil::mv(fullPath, backupPath);
    }
    if (!fslib::util::FileUtil::writeFileLocalSafe(fullPath, content)) {
        HIPPO_LOG(ERROR, "write locat state file %s failed", fullPath.c_str());
        return false;
    }
    string basePath = _basePath;
    string reclaimFile = fileName;
    if (StringUtil::startsWith(fileName, "federation/")) {
        auto e = StringUtil::split(fileName, "/");
        assert(e.size() > 2);
        string subPath;
        for (size_t i = 0; i < e.size() - 1; i++) {
            subPath += e[i] + "/";
        }
        basePath = fslib::util::FileUtil::joinPath(_basePath, subPath);
        reclaimFile = *e.rbegin();
        if (StringUtil::startsWith(reclaimFile, FEDERATION_NATIVE_RESOURCEPLAN_PATCH_FILE_PREFIX)){
            reclaimFile = "resource_plan_patch";
        }
    }
    LocalStateReclaimItem* item = new LocalStateReclaimItem(basePath, reclaimFile);
    autil::ThreadPool::ERROR_TYPE ret = _bgReclaimThreadPool->pushWorkItem(
            item, false);
    if (ret != autil::ThreadPool::ERROR_NONE) {
        HIPPO_LOG(WARN, "bgReclaimThreadPool pushWorkItem error %d", ret);
        delete item;
    }
    return true;
}

bool LocalState::check(const std::string &fileName, bool &bExist) {
    string path = fslib::util::FileUtil::joinPath(_basePath, fileName);
    if (fslib::util::FileUtil::isFileExist(path) || fslib::util::FileUtil::isPathExist(path)) {
        bExist = true;
    } else {
        HIPPO_LOG(DEBUG, "Check local path [%s] isExist failed.", path.c_str());
        bExist = false;
    }
    return true;
}

bool LocalState::remove(const std::string &path) {
    bool bExist;
    if (!check(path, bExist)) {
        return false;
    }
    if (!bExist) {
        return true;
    }
    string absPath = fslib::util::FileUtil::joinPath(_basePath, path);
    if (!fslib::util::FileUtil::removePath(absPath)) {
        HIPPO_LOG(WARN, "remove local path [%s] failed.", absPath.c_str());
        return false;
    }
    return true;
}


END_HIPPO_NAMESPACE(common);

