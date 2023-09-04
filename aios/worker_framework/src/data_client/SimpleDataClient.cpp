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
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fslib.h"
#include "worker_framework/DataClient.h"
#include "worker_framework/PathUtil.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

namespace worker_framework {
AUTIL_DECLARE_AND_SETUP_LOGGER(worker_framework.data_client, DataClient);

DataClient::DataClient() {}

DataClient::DataClient(int32_t port) {}

DataClient::~DataClient() {}

bool DataClient::initPort() { return true; }

bool DataClient::startThread() { return true; }

bool DataClient::init() { return true; }

bool DataClient::init(const string &slaveSpec) { return true; }

void DataClient::watch() {}

DataItemPtr DataClient::getData(const std::string &srcBaseUri, const std::string &dstDir, const DataOption &option) {
    vector<string> srcFilePathes{""};
    return getData(srcBaseUri, srcFilePathes, dstDir, option);
}

DataItemPtr DataClient::getData(const std::string &srcBaseUri,
                                const std::vector<std::string> &srcFilePathes,
                                const std::string &dstDir,
                                const DataOption &option) {
    ScopedLock lock(_lock);
    DataItemPtr dataItemPtr(new DataItem(srcBaseUri, dstDir, option));
    dataItemPtr->setStatus(DS_DEPLOYING);
    AUTIL_LOG(INFO, "start getData from %s to %s.", srcBaseUri.c_str(), dstDir.c_str());
    for (auto &fileName : srcFilePathes) {
        std::string srcFile = PathUtil::joinPath(srcBaseUri, fileName);
        std::string dstFile = PathUtil::joinPath(dstDir, fileName);
        if (fslib::EC_TRUE == FileSystem::isExist(dstFile)) {
            if (fslib::EC_OK != FileSystem::remove(dstFile)) {
                dataItemPtr->setStatus(DS_FAILED);
                AUTIL_LOG(INFO, "file already exist, and failed to remove file: [%s]", dstFile.c_str());
                return dataItemPtr;
            }
        }
        auto ec = FileSystem::copy(srcFile, dstFile);
        if (ec != fslib::EC_OK) {
            dataItemPtr->setStatus(DS_FAILED);
            AUTIL_LOG(INFO, "failed to copy file from [%s] to [%s]", srcFile.c_str(), dstFile.c_str());
            return dataItemPtr;
        }
    }
    dataItemPtr->setStatus(DS_FINISHED);
    return dataItemPtr;
}

DataItemPtr DataClient::getData(const std::string &srcBaseUri,
                                const std::vector<DataFileMeta> &srcFileMetas,
                                const std::string &dstDir,
                                const DataOption &option) {
    vector<string> filePathes;
    for (const auto &fileMeta : srcFileMetas) {
        filePathes.push_back(fileMeta.path);
    }
    return getData(srcBaseUri, filePathes, dstDir, option);
}

bool DataClient::queryDataTaskInfoWithDstDirPrefix(const string &dstDirPrefix, vector<DataTaskInfo> &dataTaskInfos) {
    return true;
}

bool DataClient::getEnv(const string &name, string *value) { return true; }

bool DataClient::runInHippoContainer() { return false; }

bool DataClient::redressPath(string *path) { return false; }

bool DataClient::removeData(const std::string &dataPath, bool immediately) {
    if (FileSystem::remove(dataPath) != EC_OK) {
        AUTIL_LOG(ERROR, "remove %s failed.", dataPath.c_str());
        return false;
    }
    return true;
}

}; // namespace worker_framework
