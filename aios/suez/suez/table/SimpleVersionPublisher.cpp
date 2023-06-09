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
#include "suez/table/SimpleVersionPublisher.h"

#include "autil/Log.h"
#include "autil/legacy/fast_jsonizable.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "suez/table/TablePathDefine.h"

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SimpleVersionPublisher);

void SimpleVersionPublisher::remove(const PartitionId &pid) {
    autil::ScopedWriteLock lock(_lock);
    _publishedVersionMap.erase(pid);
}

bool SimpleVersionPublisher::publish(const std::string &root, const PartitionId &pid, const TableVersion &version) {
    if (!needPublish(pid, version)) {
        return false;
    }

    if (doPublish(root, pid, version)) {
        updatePublishedVersion(pid, version);
        return true;
    } else {
        return false;
    }
}

bool SimpleVersionPublisher::getPublishedVersion(const std::string &path,
                                                 const PartitionId &pid,
                                                 IncVersion versionId,
                                                 TableVersion &version) {
    auto filePath = TablePathDefine::constructVersionPublishFilePath(path, pid, versionId);
    std::string content;
    auto status = indexlib::file_system::FslibWrapper::AtomicLoad(filePath, content).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(INFO,
                  "[%s]: version %d load failed [%s]",
                  ToJsonString(pid, true).c_str(),
                  versionId,
                  status.ToString().c_str());
        return false;
    }
    try {
        autil::legacy::FastFromJsonString(version, content);
    } catch (...) { return false; }
    return true;
}

bool SimpleVersionPublisher::needPublish(const PartitionId &pid, const TableVersion &version) const {
    TableVersion published;
    if (!getPublishedVersion(pid, published)) {
        return true;
    }
    if (published.getVersionId() < version.getVersionId()) {
        return true;
    } else {
        if (published.getVersionId() == version.getVersionId() && published != version) {
            AUTIL_LOG(ERROR,
                      "conflict version, published is: %s, to_publish is: %s",
                      ToJsonString(published, true).c_str(),
                      ToJsonString(version, true).c_str());
        }
        return false;
    }
}

bool SimpleVersionPublisher::doPublish(const std::string &path, const PartitionId &pid, const TableVersion &version) {
    auto filePath = TablePathDefine::constructVersionPublishFilePath(path, pid, version.getVersionId());
    auto content = ToJsonString(version, true);
    auto status = indexlib::file_system::FslibWrapper::AtomicStore(filePath, content).Status();
    if (status.IsOK() || status.IsExist()) {
        AUTIL_LOG(INFO,
                  "[%s] publish version [%s] to %s",
                  ToJsonString(pid, true).c_str(),
                  content.c_str(),
                  filePath.c_str());
        return true;
    } else {
        AUTIL_LOG(WARN,
                  "[%s] publish version [%s] to %s failed with error %s",
                  ToJsonString(pid, true).c_str(),
                  content.c_str(),
                  filePath.c_str(),
                  status.ToString().c_str());
        return false;
    }
}

void SimpleVersionPublisher::updatePublishedVersion(const PartitionId &pid, const TableVersion &version) {
    autil::ScopedWriteLock lock(_lock);
    _publishedVersionMap[pid] = version;
}

bool SimpleVersionPublisher::getPublishedVersion(const PartitionId &pid, TableVersion &version) const {
    autil::ScopedReadLock lock(_lock);
    auto it = _publishedVersionMap.find(pid);
    if (it == _publishedVersionMap.end()) {
        return false;
    }
    version = it->second;
    return true;
}

} // namespace suez
