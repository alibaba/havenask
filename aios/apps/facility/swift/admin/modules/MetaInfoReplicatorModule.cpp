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
#include "swift/admin/modules/MetaInfoReplicatorModule.h"

#include <functional>
#include <unistd.h>
#include <utility>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "swift/admin/SysController.h"
#include "swift/common/Common.h"
#include "swift/common/CreatorRegistry.h"
#include "swift/common/PathDefine.h"
#include "swift/config/AdminConfig.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/ZkDataAccessor.h"
#include "zookeeper/zookeeper.h"

namespace swift {
namespace admin {

using namespace autil;
using namespace swift::util;
using namespace swift::common;
using namespace swift::protocol;
using namespace fslib::fs;

AUTIL_LOG_SETUP(swift, MetaInfoReplicatorModule);

MetaInfoReplicatorModule::MetaInfoReplicatorModule() : _stopped(true), _lastDoneTimestamp(-1) {}

MetaInfoReplicatorModule::~MetaInfoReplicatorModule() { doUnload(); }

bool MetaInfoReplicatorModule::doInit() {
    _selfZkRoot = _adminConfig->getZkRoot();
    _selfZkPath = PathDefine::getPathFromZkPath(_selfZkRoot);
    _mirrorZkRoot = _adminConfig->getMirrorZkRoot();
    _mirrorZkPath = PathDefine::getPathFromZkPath(_mirrorZkRoot);
    _channelManager.reset(new network::SwiftRpcChannelManager());
    if (!_channelManager->init(nullptr)) {
        AUTIL_LOG(ERROR, "init swift rpc channel manager failed.");
        return false;
    }
    _adminAdapter.reset(new network::SwiftAdminAdapter(_adminConfig->getMirrorZkRoot(), _channelManager, false));
    return true;
}

bool MetaInfoReplicatorModule::doLoad() {
    ScopedLock lock(_lock);
    _selfZkAccessor = std::make_unique<ZkDataAccessor>();
    if (!_selfZkAccessor->init(_selfZkRoot)) {
        AUTIL_LOG(ERROR, "init zk accessor failed, zk root[%s]", _selfZkRoot.c_str());
        return false;
    }
    _mirrorZkAccessor = std::make_unique<ZkDataAccessor>();
    if (!_mirrorZkAccessor->init(_mirrorZkRoot)) {
        AUTIL_LOG(ERROR, "init zk accessor failed, zk root[%s]", _mirrorZkRoot.c_str());
        return false;
    }
    if (!_stopped) {
        AUTIL_LOG(INFO, "MetaInfoReplicatorModule has already start");
        return true;
    }
    _loopThread = LoopThread::createLoopThread(std::bind(&MetaInfoReplicatorModule::workLoop, this),
                                               _adminConfig->getMetaInfoReplicateInterval(),
                                               "meta_replicator");
    if (!_loopThread) {
        AUTIL_LOG(ERROR, "create replicator meta info thread failed");
        return false;
    }
    _stopped = false;
    return true;
}

void MetaInfoReplicatorModule::workLoop() {
    if (_stopped) {
        AUTIL_LOG(WARN, "meta info replicator stopped");
        return;
    }
    if (!checkReplicator()) {
        AUTIL_LOG(WARN, "meta info replicator check failed");
        usleep(500 * 1000);
        return;
    }
    if (!syncTopicAclData()) {
        AUTIL_LOG(WARN, "sync topic acl data failed");
        return;
    }
    if (!syncTopicMeta()) {
        AUTIL_LOG(WARN, "sync topic meta failed");
        return;
    }
    if (!syncTopicSchemas()) {
        AUTIL_LOG(WARN, "sync topic schema failed");
        return;
    }
    if (!syncWriterVersion()) {
        AUTIL_LOG(WARN, "sync writer version failed");
        return;
    }
    _lastDoneTimestamp = TimeUtility::currentTime();
}

bool MetaInfoReplicatorModule::checkReplicator() {
    if (isMaster()) {
        AUTIL_LOG(WARN, "current cluster is master");
        return false;
    }
    EmptyRequest request;
    MasterInfoResponse response;
    auto ec = _adminAdapter->getMasterInfo(request, response);
    if (ec != ERROR_NONE) {
        AUTIL_LOG(WARN, "get peer cluster master info failed, error[%d]", ec);
        return false;
    }
    if (ERROR_NONE != response.errorinfo().errcode()) {
        AUTIL_LOG(WARN, "get peer cluster master info failed, response[%s]", response.DebugString().c_str());
        return false;
    }
    if (!response.ismaster()) {
        AUTIL_LOG(INFO, "peer cluster is not master, response[%s]", response.DebugString().c_str());
        return false;
    }
    if (response.masterversion() <= _sysController->getMasterVersion()) {
        AUTIL_LOG(INFO,
                  "peer cluster master version is less than self version, self version[%lu] response[%s]",
                  _sysController->getMasterVersion(),
                  response.DebugString().c_str());
        return false;
    }
    return true;
}

bool MetaInfoReplicatorModule::syncTopicAclData() {
    std::string topicAclData;
    std::string mirrorPath = PathDefine::getTopicAclDataFile(_mirrorZkPath);
    if (!_mirrorZkAccessor->getData(mirrorPath, topicAclData)) {
        AUTIL_LOG(WARN, "read mirror swift topic acl data failed. mirror cluster[%s]", _mirrorZkPath.c_str());
        return false;
    }
    if (topicAclData != _topicAclData) {
        std::string selfPath = PathDefine::getTopicAclDataFile(_selfZkPath);
        if (!_selfZkAccessor->writeFile(selfPath, topicAclData)) {
            AUTIL_LOG(WARN, "write self swift topic acl data failed. self cluster[%s]", _selfZkPath.c_str());
            return false;
        }
        AUTIL_LOG(INFO,
                  "sync topic acl data from mirror cluster, self path[%s] mirror path[%s] data size[%lu]",
                  _selfZkPath.c_str(),
                  _mirrorZkPath.c_str(),
                  topicAclData.size());
        _topicAclData = topicAclData;
    }
    return true;
}

bool MetaInfoReplicatorModule::syncTopicMeta() {
    std::string topicMeta;
    std::string mirrorPath = PathDefine::getTopicMetaFile(_mirrorZkPath);
    if (!_mirrorZkAccessor->getData(mirrorPath, topicMeta)) {
        AUTIL_LOG(WARN, "read mirror swift meta info failed. mirror cluster[%s]", _mirrorZkPath.c_str());
        return false;
    }
    if (topicMeta != _topicMeta) {
        std::string selfPath = PathDefine::getTopicMetaFile(_selfZkPath);
        if (!_selfZkAccessor->writeFile(selfPath, topicMeta)) {
            AUTIL_LOG(WARN, "write self swift meta info failed. self cluster[%s]", _selfZkPath.c_str());
            return false;
        }
        AUTIL_LOG(INFO,
                  "sync topic meta from mirror cluster, self path[%s] mirror path[%s] meta size[%lu]",
                  _selfZkPath.c_str(),
                  _mirrorZkPath.c_str(),
                  topicMeta.size());
        _topicMeta = topicMeta;
    }
    return true;
}

bool MetaInfoReplicatorModule::syncTopicSchemas() {
    bool exist = false;
    std::string path = PathDefine::getSchemaDir(_mirrorZkPath);
    auto zkWrapper = _mirrorZkAccessor->getZkWrapper();
    if (!zkWrapper) {
        AUTIL_LOG(WARN, "mirror zk wrapper empty");
        return false;
    }
    if (!zkWrapper->check(path, exist)) {
        AUTIL_LOG(WARN, "fail to check schema dir[%s]", path.c_str());
        return false;
    }
    if (!exist) {
        AUTIL_LOG(WARN, "schema dir[%s] not exist", path.c_str());
        return true;
    }
    std::vector<std::string> topics;
    ZOO_ERRORS ec = zkWrapper->getChild(path, topics);
    if (ZOK != ec) {
        AUTIL_LOG(WARN, "Failed to list dir %s.", path.c_str());
        return false;
    }
    for (const auto &topic : topics) {
        std::string schema;
        auto mirrorPath = PathDefine::getTopicSchemaFile(_mirrorZkPath, topic);
        auto selfPath = PathDefine::getTopicSchemaFile(_selfZkPath, topic);
        if (!_mirrorZkAccessor->getData(mirrorPath, schema)) {
            AUTIL_LOG(WARN, "read mirror swift topic schema failed. topic[%s]", topic.c_str());
            return false;
        }
        auto it = _topicSchemas.find(topic);
        if (it == _topicSchemas.end() || it->second != schema) {
            if (!_selfZkAccessor->writeFile(selfPath, schema)) {
                AUTIL_LOG(WARN, "write self swift topic schema failed. topic[%s]", topic.c_str());
                return false;
            }
            _topicSchemas[topic] = schema;
        }
    }
    return true;
}

bool MetaInfoReplicatorModule::syncWriterVersion() {
    bool exist = false;
    std::string path = PathDefine::getWriteVersionControlPath(_mirrorZkPath);
    auto zkWrapper = _mirrorZkAccessor->getZkWrapper();
    if (!zkWrapper) {
        AUTIL_LOG(WARN, "mirror zk wrapper empty");
        return false;
    }
    if (!zkWrapper->check(path, exist)) {
        AUTIL_LOG(WARN, "fail to check writer version dir[%s]", path.c_str());
        return false;
    }
    if (!exist) {
        AUTIL_LOG(DEBUG, "writer version path[%s] not exist", path.c_str());
        return true;
    }
    std::vector<std::string> topics;
    ZOO_ERRORS ec = zkWrapper->getChild(path, topics);
    if (ZOK != ec) {
        AUTIL_LOG(WARN, "Failed to list dir %s.", path.c_str());
        return false;
    }
    for (const auto &topic : topics) {
        std::string schema;
        auto mirrorPath = FileSystem::joinFilePath(path, topic);
        if (!_mirrorZkAccessor->getData(mirrorPath, schema)) {
            AUTIL_LOG(WARN, "read mirror swift writer version failed. topic[%s]", topic.c_str());
            return false;
        }
        auto selfPath = FileSystem::joinFilePath(PathDefine::getWriteVersionControlPath(_selfZkPath), topic);
        auto it = _writerVersion.find(topic);
        if (it == _writerVersion.end() || it->second != schema) {
            if (!_selfZkAccessor->writeFile(selfPath, schema)) {
                AUTIL_LOG(WARN, "write self swift writer version failed. topic[%s]", topic.c_str());
                return false;
            }
            AUTIL_LOG(INFO, "write self swift writer version. topic[%s]", topic.c_str());
            _writerVersion[topic] = schema;
        }
    }
    return true;
}

bool MetaInfoReplicatorModule::doUnload() {
    ScopedLock lock(_lock);
    if (_stopped) {
        AUTIL_LOG(INFO, "MetaInfoReplicatorModule has already stop");
        return true;
    }
    AUTIL_LOG(INFO, "replicator last done time[%ld]", _lastDoneTimestamp.load());
    if (_loopThread) {
        _loopThread->stop();
    }
    _stopped = true;
    return true;
}

REGISTER_MODULE(MetaInfoReplicatorModule, "S", "L");
} // namespace admin
;
} // namespace swift
