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
#include "swift/admin/modules/NoUseTopicModule.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <stddef.h>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/admin/SysController.h"
#include "swift/admin/TopicInStatusManager.h"
#include "swift/admin/TopicInfo.h"
#include "swift/admin/TopicTable.h"
#include "swift/common/CreatorRegistry.h"
#include "swift/config/AdminConfig.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

using namespace autil;
using namespace swift::protocol;

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, NoUseTopicModule);

NoUseTopicModule::NoUseTopicModule() {}

NoUseTopicModule::~NoUseTopicModule() {}

bool NoUseTopicModule::doInit() {
    if (!initAdminClient()) {
        AUTIL_LOG(ERROR, "admin client init fail");
        return false;
    }
    _zkDataAccessor = _sysController->getAdminZkDataAccessor();
    _topicInStatusManager = _sysController->getTopicInStatusManager();
    _topicTable = _sysController->getTopicTable();
    return true;
}

bool NoUseTopicModule::doLoad() {
    _noUseTopicInfo.clear();
    int64_t interval = _adminConfig->getLongPeriodIntervalSec() * 1000 * 1000;
    _deleteThread =
        LoopThread::createLoopThread(std::bind(&NoUseTopicModule::deleteNoUseTopic, this), interval, "del_nouse_topic");
    if (!_deleteThread) {
        AUTIL_LOG(ERROR, "create loop thread fail");
        return false;
    } else {
        AUTIL_LOG(INFO, "create loop thread success, loop interval[%ld us]", interval);
    }

    if (0 >= _adminConfig->getMissTopicRecoverIntervalSec()) {
        return true;
    }
    interval = int64_t(_adminConfig->getMissTopicRecoverIntervalSec()) * 1000 * 1000;
    _recoverThread = LoopThread::createLoopThread(
        std::bind(&NoUseTopicModule::recoverMissTopics, this), interval, "recover_miss_topic");
    if (!_recoverThread) {
        AUTIL_LOG(ERROR, "create recover miss topic thread fail");
        return false;
    } else {
        AUTIL_LOG(INFO,
                  "create recover miss topic thread success, "
                  "loop interval[%ld us]",
                  interval);
    }
    loadLastDeletedNoUseTopics();
    return true;
}

bool NoUseTopicModule::doUnload() {
    if (_deleteThread) {
        _deleteThread->stop();
    }
    if (_recoverThread) {
        _recoverThread->stop();
    }
    return true;
}

bool NoUseTopicModule::loadLastDeletedNoUseTopics() {
    TopicMetas metas;
    if (!_zkDataAccessor->loadLastDeletedNoUseTopics(metas)) {
        AUTIL_LOG(ERROR, "load deleted not use topics failed");
        return false;
    }
    _noUseTopicInfo.setLastNoUseTopics(metas);
    return true;
}

void NoUseTopicModule::getLastNoUseTopicsMeta(protocol::LastDeletedNoUseTopicResponse *response) {
    _noUseTopicInfo.getLastNoUseTopicsMeta(response);
}

void NoUseTopicModule::insertNotExistTopics(const std::string &topic) { _noUseTopicInfo.insertNotExistTopics(topic); }

bool NoUseTopicModule::initAdminClient() {
    _channelManager.reset(new network::SwiftRpcChannelManager());
    if (!_channelManager->init(NULL)) {
        AUTIL_LOG(ERROR, "init swift rpc channel manager failed.");
        return false;
    }
    _adminAdapter.reset(new network::SwiftAdminAdapter(_adminConfig->getZkRoot(), _channelManager, false));
    return true;
}

void NoUseTopicModule::deleteNoUseTopic() {
    if (!isLeader()) {
        return;
    }
    uint32_t topicExpireSec = _adminConfig->getNoUseTopicExpireTimeSec();
    if (0 >= topicExpireSec) {
        return;
    }
    std::string lastFileName;
    if (!_zkDataAccessor->getLastDeletedNoUseTopicFileName(lastFileName)) {
        return;
    }
    if (!lastFileName.empty()) {
        uint32_t lastDeleteTime = 0;
        if (!StringUtil::fromString(lastFileName, lastDeleteTime)) {
            AUTIL_LOG(ERROR, "convert [%s] to uint32 fail", lastFileName.c_str());
            return;
        }
        uint32_t curTime = TimeUtility::currentTimeInSeconds();
        uint32_t noUseTopicDeleteInterval = _adminConfig->getNoUseTopicDeleteIntervalSec();
        if (curTime - lastDeleteTime < noUseTopicDeleteInterval) {
            AUTIL_LOG(INFO,
                      "accident delete, ignore. curTime:%d, last delete time:%d,"
                      " delete interval:%d > %d",
                      curTime,
                      lastDeleteTime,
                      noUseTopicDeleteInterval,
                      curTime - lastDeleteTime);
            return;
        }
    }

    TopicRWInfos rwInfos;
    _topicInStatusManager->getTopicWriteReadTime(std::string(), rwInfos);
    if (0 == rwInfos.infos_size()) {
        return;
    }
    AUTIL_LOG(INFO, "begin delete no use topic, topic size[%d], expire sec[%d]", rwInfos.infos_size(), topicExpireSec);
    uint32_t curTime = TimeUtility::currentTimeInSeconds();
    uint32_t expireTime = curTime - topicExpireSec;
    std::vector<std::string> notUseTopics;
    std::vector<std::pair<uint32_t, uint32_t>> notUseTopicsRWtime;
    for (int idx = 0; idx < rwInfos.infos_size(); ++idx) {
        const TopicRWInfo &info = rwInfos.infos(idx);
        if (info.lastwritetime() < expireTime && info.lastreadtime() < expireTime) {
            notUseTopics.emplace_back(info.topicname());
            notUseTopicsRWtime.emplace_back(info.lastwritetime(), info.lastreadtime());
        }
    }
    if (0 == notUseTopics.size()) {
        return;
    }
    std::map<std::string, TopicCreationRequest> notUseTopicMetas;
    TopicMetas delMetas;
    std::set<std::string> validTopics;
    for (size_t idx = 0; idx < notUseTopics.size(); ++idx) {
        const std::string &topicName = notUseTopics[idx];
        TopicInfoPtr info = _topicTable->findTopic(topicName);
        if (!info) {
            continue;
        }
        uint32_t modifyTime = info->getModifyTime() / 1000000;
        if (modifyTime > expireTime) {
            continue;
        }
        AUTIL_LOG(INFO,
                  "[%s] expired, write[%d], read[%d], topic modify[%d]",
                  topicName.c_str(),
                  notUseTopicsRWtime[idx].first,
                  notUseTopicsRWtime[idx].second,
                  modifyTime);
        validTopics.insert(topicName);
        *delMetas.add_topicmetas() = info->getTopicMeta();
        notUseTopicMetas[topicName] = info->getTopicMeta();
    }
    if (0 == validTopics.size()) {
        return;
    }
    //写文件，设置保留文件个数，文件以时间戳命名
    if (!_zkDataAccessor->writeDeletedNoUseTopics(delMetas, _adminConfig->getMaxAllowNouseFileNum())) {
        AUTIL_LOG(ERROR, "save expire file failed");
        return;
    }
    TopicBatchDeletionResponse response;
    response = _sysController->doDeleteTopicBatch(validTopics, false);
    if (ERROR_NONE == response.errorinfo().errcode()) {
        AUTIL_LOG(INFO, "delete success");
        _noUseTopicInfo.setLastNoUseTopics(notUseTopicMetas);
    } else {
        AUTIL_LOG(ERROR, "delete fail[%s]", response.errorinfo().ShortDebugString().c_str());
    }
}

void NoUseTopicModule::reportMissTopic(const MissTopicRequest *request, MissTopicResponse *response) {
    auto *errorInfo = response->mutable_errorinfo();
    errorInfo->set_errcode(ERROR_NONE);
    errorInfo->set_errmsg("ERROR_NONE");
    TopicBatchCreationRequest metas;
    metas.set_ignoreexist(true);
    _noUseTopicInfo.getLastNoUseTopicsMeta(request, metas);
    if (0 == metas.topicrequests_size()) {
        AUTIL_LOG(INFO, "topic empty or not in recent delete table");
        return;
    }
    TopicBatchCreationResponse createResponse;
    _sysController->createTopicBatch(&metas, &createResponse);
    if (ERROR_NONE != createResponse.errorinfo().errcode()) {
        AUTIL_LOG(ERROR,
                  "recover miss topic fail, request[%s], response[%s]",
                  request->ShortDebugString().c_str(),
                  createResponse.ShortDebugString().c_str());
        errorInfo->set_errcode(createResponse.errorinfo().errcode());
        errorInfo->set_errmsg(ErrorCode_Name(errorInfo->errcode()) + "[recover fail]");
        return;
    }
    _noUseTopicInfo.removeLastNoUseTopic(metas);
    AUTIL_LOG(INFO, "recover miss topic[%s] success", request->ShortDebugString().c_str());
}

// loop thread, follower ask leader to recover, leader recover itself
void NoUseTopicModule::recoverMissTopics() {
    AUTIL_LOG(INFO, "begin recover miss topic");
    auto recoverTopics = _noUseTopicInfo.getNeedRecoverTopic();
    AUTIL_LOG(INFO, "recoverTopics [%s]", autil::StringUtil::toString(recoverTopics).c_str());
    if (0 == recoverTopics.size()) {
        return;
    }
    MissTopicRequest recoverRequest;
    MissTopicResponse response;
    for (const std::string &topic : recoverTopics) {
        *recoverRequest.add_topicnames() = topic;
    }
    recoverRequest.set_timeout(4000);
    if (isLeader()) {
        AUTIL_LOG(INFO, "leader recover itself request[%s]", recoverRequest.ShortDebugString().c_str());
        reportMissTopic(&recoverRequest, &response);
    } else {
        if (!_adminAdapter && !initAdminClient()) {
            AUTIL_LOG(ERROR, "admin client init fail");
            return;
        }
        // only follower has _adminClient
        AUTIL_LOG(INFO, "send recover topic request[%s]", recoverRequest.ShortDebugString().c_str());
        _adminAdapter->reportMissTopic(recoverRequest, response);
    }
    if (ERROR_NONE != response.errorinfo().errcode()) {
        AUTIL_LOG(ERROR, "recover fail, error[%s]", response.ShortDebugString().c_str());
        return;
    }
    _noUseTopicInfo.clearNotExistTopics();
}

REGISTER_MODULE(NoUseTopicModule, "M", "L|F");

} // namespace admin
} // namespace swift
