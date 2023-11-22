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
#include "build_service/common/CheckpointAccessor.h"

#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "build_service/util/ErrorLogCollector.h"

using namespace std;
using namespace build_service::common;
using namespace build_service::util;
using namespace autil;
using namespace autil::legacy;

namespace build_service { namespace common {
BS_LOG_SETUP(common, CheckpointAccessor);

void CheckpointAccessor::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("checkpoints", _checkpoints, _checkpoints);
    json.Jsonize("savepoints", _savepoints, _savepoints);
    json.Jsonize("savepoint_comments", _savepointComments, _savepointComments);
    // keep snapshots for compatibility
    json.Jsonize("snapshots", _savepoints, _savepoints);
}

void CheckpointAccessor::updateReservedCheckpoint(const std::string& id, int32_t keepCheckpointNum,
                                                  std::vector<std::pair<std::string, std::string>>& removeCheckpoints)
{
    ScopedLock lock(_mutex);
    auto iter = _checkpoints.find(id);
    if (keepCheckpointNum < 0 || iter == _checkpoints.end() || iter->second.size() <= (size_t)keepCheckpointNum) {
        return;
    }
    vector<pair<string, string>> newCheckpoints;
    size_t beginIdx = iter->second.size() - keepCheckpointNum;
    for (size_t i = 0; i < iter->second.size(); i++) {
        string ckpName = iter->second[i].first;
        string ckpValue = iter->second[i].second;
        pair<string, string> newCkp = make_pair(ckpName, ckpValue);
        if (i >= beginIdx) {
            // checkpoint in keep count
            newCheckpoints.push_back(newCkp);
            continue;
        }
        if (_savepoints.find(make_pair(id, ckpName)) != _savepoints.end()) {
            // reserve savepoint
            newCheckpoints.push_back(newCkp);
            continue;
        }
        removeCheckpoints.push_back(newCkp);
    }
    iter->second = newCheckpoints;
}

bool CheckpointAccessor::findCheckpointUnsafe(const std::string& id, const std::string& checkpointName,
                                              std::string& checkpoint)
{
    auto iter = _checkpoints.find(id);
    if (iter == _checkpoints.end()) {
        return false;
    }
    for (auto item : iter->second) {
        if (item.first == checkpointName) {
            checkpoint = item.second;
            return true;
        }
    }
    return false;
}

void CheckpointAccessor::addCheckpoint(const std::string& id, const std::string& checkpointName,
                                       const std::string& checkpointStr)
{
    ScopedLock lock(_mutex);
    string checkPoint;
    if (findCheckpointUnsafe(id, checkpointName, checkPoint)) {
        BS_LOG(ERROR, "id [%s] checkpoint [%s] is exist", id.c_str(), checkpointName.c_str());
        return;
    }
    _checkpoints[id].push_back(make_pair(checkpointName, checkpointStr));
    BS_LOG(INFO, "add checkpoint[%s, %s]", id.c_str(), checkpointName.c_str());
}

bool CheckpointAccessor::updateCheckpoint(const std::string& id, const std::string& checkpointName,
                                          const std::string& checkpointStr)
{
    ScopedLock lock(_mutex);
    auto iter = _checkpoints.find(id);
    if (iter == _checkpoints.end()) {
        return false;
    }
    for (auto& item : iter->second) {
        if (item.first == checkpointName) {
            item.second = checkpointStr;
            return true;
        }
    }
    return false;
}

void CheckpointAccessor::addOrUpdateCheckpoint(const string& id, const string& checkpointName,
                                               const string& checkpointStr)
{
    ScopedLock lock(_mutex);
    auto iter = _checkpoints.find(id);
    if (iter == _checkpoints.end()) {
        _checkpoints[id].push_back(make_pair(checkpointName, checkpointStr));
        return;
    }
    for (auto& item : iter->second) {
        if (item.first == checkpointName) {
            item.second = checkpointStr;
            return;
        }
    }
    _checkpoints[id].push_back(make_pair(checkpointName, checkpointStr));
    return;
}

void CheckpointAccessor::cleanCheckpoint(const std::string& id, const std::string& checkpointName)
{
    ScopedLock lock(_mutex);
    auto savepointIter = _savepoints.find(make_pair(id, checkpointName));
    if (savepointIter != _savepoints.end()) {
        return;
    }
    auto iter = _checkpoints.find(id);
    if (iter == _checkpoints.end()) {
        return;
    }
    size_t i = 0;
    for (; i < iter->second.size(); i++) {
        if (iter->second[i].first == checkpointName) {
            break;
        }
    }
    if (i < iter->second.size()) {
        iter->second.erase(iter->second.begin() + i);
    }
    if (iter->second.size() == 0) {
        _checkpoints.erase(iter);
    }
}

void CheckpointAccessor::cleanCheckpoints(const string& id, const set<string>& checkpointNames)
{
    ScopedLock lock(_mutex);
    auto iter = _checkpoints.find(id);
    if (iter == _checkpoints.end()) {
        return;
    }

    auto ckpIter = iter->second.begin();
    std::string comment;
    while (ckpIter != iter->second.end()) {
        if (!isSavepoint(id, (*ckpIter).first, &comment) &&
            checkpointNames.find((*ckpIter).first) != checkpointNames.end()) {
            ckpIter = iter->second.erase(ckpIter);
            continue;
        }
        ckpIter++;
    }
}

void CheckpointAccessor::listCheckpoints(const std::string& id,
                                         std::vector<std::pair<std::string, std::string>>& checkpoints)
{
    ScopedLock lock(_mutex);
    auto iter = _checkpoints.find(id);
    if (iter == _checkpoints.end()) {
        return;
    }
    checkpoints = iter->second;
}

bool CheckpointAccessor::getCheckpoint(const string& id, const string& checkpointName, string& checkpointValue,
                                       bool printLog)
{
    ScopedLock lock(_mutex);
    auto iter = _checkpoints.find(id);
    if (iter == _checkpoints.end()) {
        if (printLog) {
            BS_LOG(ERROR, "get checkpoint[%s:%s] failed, not exist.", id.c_str(), checkpointName.c_str());
        }
        return false;
    }
    for (auto ckp : iter->second) {
        if (ckp.first == checkpointName) {
            checkpointValue = ckp.second;
            return true;
        }
    }
    BS_LOG(ERROR, "get checkpoint[%s:%s] failed, not exist.", id.c_str(), checkpointName.c_str());
    return false;
}

bool CheckpointAccessor::getLatestCheckpoint(const std::string& id, std::string& checkpointName,
                                             std::string& checkpointValue)
{
    ScopedLock lock(_mutex);
    auto iter = _checkpoints.find(id);
    if (iter == _checkpoints.end() || iter->second.size() <= 0) {
        BS_LOG(INFO, "no check point found for checkpointId [%s]", id.c_str());
        return false;
    }
    checkpointName = iter->second[iter->second.size() - 1].first;
    checkpointValue = iter->second[iter->second.size() - 1].second;
    return true;
}

bool CheckpointAccessor::createSavepoint(const std::string& applyRole, const std::string& id,
                                         const std::string& checkpointName, std::string& errorMsg,
                                         const std::string& comment)
{
    ScopedLock lock(_mutex);
    string checkpoint;
    if (!findCheckpointUnsafe(id, checkpointName, checkpoint)) {
        errorMsg = id + "create savepoint " + checkpointName + " failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    auto savepointIter = _savepoints.find(make_pair(id, checkpointName));
    if (savepointIter != _savepoints.end()) {
        for (size_t i = 0; i < savepointIter->second.size(); i++) {
            if (savepointIter->second[i] == applyRole) {
                return true;
            }
        }
    }
    _savepoints[make_pair(id, checkpointName)].push_back(applyRole);
    _savepointComments[make_pair(id, checkpointName)].push_back(comment);
    BS_LOG(INFO, "create savepoint role [%s] ckpId[%s] ckpName[%s]", applyRole.c_str(), id.c_str(),
           checkpointName.c_str());
    return true;
}

bool CheckpointAccessor::isSavepoint(const string& id, const string& checkpointName, std::string* comment) const
{
    bool isSavepoint = _savepoints.find(make_pair(id, checkpointName)) != _savepoints.end();
    if (isSavepoint) {
        auto commentsIter = _savepointComments.find(make_pair(id, checkpointName));
        if (commentsIter != _savepointComments.end() && commentsIter->second.size() > 0) {
            *comment = commentsIter->second.at(0);
        }
    }
    return isSavepoint;
}

bool CheckpointAccessor::removeSavepoint(const std::string& applyRole, const std::string& id,
                                         const std::string& checkpointName, std::string& errorMsg)
{
    ScopedLock lock(_mutex);
    auto savepointIter = _savepoints.find(make_pair(id, checkpointName));
    if (savepointIter == _savepoints.end()) {
        return true;
    }
    size_t i = 0;
    for (; i < savepointIter->second.size(); i++) {
        if (savepointIter->second[i] == applyRole) {
            break;
        }
    }
    if (i != savepointIter->second.size()) {
        savepointIter->second.erase(savepointIter->second.begin() + i);
    }
    if (savepointIter->second.size() == 0) {
        _savepoints.erase(savepointIter);
    }

    auto savepointCommentIter = _savepointComments.find(make_pair(id, checkpointName));
    if (savepointCommentIter == _savepointComments.end()) {
        return true;
    }
    if (i != savepointCommentIter->second.size()) {
        savepointCommentIter->second.erase(savepointCommentIter->second.begin() + i);
    }
    if (savepointCommentIter->second.size() == 0) {
        _savepointComments.erase(savepointCommentIter);
    }
    BS_LOG(INFO, "remove savepoint role [%s] ckpId[%s] ckpName[%s]", applyRole.c_str(), id.c_str(),
           checkpointName.c_str());

    return true;
}

}} // namespace build_service::common
