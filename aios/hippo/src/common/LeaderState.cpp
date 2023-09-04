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
#include "common/LeaderState.h"
#include "common/ZkState.h"
#include "common/LocalState.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
using namespace worker_framework;

BEGIN_HIPPO_NAMESPACE(common);

HIPPO_LOG_SETUP(common, LeaderState);

LeaderState::LeaderState()
    : _leaderChecker(NULL)
    , _realState(NULL)
    , _backupState(NULL)
{}

LeaderState::LeaderState(LeaderChecker *leaderChecker,
                         cm_basic::ZkWrapper *zkWrapper,
                         const string &basePath,
                         const string &backupPath)
{
    assert(leaderChecker);
    assert(zkWrapper);
    _leaderChecker = leaderChecker;
    _realState = new ZkState(zkWrapper, basePath);
    if (!backupPath.empty()) {
        _backupState = new LocalState(backupPath);
    } else {
        _backupState = NULL;
    }
}

LeaderState::~LeaderState() {
    DELETE_AND_SET_NULL_HIPPO(_realState);
    DELETE_AND_SET_NULL_HIPPO(_backupState);
}

bool LeaderState::check(const std::string &fileName, bool &bExist) {
    assert(_realState);
    return _realState->check(fileName, bExist);
}

bool LeaderState::read(const string &fileName, string &content) const {
    assert(_realState);
    int64_t startTime = TimeUtility::currentTime();    
    bool ret = _realState->read(fileName, content);
    int64_t endTime = TimeUtility::currentTime();
    if (endTime - startTime > 1 * 1000 * 1000) {
        HIPPO_LOG(WARN, "leader state read too slow, used %ld ms.",
                  (endTime - startTime) / 1000);
    }
    return ret;
}

bool LeaderState::write(const string &fileName, const string &content) {
    int64_t startTime = TimeUtility::currentTime();    
    if (!_leaderChecker->isLeader()) {
        HIPPO_LOG(INFO, "LeaderState write %s prohibited, not leader now.",
                  fileName.c_str());
        return false;
    }
    // if (_contentCache.find(fileName) != _contentCache.end()
    //     && _contentCache[fileName] == content)
    // {
    //     return true;
    // }
    if (!_realState->write(fileName, content)) {
        HIPPO_LOG(ERROR, "write %s failed.", fileName.c_str());
        return false;
    }
    // _contentCache[fileName] = content;
    if (_backupState) {
        if (!_backupState->write(fileName, content)) {
            HIPPO_LOG(WARN, "LeaderState serialize succeed, but backup to %s "
                      "failed, error ignored", fileName.c_str());
        }
    }
    int64_t endTime = TimeUtility::currentTime();
    if (endTime - startTime > 1 * 1000 * 1000) {
        HIPPO_LOG(WARN, "leader state write too slow, used %ld ms.",
                  (endTime - startTime) / 1000);
    }
    return true;
}

bool LeaderState::remove(const string &path) {
    if (!_leaderChecker->isLeader()) {
        HIPPO_LOG(INFO, "LeaderState remove %s prohibited, not leader now.",
                  path.c_str());
        return false;
    }
    if (!_realState->remove(path)) {
        HIPPO_LOG(ERROR, "remove %s failed.", path.c_str());
        return false;
    }
    if (_backupState) {
        if (!_backupState->remove(path)) {
            HIPPO_LOG(WARN, "LeaderState remove succeed, but remove backup file %s "
                      "failed, error ignored", path.c_str());
        }
    }
    return true;
}

END_HIPPO_NAMESPACE(common);

