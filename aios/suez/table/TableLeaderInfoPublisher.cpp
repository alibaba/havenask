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
#include "suez/table/TableLeaderInfoPublisher.h"

#include "suez/common/GeneratorDef.h"
#include "suez/table/TableInfoPublishWrapper.h"
#include "suez/table/ZkLeaderElectionManager.h"

using namespace std;
using namespace multi_call;
namespace suez {

static constexpr int32_t DEFAULT_TARGET_WEIGHT = 3;
static constexpr int32_t DEFAULT_PUBLISHED_WEIGHT = std::numeric_limits<int32_t>::min();

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TableLeaderInfoPublisher);

TableLeaderInfoPublisher::TableLeaderInfoPublisher(const PartitionId &pid,
                                                   const string &zoneName,
                                                   TableInfoPublishWrapper *publishWrapper,
                                                   LeaderElectionManager *leaderElectionMgr)
    : _allowPublishLeader(false)
    , _leaderInfoPublished(false)
    , _signaturePublished(false)
    , _signaturePublishedForRawTable(false)
    , _lastPublishedRoleType(RT_FOLLOWER)
    , _targetWeight(DEFAULT_TARGET_WEIGHT)
    , _publishedWeight(DEFAULT_PUBLISHED_WEIGHT)
    , _pid(pid)
    , _zoneName(zoneName)
    , _publishWrapper(publishWrapper)
    , _leaderElectionMgr(leaderElectionMgr)
    , _signature(0)
    , _signatureForRawTable(0) {}

TableLeaderInfoPublisher::~TableLeaderInfoPublisher() { unpublish(); }

void TableLeaderInfoPublisher::setAllowPublishLeader(bool allowPublishLeader) {
    _allowPublishLeader = allowPublishLeader;
}

bool TableLeaderInfoPublisher::updateLeaderInfo() {
    if (_leaderInfoPublished && _leaderElectionMgr && _lastPublishedRoleType == _leaderElectionMgr->getRoleType(_pid)) {
        AUTIL_LOG(DEBUG, "leader info already updated for pid[%s]", FastToJsonString(_pid, true).c_str());
        return true;
    }
    bool ret = publish();
    if (!ret) {
        _leaderInfoPublished = false;
        // log and retry
        AUTIL_LOG(WARN, "update leader info failed for pid[%s]", FastToJsonString(_pid, true).c_str());
    } else {
        _leaderInfoPublished = true;
        if (_leaderElectionMgr) {
            _lastPublishedRoleType = _leaderElectionMgr->getRoleType(_pid);
        }
        AUTIL_LOG(INFO, "update leader info success for pid[%s]", FastToJsonString(_pid, true).c_str());
    }
    return ret;
}

bool TableLeaderInfoPublisher::updateWeight(int32_t weight) {
    if (_publishedWeight == weight) {
        return true;
    }
    _targetWeight = weight;
    AUTIL_LOG(INFO, "[%s]: do update weight to [%d]", FastToJsonString(_pid, true).c_str(), weight);
    return publish();
}

string TableLeaderInfoPublisher::getBizName() const {
    string publishZoneName;
    auto pos = _zoneName.rfind('-');
    publishZoneName = string::npos == pos ? _zoneName : _zoneName.substr(pos + 1);
    string bizName = GeneratorDef::getTableWriteBizName(
        publishZoneName, _pid.tableId.tableName, _leaderElectionMgr->getLeaderElectionConfig().needZoneName);

    return bizName;
}

bool TableLeaderInfoPublisher::publish() {
    if (!_publishWrapper) {
        return false;
    }
    if (CLT_BY_INDICATION == _leaderElectionMgr->getLeaderElectionConfig().campaginLeaderType && !_allowPublishLeader) {
        AUTIL_LOG(INFO, "not allow publish leader info pid: [%s]", FastToJsonString(_pid, true).c_str());
        return false;
    }
    string bizName = getBizName();
    bool ret = doPublish(bizName, _signature, _signaturePublished);
    if (ret && _leaderElectionMgr->getLeaderElectionConfig().extraPublishRawTable) {
        ret = doPublish(_pid.tableId.tableName, _signatureForRawTable, _signaturePublishedForRawTable);
    }
    if (ret) {
        _publishedWeight = _targetWeight;
    }
    return ret;
}

bool TableLeaderInfoPublisher::unpublish() {
    if (!_publishWrapper || !_signaturePublished) {
        return true;
    }
    string bizName = getBizName();
    return doUnpublish(bizName, _signature, _signaturePublished) &&
           doUnpublish(_pid.tableId.tableName, _signatureForRawTable, _signaturePublishedForRawTable);
}

bool TableLeaderInfoPublisher::doUnpublish(const string &bizName, SignatureTy &signature, bool &signaturePublished) {
    if (signaturePublished) {
        signaturePublished = !_publishWrapper->unpublish(signature);
        AUTIL_CONDITION_LOG(signaturePublished,
                            ERROR,
                            INFO,
                            "unpublish leader info success[%d], pid: [%s], bizName: [%s]",
                            !signaturePublished,
                            FastToJsonString(_pid, true).c_str(),
                            bizName.c_str());
    }
    return !signaturePublished;
}

bool TableLeaderInfoPublisher::doPublish(const string &bizName, SignatureTy &signature, bool &signaturePublished) {
    if (signaturePublished && _publishedWeight < 0 && _targetWeight >= 0 &&
        !doUnpublish(bizName, signature, signaturePublished)) {
        return false;
    }
    multi_call::TagMap tagMap;
    auto leaderInfo = _leaderElectionMgr->getLeaderElectionInfo(_pid);
    if (leaderInfo.first) {
        tagMap[GeneratorDef::LEADER_TAG] = leaderInfo.second;
    }
    if (signaturePublished) {
        BizVolatileInfo info;
        info.tags = tagMap;
        info.targetWeight = _targetWeight;
        signaturePublished = _publishWrapper->updateVolatileInfo(signature, info);
    } else {
        ServerBizTopoInfo info;
        info.bizName = bizName;
        info.partCount = _pid.partCount;
        info.partId = _pid.index;
        info.version = 0;
        info.tags = tagMap;
        info.targetWeight = _targetWeight;
        signaturePublished = _publishWrapper->publish(info, signature);
    }
    AUTIL_CONDITION_LOG(signaturePublished,
                        INFO,
                        ERROR,
                        "update leader info success[%d], isLeader: %d, pid: [%s], bizName: [%s], weight: [%d]",
                        signaturePublished,
                        leaderInfo.first,
                        FastToJsonString(_pid, true).c_str(),
                        bizName.c_str(),
                        _targetWeight);
    return signaturePublished;
}

} // namespace suez
