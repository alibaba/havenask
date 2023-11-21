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
#include "build_service/common/BrokerTopicKeeper.h"

#include <assert.h>
#include <iosfwd>
#include <memory>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "beeper/beeper.h"
#include "beeper/common/common_type.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/proto/Heartbeat.pb.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::common;

namespace build_service { namespace common {
BS_LOG_SETUP(common, BrokerTopicKeeper);

int64_t BrokerTopicKeeper::MAX_RETRY_INTERVAL = 60;

BrokerTopicKeeper::BrokerTopicKeeper(const BrokerTopicKeeper& other)
    : _clusterName(other._clusterName)
    , _topicStatus(other._topicStatus)
    , _topicConfigName(other._topicConfigName)
    , _topicName(other._topicName)
    , _retryInterval(other._retryInterval)
    , _lastRetryTimestamp(other._lastRetryTimestamp)
    , _versionControl(other._versionControl)
    , _swiftClientCreator(other._swiftClientCreator)
{
    if (other._swiftAdminFacade) {
        _swiftAdminFacade.reset(new SwiftAdminFacade(*(other._swiftAdminFacade)));
    }
}

bool BrokerTopicKeeper::init(const proto::BuildId& buildId, const config::ResourceReaderPtr& resourceReader,
                             const std::string& clusterName, const std::string& topicConfigName,
                             const std::string& topicName, bool versionControl)
{
    _versionControl = versionControl;
    setBeeperCollector(GENERATION_ERROR_COLLECTOR_NAME);
    initBeeperTag(buildId);
    addBeeperTag("cluster", clusterName);
    _swiftAdminFacade.reset(new SwiftAdminFacade(_swiftClientCreator));
    if (!_swiftAdminFacade->init(buildId, resourceReader, clusterName)) {
        BS_LOG(ERROR, "init swiftAdminFacade failed");
        return false;
    }
    _clusterName = clusterName;
    _topicConfigName = topicConfigName;
    _topicName = topicName;
    return true;
}

void BrokerTopicKeeper::updateRetryStatus(bool success)
{
    if (success) {
        _lastRetryTimestamp = 0;
        _retryInterval = 0;
        return;
    }
    _retryInterval = _retryInterval == 0 ? 1 : _retryInterval * 2;
    if (_retryInterval > MAX_RETRY_INTERVAL) {
        _retryInterval = MAX_RETRY_INTERVAL;
    }
    _lastRetryTimestamp = TimeUtility::currentTimeInSeconds();
}

bool BrokerTopicKeeper::canRetry()
{
    int64_t currentTime = TimeUtility::currentTimeInSeconds();
    return currentTime - _lastRetryTimestamp > _retryInterval;
}

void BrokerTopicKeeper::collectBatchTopicCreationInfos(TopicCreationInfoMap& infos)
{
    if (_topicStatus != STS_CREATED) {
        _swiftAdminFacade->collectBatchTopicCreationInfos(_topicConfigName, _topicName, infos);
    }
}

bool BrokerTopicKeeper::prepareBrokerTopic()
{
    if (_topicStatus == STS_CREATED) {
        return true;
    }

    if (!canRetry()) {
        return false;
    }
    if (_swiftAdminFacade->createTopic(_topicConfigName, _topicName, _versionControl)) {
        markCreatedTopic();
    } else {
        REPORT(SERVICE_ADMIN_ERROR, "create topic:[%s] failed, need retry", _topicName.c_str());
    }
    updateRetryStatus(_topicStatus == STS_CREATED);
    return _topicStatus == STS_CREATED;
}

bool BrokerTopicKeeper::destroyAllBrokerTopic()
{
    if (!canRetry()) {
        return false;
    }

    assert(_swiftAdminFacade);
    if (_topicStatus == STS_DELETED) {
        return true;
    }
    if (!_swiftAdminFacade->deleteTopic(_topicConfigName, _topicName)) {
        reportDeleteTopic(false);
    } else {
        reportDeleteTopic(true);
    }

    updateRetryStatus(_topicStatus == STS_DELETED);
    return _topicStatus == STS_DELETED;
}

void BrokerTopicKeeper::markCreatedTopic()
{
    _topicStatus = STS_CREATED;
    beeper::EventTags tags;
    _globalTags->MergeTags(&tags);
    tags.AddTag("clusters", _clusterName);
    string msg = string("create topic:[") + _topicName + "] success";
    BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg, tags);
}

void BrokerTopicKeeper::reportCreateTopicFail()
{
    _topicStatus = STS_UNKOWN;
    REPORT(SERVICE_ADMIN_ERROR, "create topic:[%s] failed, need retry", _topicName.c_str());
}

void BrokerTopicKeeper::reportDeleteTopic(bool success)
{
    beeper::EventTags tags;
    _globalTags->MergeTags(&tags);
    tags.AddTag("clusters", _clusterName);

    if (success) {
        string msg = "delete topic:[" + _topicName + "] success";
        BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg, tags);
        BS_LOG(INFO, "%s", msg.c_str());
        _topicStatus = STS_DELETED;
    } else {
        REPORT(SERVICE_ADMIN_ERROR, "delete topic:[%s] failed, need retry", _topicName.c_str());
        _topicStatus = STS_UNKOWN;
    }
}

string BrokerTopicKeeper::getSwiftRoot() const { return _swiftAdminFacade->getSwiftRoot(_topicConfigName); }

void BrokerTopicKeeper::collectDeleteAllTopic(BatchDeleteTopicInfoMap& infoMap)
{
    string swiftRoot = _swiftAdminFacade->getSwiftRoot(_topicConfigName);
    if (_topicStatus != STS_DELETED) {
        infoMap[swiftRoot].toDeleteTopicNames.insert(_topicName);
    }
}

}} // namespace build_service::common
