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
#pragma once

#include <atomic>
#include <cstdint>

#include "aios/network/gig/multi_call/common/common.h"
#include "suez/common/InnerDef.h"
#include "suez/sdk/PartitionId.h"

namespace suez {
class TableInfoPublishWrapper;
class LeaderElectionManager;

class TableLeaderInfoPublisher {
public:
    TableLeaderInfoPublisher(const PartitionId &pid,
                             const std::string &zoneName,
                             TableInfoPublishWrapper *publishWrapper,
                             LeaderElectionManager *leaderElectionMgr);
    virtual ~TableLeaderInfoPublisher();

public:
    bool updateLeaderInfo();
    bool updateWeight(int32_t weight);
    void setAllowPublishLeader(bool allowPublishLeader);
    virtual bool publish();
    virtual bool unpublish();

private:
    std::string getBizName() const;
    bool doPublish(const std::string &bizName, multi_call::SignatureTy &signature, bool &signaturePublished);
    bool doUnpublish(const std::string &bizName, multi_call::SignatureTy &signature, bool &signaturePublished);

public:
    // for test
    bool signaturePublished() const { return _signaturePublished; }
    bool signaturePublishedForRawTable() const { return _signaturePublishedForRawTable; }

private:
    bool _allowPublishLeader;
    bool _leaderInfoPublished;
    bool _signaturePublished;
    bool _signaturePublishedForRawTable;
    RoleType _lastPublishedRoleType;
    int32_t _targetWeight;
    int32_t _publishedWeight;
    PartitionId _pid;
    std::string _zoneName;
    TableInfoPublishWrapper *_publishWrapper;
    LeaderElectionManager *_leaderElectionMgr;
    multi_call::SignatureTy _signature;
    multi_call::SignatureTy _signatureForRawTable;
};

} // namespace suez
