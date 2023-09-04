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
#include "common/Log.h"
#include "master_framework/SimpleMasterScheduler.h"
#include "master_framework/SimpleMasterSchedulerAdapter.h"
#include "simple_master/SimpleMasterSchedulerAdapterImpl.h"

using namespace std;
using namespace hippo;
using namespace autil;

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

SimpleMasterScheduler::SimpleMasterScheduler() {
    _SimpleMasterSchedulerAdapterPtr.reset(SimpleMasterSchedulerAdapter::createSimpleMasterSchedulerAdapter());
}

SimpleMasterScheduler::~SimpleMasterScheduler() {
   _SimpleMasterSchedulerAdapterPtr.reset();
}

bool SimpleMasterScheduler::init(
        const std::string &hippoZkRoot,
        worker_framework::LeaderChecker *leaderChecker,
        const std::string &applicationId)
{
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->init(
        hippoZkRoot, leaderChecker, applicationId);
}

bool SimpleMasterScheduler::start() {
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->start();
}

bool SimpleMasterScheduler::stop() {
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->stop();
}

bool SimpleMasterScheduler::isLeader() {
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->isLeader();
}

SlotInfos SimpleMasterScheduler::getRoleSlots(const string &roleName) {
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->getRoleSlots(roleName);
}

void SimpleMasterScheduler::getAllRoleSlots(
        map<string, SlotInfos> &roleSlots) {
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->getAllRoleSlots(roleSlots);
}

void SimpleMasterScheduler::setAppPlan(const AppPlan &appPlan) {
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->setAppPlan(appPlan);
}

void SimpleMasterScheduler::releaseSlots(const vector<hippo::SlotId> &slots,
        const hippo::ReleasePreference &pref)
{
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->releaseSlots(slots, pref);
}

hippo::LeaderSerializer* SimpleMasterScheduler::createLeaderSerializer(
        const std::string &zookeeperRoot, const std::string &fileName,
        const std::string &backupRoot)
{
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->createLeaderSerializer(
        zookeeperRoot, fileName, backupRoot);
}

hippo::MasterDriver* SimpleMasterScheduler::getMasterDriver() {
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->getMasterDriver();
}

void SimpleMasterScheduler::setMasterDriver(hippo::MasterDriver* masterDriver) {
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->setMasterDriver(masterDriver);
}

::google::protobuf::Service* SimpleMasterScheduler::getService() {
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->getService();
}

::google::protobuf::Service* SimpleMasterScheduler::getOpsService() {
    assert(_SimpleMasterSchedulerAdapterPtr != NULL);
    return _SimpleMasterSchedulerAdapterPtr->getOpsService();
}

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

