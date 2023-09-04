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
#include "sdk/default/DefaultMasterDriver.h"
#include "sdk/default/DefaultSlotAssigner.h"
#include "sdk/default/DefaultSlotAllocator.h"
#include "sdk/default/DefaultProcessLauncher.h"

using namespace std;
BEGIN_HIPPO_NAMESPACE(sdk);

const string DefaultMasterDriver::ASSIGNED_SLOTS_FILE = "assigned_slots_info";
const string DefaultMasterDriver::CANDIDATE_IPS_FILE = "candidate_ips";

HIPPO_LOG_SETUP(sdk, DefaultMasterDriver);

DefaultMasterDriver::DefaultMasterDriver()
    : BasicMasterDriver()
    , _assignedSlotsSerializer(nullptr)
    , _ipListReader(nullptr)
    , _isFirstStart(true)
{
    auto charPtr = std::getenv("HOME");
    if (charPtr) {
        _homeDir = string(charPtr);
    }
    _slotAssigner = new DefaultSlotAssigner();
    _slotAllocator = new DefaultSlotAllocator(_eventTrigger, _slotAssigner);
    _processLauncher = new DefaultProcessLauncher();
}

DefaultMasterDriver::~DefaultMasterDriver() {
    detach();
    DELETE_AND_SET_NULL_HIPPO(_amLeaderChecker);
    DELETE_AND_SET_NULL_HIPPO(_processLauncher);
    DELETE_AND_SET_NULL_HIPPO(_slotAllocator);
    DELETE_AND_SET_NULL_HIPPO(_slotAssigner);
}

bool DefaultMasterDriver::init(const string &masterZkRoot,
                               const string &leaderElectRoot,
                               const string &appMasterAddr,
                               const string &applicationId)
{
    HIPPO_LOG(ERROR, "unspported init method.");
    assert(false);
    return false;
}

bool DefaultMasterDriver::init(const string &masterZkRoot,
                               worker_framework::LeaderChecker * leaderChecker,
                               const string &applicationId)
{
    if (!BasicMasterDriver::init(masterZkRoot, leaderChecker, applicationId)) {
        return false;
    }

    if (!createSerializer(masterZkRoot, ASSIGNED_SLOTS_FILE, &_assignedSlotsSerializer)
        || !createSerializer(masterZkRoot, CANDIDATE_IPS_FILE, &_ipListReader))
    {
        return false;
    }
    if (!_slotAssigner->init(applicationId, _assignedSlotsSerializer, _ipListReader))
    {
        return false;
    }
    HIPPO_LOG(INFO, "default master driver init success. "
              "masterZkRoot:[%s], appId:[%s]",
              masterZkRoot.c_str(), applicationId.c_str());
    return true;
}

bool DefaultMasterDriver::createSerializer(const string &zookeeperRoot,
        const string &fileName, LeaderSerializer **serializer)
{
    LeaderSerializer *tmp = *serializer;
    if (tmp) {
        delete tmp;
        *serializer = nullptr;
    }

    tmp = createLeaderSerializer(zookeeperRoot, fileName, "");
    if (tmp == nullptr) {
        HIPPO_LOG(ERROR, "create serializer for slots failed, serializer:%s",
                  fileName.c_str());
        return false;
    }
    *serializer = tmp;
    return true;
}

bool DefaultMasterDriver::initSlotAllocator(const std::string &masterZkRoot,
        const std::string &applicationId)
{
    _slotAllocator->setApplicationId(applicationId);
    return true;
}

bool DefaultMasterDriver::initPorcessLauncher(const std::string &masterZkRoot,
        const std::string &applicationId)
{
    _processLauncher->setApplicationId(applicationId);
    _processLauncher->setAppChecksum(getAppChecksum());
    return true;
}

string DefaultMasterDriver::getProcessWorkDir(const string &procName) const
{
    return "";
}

string DefaultMasterDriver::getProcessWorkDir(const string &workDirTag,
        const string &procName) const
{
    return "";
}

void DefaultMasterDriver::beforeLoop() {
    auto slotAllocator = dynamic_cast<DefaultSlotAllocator *>(_slotAllocator);
    auto processLauncher = dynamic_cast<DefaultProcessLauncher *>(_processLauncher);
    assert(slotAllocator);
    assert(processLauncher);
    auto launchedMetas = processLauncher->getLaunchedMetas();
    slotAllocator->setLaunchedMetas(launchedMetas);
}

void DefaultMasterDriver::afterLoop() {
    auto slotAllocator = dynamic_cast<DefaultSlotAllocator *>(_slotAllocator);
    auto processLauncher = dynamic_cast<DefaultProcessLauncher *>(_processLauncher);
    assert(slotAllocator);
    assert(processLauncher);
    map<string, set<hippo::SlotId> > slotIds;
    map<hippo::SlotId, LaunchMeta> launchMetas;
    map<hippo::SlotId, hippo::SlotResource> slotResources;
    slotAllocator->getAllSlotIds(&slotIds, &launchMetas, &slotResources);
    processLauncher->setLaunchMetas(launchMetas);
    processLauncher->setSlotResources(slotResources);
    processLauncher->launch(slotIds);
}

END_HIPPO_NAMESPACE(sdk);
