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
#include "sdk/default/DefaultProcessLauncher.h"
#include "sdk/default/ProcessStartWorkItem.h"
#include "hippo/ProtoWrapper.h"
#include "util/SignatureUtil.h"
#include "util/JsonUtil.h"

using namespace std;
using namespace autil;

USE_HIPPO_NAMESPACE(util);
BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, DefaultProcessLauncher);

DefaultProcessLauncher::DefaultProcessLauncher()
    : ProcessLauncherBase()
    , _processCheckInterval(30000000)
    , _launchedMetasSerializer(nullptr)
    , _reboot(true)
{
    auto charPtr = std::getenv("HOME");
    if (charPtr) {
        _homeDir = string(charPtr);
    }
}

DefaultProcessLauncher::~DefaultProcessLauncher() {
}

bool DefaultProcessLauncher::isPackageReady(const SlotInfo &slotInfo) const {
    return true;
}

bool DefaultProcessLauncher::isPreDeployPackageReady(const SlotInfo &slotInfo) const
{
    return true;
}

bool DefaultProcessLauncher::isDataReady(const SlotInfo &slotInfo,
        const vector<string> &dataNames) const
{
    return true;
}

void DefaultProcessLauncher::launch(const map<string, std::set<SlotId> > &slotIds)
{
    HIPPO_LOG(INFO, "begin launch");
    if (!recoverLaunchedMetas()) {
        HIPPO_LOG(ERROR, "recover launced metas failed");
        return;
    }
    if (!_controller.started()) {
        _controller.start();
    }
    map<string, std::set<SlotId> > releasedSlotIds;
    getReleasedSlots(slotIds, releasedSlotIds);
    stopProcess(releasedSlotIds);

    clearUselessContexts(slotIds);
    _role2SlotIds = slotIds;
    asyncLaunch(slotIds);
    HIPPO_LOG(DEBUG, "after async launch, counter:%d", _counter.peek());
    _counter.wait();
    backupLaunchedMetas();
    HIPPO_LOG(INFO, "end launch");
}

void DefaultProcessLauncher::asyncLaunchOneSlot(
        const string& role,
        const hippo::SlotId &slotId,
        const hippo::ProcessContext &slotProcessContext)
{
    HIPPO_LOG(DEBUG, "begin launch slot, role:%s, slot address:%s slot id:%d",
              role.c_str(), slotId.slaveAddress.c_str(), slotId.id);
    ProcessStartWorkItem *workItem = new ProcessStartWorkItem();
    workItem->applicationId  = _applicationId;
    workItem->role = role;
    workItem->slotId = slotId;
    workItem->homeDir = _homeDir;
    workItem->cmdExecutor = _controller.getCmdExecutor();
    getLaunchSignature(slotId, workItem->launchSignature);
    ProtoWrapper::convert(slotProcessContext, &(workItem->processContext));
    workItem->callback = std::bind(&DefaultProcessLauncher::callback, this,
                                   std::placeholders::_1, std::placeholders::_2,
                                   std::placeholders::_3, std::placeholders::_4);
    if (!getSlotResource(slotId, workItem->slotResource)) {
        HIPPO_LOG(WARN, "get slot resource failed, slot role:%s, "
                  "slot address:%s slot id:%d",
                  role.c_str(), slotId.slaveAddress.c_str(), slotId.id);
        delete workItem;
        return;
    }
    if (!needLaunch(slotId, workItem)) {
        HIPPO_LOG(DEBUG, "no need launch, slot role:%s, slot address:%s slot id:%d",
                  role.c_str(), slotId.slaveAddress.c_str(), slotId.id);
        delete workItem;
        return;
    }
    HIPPO_LOG(INFO, "start process for role:[%s], slaveAddr:[%s], slot:[%d], declareTime:%ld",
              role.c_str(), slotId.slaveAddress.c_str(), slotId.id, slotId.declareTime);
    HIPPO_LOG(DEBUG, "process context:[%s]", workItem->processContext.DebugString().c_str());
    _counter.inc();
    if (!_controller.startProcess(workItem)) {
        HIPPO_LOG(ERROR, "start process on slave:[%s] failed.",
                  slotId.slaveAddress.c_str());
        delete workItem;
        _counter.dec();
    }
}

bool DefaultProcessLauncher::getSlotResource(const hippo::SlotId &slotId,
        hippo::SlotResource &slotResource)
{
    auto it = _slotResources.find(slotId);
    if (it != _slotResources.end()) {
        slotResource = it->second;
        HIPPO_LOG(DEBUG, "get resource, resource size:%ld", slotResource.resources.size());
        for (auto &resource : slotResource.resources) {
            HIPPO_LOG(DEBUG, "resource name:%s, type:%d, value:%d",
                      resource.name.c_str(), resource.type, resource.amount);
        }
        return true;
    }

    return false;
}

void DefaultProcessLauncher::getReleasedSlots(
        const map<string, set<hippo::SlotId> > &slotIds,
        map<string, set<hippo::SlotId> > &releasedSlots)
{
    for (const auto &roleSlots : _role2SlotIds) {
        auto it = slotIds.find(roleSlots.first);
        if (it == slotIds.end()) {
            releasedSlots[roleSlots.first] = roleSlots.second;
        } else {
            auto &newSlots = it->second;
            auto &oldSlots = roleSlots.second;
            for (auto &slotId : oldSlots) {
                if (newSlots.count(slotId) == 0) {
                    releasedSlots[roleSlots.first].insert(slotId);
                }
            }
        }
    }
}

void DefaultProcessLauncher::stopProcess(
        const map<string, set<hippo::SlotId> > &releasedSlots)
{
    for (const auto& roleSlotIds : releasedSlots) {
        const string &role = roleSlotIds.first;
        for (const auto& slotId : roleSlotIds.second) {
            HIPPO_LOG(INFO, "stop unused slot, role:[%s], "
                      "slot address:[%s], slot id:[%d]", role.c_str(),
                      slotId.slaveAddress.c_str(), slotId.id);
            hippo::ProcessContext slotProcessContext;
            vector<string> processNames;
            if (getSlotProcessContext(role, slotId, &slotProcessContext)) {
                for (auto process: slotProcessContext.processes) {
                    processNames.push_back(process.cmd);
                }
            }
            _controller.stopProcess(slotId, processNames);
        }
    }
}

bool DefaultProcessLauncher::resetSlot(const hippo::SlotId &slotId) {
    return _controller.resetSlot(slotId);
}

bool DefaultProcessLauncher::getLaunchSignature(const hippo::SlotId &slotId,
        int64_t &signature)
{
    map<hippo::SlotId, LaunchMeta>::const_iterator it = _launchMetas.find(slotId);
    if (it == _launchMetas.end()) {
        return false;
    }
    signature = it->second.launchSignature;
    return true;
}

bool DefaultProcessLauncher::needLaunch(const hippo::SlotId &slotId,
                                        const ProcessStartWorkItem *workItem)
{
    int64_t targetSignature = -1;
    if (!getLaunchSignature(slotId, targetSignature)) {
        HIPPO_LOG(DEBUG, "target launch meta not exists, no need launch, "
                  "slotId address:%s, slotId id:%d", slotId.slaveAddress.c_str(),
                  slotId.id);
        return false;
    }
    int64_t currentSignature = -1;
    ScopedLock lock(_mutex);
    auto cIt = _launchedMetas.find(slotId);
    if (cIt == _launchedMetas.end()) {
        HIPPO_LOG(DEBUG, "launched meta not exists, need launch, "
                  "slotId address:%s, slotId id:%d", slotId.slaveAddress.c_str(),
                  slotId.id);
            return true;
    }
    currentSignature = cIt->second.first;
    HIPPO_LOG(DEBUG, "launch meta, slotId address:%s, slotId id:%d, "
              "src launchSignature:%ld, target launchSignature:%ld",
              slotId.slaveAddress.c_str(), slotId.id,
              currentSignature, targetSignature);
    int64_t currentTime = TimeUtility::currentTime();
    if (targetSignature != currentSignature) {
        return true;
    } else if (currentTime - _processCheckInterval > cIt->second.second) {
        HIPPO_LOG(DEBUG, "need check slot status, slotId address:%s, slotId id:%d, "
                  "currentTime:%ld lastCheckTime:%ld",
                  slotId.slaveAddress.c_str(), slotId.id,
                  currentTime, cIt->second.second);
        if (isSlotRunning(workItem)) {
            return false;
        }
        HIPPO_LOG(DEBUG, "slot not running, need launch");
        return true;
    }
    return false;
}

void DefaultProcessLauncher::callback(const hippo::SlotId &slotId,
                                      int64_t launchSignature,
                                      ProcessError ret,
                                      const string& errorMsg)
{
    _counter.dec();
    if (ret != ERROR_NONE) {
        clearLaunchedSignature(slotId);
        HIPPO_LOG(ERROR, "start process failed, msg:%s", errorMsg.c_str());
        return;
    }
    setLaunchedSignature(slotId, launchSignature);
    HIPPO_LOG(DEBUG, "start process callback, counter:%d, signature:%d",
              _counter.peek(), launchSignature);
}

void DefaultProcessLauncher::clearLaunchedSignature(const hippo::SlotId &slotId)
{
    ScopedLock lock(_mutex);
    _launchedMetas.erase(slotId);
}

void DefaultProcessLauncher::setLaunchedSignature(const hippo::SlotId &slotId,
        int64_t launchSignature)
{
    int64_t currentTime = TimeUtility::currentTime();
    ScopedLock lock(_mutex);
    _launchedMetas[slotId].first = launchSignature;
    _launchedMetas[slotId].second = currentTime;
}

void DefaultProcessLauncher::setLaunchMetas(
        const std::map<hippo::SlotId, LaunchMeta> &launchMetas)
{
    _launchMetas = launchMetas;

    for (auto &[slotId, launchMeta] : _launchMetas) {
        hippo::ProcessContext context;
        for (auto &[role, _] : _roleProcessContextMap) {
            if (doGetSlotProcessContext(role, slotId, &context)) {
                proto::ProcessLaunchContext launchContext;
                ProtoWrapper::convert(context, &launchContext);
                int64_t curVersionSignature = SignatureUtil::signature(launchContext);
                HIPPO_LOG(DEBUG, "rewrite launchSignature, curVersionSignature:%ld, allocateSignature:%ld",
                        curVersionSignature, launchMeta.launchSignature);
                launchMeta.launchSignature = curVersionSignature;
                break;
            }
        }
    }
    ScopedLock lock(_mutex);
    //clear useless slot
    for (auto it = _launchedMetas.begin(); it != _launchedMetas.end();) {
        if (launchMetas.find(it->first) == launchMetas.end()) {
            _launchedMetas.erase(it++);
        } else {
            it++;
        }
    }
}

std::map<hippo::SlotId, LaunchMeta> DefaultProcessLauncher::getLaunchedMetas() {
    map<hippo::SlotId, LaunchMeta> launchedMetaMap;
    ScopedLock lock(_mutex);
    for (const auto &it : _launchedMetas) {
        launchedMetaMap[it.first].launchSignature = it.second.first;
    }
    return launchedMetaMap;
}

void DefaultProcessLauncher::backupLaunchedMetas() {
    if (!_launchedMetasSerializer) {
        return;
    }
    map<hippo::SlotId, std::pair<int64_t, int64_t> > launchedMetaMap;
    {
        ScopedLock lock(_mutex);
        launchedMetaMap = _launchedMetas;
    }
    string content;
    serializeLaunchedMetas(launchedMetaMap, content);
    HIPPO_LOG(INFO, "serialize launced metas:%s", content.c_str());
    _launchedMetasSerializer->write(content);
}

bool DefaultProcessLauncher::recoverLaunchedMetas() {
    if (!_reboot || !_launchedMetasSerializer || !_launchedMetas.empty()) {
        return true;
    }
    string content;
    bool bExist = false;
    if (!_launchedMetasSerializer->checkExist(bExist)) {
        HIPPO_LOG(ERROR, "check backup launced metas exist failed.");
        return false;
    }
    if (!bExist) {
        HIPPO_LOG(INFO, "backup launced metas is not exist, no need recover.");
        return true;
    }
    if (_launchedMetasSerializer->read(content)) {
        HIPPO_LOG(INFO, "deserialize launced metas:%s", content.c_str());
        map<hippo::SlotId, std::pair<int64_t, int64_t> > launchedMetaMap;
        deserializeLaunchedMetas(content, launchedMetaMap);
        ScopedLock lock(_mutex);
        _launchedMetas.swap(launchedMetaMap);
        _reboot = false;
        return true;
    }
    return false;
}

void DefaultProcessLauncher::serializeLaunchedMetas(
        const map<hippo::SlotId, pair<int64_t, int64_t> > &launchedMetaMap,
        string &content)
{
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    rapidjson::Value slotInfos;
    slotInfos.SetArray();
    for (auto it = launchedMetaMap.begin(); it != launchedMetaMap.end(); ++it) {
        rapidjson::Value slotInfo;
        slotInfo.SetObject();
        serailizeSlotLaunchedMeta(it->first, it->second, allocator, slotInfo);
        slotInfos.PushBack(slotInfo, allocator);
    }
    string nameStr = "launched_metas";
    rapidjson::Value name(nameStr.c_str(), nameStr.size(), allocator);
    doc.AddMember(name, slotInfos, allocator);
    JsonUtil::toJson(doc, content);
}

void DefaultProcessLauncher::serailizeSlotLaunchedMeta(const hippo::SlotId &slotId,
        const pair<int64_t, int64_t>& meta, rapidjson::MemoryPoolAllocator<> &allocator, rapidjson::Value &slotInfo)
{
    map<string, string> kvMap;
    kvMap["address"] = slotId.slaveAddress;
    kvMap["slot_id"] = StringUtil::toString(slotId.id);
    kvMap["declare_time"] = StringUtil::toString(slotId.declareTime);
    kvMap["launch_signature"] = StringUtil::toString(meta.first);
    kvMap["launch_time"] = StringUtil::toString(meta.second);
    for (auto &kv : kvMap) {
        rapidjson::Value name(kv.first.c_str(), kv.first.size(), allocator);
        rapidjson::Value value(kv.second.c_str(), kv.second.size(), allocator);
        slotInfo.AddMember(name, value, allocator);
    }
}

void DefaultProcessLauncher::deserializeLaunchedMetas(const string& content,
        map<hippo::SlotId, std::pair<int64_t, int64_t> > &launchedMetaMap)
{
    if (content.empty()) {
        return;
    }
    HIPPO_LOG(INFO, "deserialize launched metas:%s", content.c_str());
    rapidjson::Document doc;
    if (!JsonUtil::fromJson(content, doc)) {
        HIPPO_LOG(ERROR, "deserialize launched metas failed, invalid json[%s]",
                  content.c_str());
        return;
    }
    if (!doc.IsObject()) {
        HIPPO_LOG(ERROR, "deserialize launched metas failed, [%s] is not object",
                  content.c_str());
    }
    for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
        string role = it->name.GetString();
        auto &slotLaunchedMetas = it->value;
        if (!slotLaunchedMetas.IsArray()) {
            continue;
        }
        for (auto iter = slotLaunchedMetas.Begin(); iter != slotLaunchedMetas.End(); ++iter) {
            hippo::SlotId slotId;
            std::pair<int64_t, int64_t> launchMeta;
            deserailizeSlotLaunchedMeta(*iter, slotId, launchMeta);
            launchedMetaMap[slotId] = launchMeta;
        }
    }
}

void DefaultProcessLauncher::deserailizeSlotLaunchedMeta(
        rapidjson::Value &slotLaunchedMeta,
        hippo::SlotId &slotId,
        std::pair<int64_t, int64_t> &launchMeta)
{
    if (!slotLaunchedMeta.IsObject()) {
        HIPPO_LOG(ERROR, "deserialize slot launched meta failed, "
                  "which is not object");
        return;
    }
    for (auto it = slotLaunchedMeta.MemberBegin(); it != slotLaunchedMeta.MemberEnd(); it++) {
        string key(it->name.GetString());
        string value(it->value.GetString());
        if (key == "address") {
            slotId.slaveAddress = value;
        } else if (key == "slot_id") {
            slotId.id = StringUtil::fromString<int32_t>(value);
        } else if (key == "declare_time") {
            slotId.declareTime = StringUtil::fromString<int64_t>(value);
        } else if (key == "launch_signature") {
            launchMeta.first = StringUtil::fromString<int64_t>(value);
        } else if (key == "launch_time") {
            launchMeta.second = StringUtil::fromString<int64_t>(value);
        }
    }
}

void DefaultProcessLauncher::setApplicationId(const string &appId) {
    ProcessLauncherBase::setApplicationId(appId);
    _controller.setApplicationId(appId);
}

bool DefaultProcessLauncher::isSlotRunning(const ProcessStartWorkItem *workItem) {
    if (!workItem) {
        return false;
    }
    return _controller.checkProcessExist(workItem);
}

END_HIPPO_NAMESPACE(sdk);
