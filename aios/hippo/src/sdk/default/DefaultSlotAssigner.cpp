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
#include "sdk/default/DefaultSlotAssigner.h"
#include "hippo/ProtoWrapper.h"
#include "util/JsonUtil.h"
#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace hippo;
using namespace autil;
USE_HIPPO_NAMESPACE(util);
BEGIN_HIPPO_NAMESPACE(sdk);

const string DefaultSlotAssigner::MULTI_SLOTS_IN_ONE_NODE = "MULTI_SLOTS_IN_ONE_NODE";
const string DefaultSlotAssigner::DEFAULT_RESOURCE = "default";

HIPPO_LOG_SETUP(sdk, DefaultSlotAssigner);

DefaultSlotAssigner::DefaultSlotAssigner()
    : _multiSlotsOneNode(false)
    , _isReboot(true)
{
    string multiSlotsFlag;
    if(autil::EnvUtil::getEnvWithoutDefault(MULTI_SLOTS_IN_ONE_NODE,
                    multiSlotsFlag)
       && multiSlotsFlag == "true")
    {
        _multiSlotsOneNode = true;
    }
}

DefaultSlotAssigner::~DefaultSlotAssigner() {
}

bool DefaultSlotAssigner::init(const string &applicationId,
                               LeaderSerializer* assignedResSerializer,
                               LeaderSerializer* ipListReader)
{
    _applicationId = applicationId;
    _assignedResSerializer = assignedResSerializer;
    _ipListReader = ipListReader;
    return _assignedResSerializer && _ipListReader;
}

bool DefaultSlotAssigner::assign(const proto::AllocateRequest &request,
                                 proto::AllocateResponse *response)
{
    if (!updateMachineResource()) {
        return false;
    }
    if (!doAssign(request, response)) {
        return false;
    }
    backUpAssignedResource();
    HIPPO_LOG(INFO, "assign success");
    return true;
}

bool DefaultSlotAssigner::updateMachineResource() {
    assert(_ipListReader);
    assert(_assignedResSerializer);
    string ipInfoStr;
    if (!_ipListReader->read(ipInfoStr)) {
        HIPPO_LOG(ERROR, "read candidate ips file failed.");
        return false;
    }
    if (ipInfoStr == _ipInfoStr) {
        return true;
    }
    map<string, vector<string> > ipInfos;
    deserializeIpInfo(ipInfoStr, ipInfos);
    setIpInfo(ipInfos);
    _ipInfoStr = ipInfoStr;
    if (_isReboot && _assignedSlots.empty()) {
        string assignedResource;
        if (_assignedResSerializer->read(assignedResource)) {
            deserializeAssignedResource(assignedResource);
        }
        _isReboot = false;
    }
    return true;
}

bool DefaultSlotAssigner::doAssign(const proto::AllocateRequest &request,
                                   proto::AllocateResponse *response)
{
    releaseSlots(request);
    assignSlots(request, response);
    HIPPO_LOG(DEBUG, "do Assign, request:[%s]", request.DebugString().c_str());
    HIPPO_LOG(DEBUG, "do Assign, response:[%s]", response->DebugString().c_str());
    return true;
}

void DefaultSlotAssigner::releaseSlots(const proto::AllocateRequest &request) {
    for (int i = 0; i < request.release_size(); ++i) {
        auto &slotIdIn = request.release(i);
        hippo::SlotId slotId;
        ProtoWrapper::convert(slotIdIn, &slotId);
        HIPPO_LOG(INFO, "release slot, slot address:%s, slot id:%d",
                  slotId.slaveAddress.c_str(), slotId.id);
        auto it = _assignedSlot2Role.find(slotId);
        if (it != _assignedSlot2Role.end()) {
            auto& role = it->second;
            HIPPO_LOG(INFO, "release slot of role:%s, slot address:%s, slot id:%d",
                      role.c_str(), slotId.slaveAddress.c_str(), slotId.id);
            auto rIt = _assignedSlots.find(role);
            if (rIt != _assignedSlots.end()) {
                rIt->second.erase(slotId);
            }
            if (rIt->second.empty()) {
                _assignedSlots.erase(rIt);
            }
            _assignedSlot2Role.erase(it);
        }
        clearExclusiveTag(slotId);
        clearIpIdx(slotId.slaveAddress, slotId.id);
    }
    for (int i = 0; i < request.reserveslot_size(); ++i) {
        auto &reserveSlot = request.reserveslot(i);
        auto &slotIdIn = reserveSlot.slotid();
        hippo::SlotId slotId;
        ProtoWrapper::convert(slotIdIn, &slotId);
        HIPPO_LOG(INFO, "reserve slot, slot address:%s, slot id:%d",
                  slotId.slaveAddress.c_str(), slotId.id);
        auto it = _assignedSlot2Role.find(slotId);
        if (it != _assignedSlot2Role.end()) {
            auto& role = it->second;
            HIPPO_LOG(INFO, "reserve slot of role:%s, slot address:%s, slot id:%d",
                      role.c_str(), slotId.slaveAddress.c_str(), slotId.id);
            auto rIt = _assignedSlots.find(role);
            if (rIt != _assignedSlots.end()) {
                rIt->second.erase(slotId);
            }
            if (rIt->second.empty()) {
                _assignedSlots.erase(rIt);
            }
            _assignedSlot2Role.erase(it);
        }
        clearExclusiveTag(slotId);
        clearIpIdx(slotId.slaveAddress, slotId.id);
    }
}

void DefaultSlotAssigner::clearExclusiveTag(const hippo::SlotId &slotId) {
    string exclusiveTag;
    getExclusiveTag(slotId, exclusiveTag);
    if (!exclusiveTag.empty()) {
        _assignedSlot2ExclusiveTags.erase(slotId);
        clearIpExclusiveTag(slotId.slaveAddress, exclusiveTag);
    }

}

void DefaultSlotAssigner::clearIpIdx(const string& ip, int32_t id) {
    auto idxIt = _ip2SlotIdxs.find(ip);
    if (idxIt != _ip2SlotIdxs.end()) {
        idxIt->second.erase(id);
        if (idxIt->second.empty()) {
            _ip2SlotIdxs.erase(idxIt);
        }
    }
}

void DefaultSlotAssigner::clearIpExclusiveTag(const string& ip, const string &exclusiveTag) {
    auto idxIt = _ip2ExclusiveTags.find(ip);
    if (idxIt != _ip2ExclusiveTags.end()) {
        idxIt->second.erase(exclusiveTag);
        if (idxIt->second.empty()) {
            _ip2ExclusiveTags.erase(idxIt);
        }
    }
}

void DefaultSlotAssigner::assignSlots(const proto::AllocateRequest &request,
                                   proto::AllocateResponse *response)
{
    for (int i = 0; i < request.require_size(); ++i) {
        auto& resourceRequest = request.require(i);
        auto resourceResponse = response->add_assignedresources();
        assignRoleSlots(resourceRequest, resourceResponse);
    }
}

void DefaultSlotAssigner::assignRoleSlots(
        const proto::ResourceRequest &resourceRequest,
        proto::ResourceResponse *resourceResponse)
{
    const string& role = resourceRequest.tag();
    HIPPO_LOG(INFO, "assign slots for role:%s", role.c_str());
    int32_t replicaCount = resourceRequest.count();
    int32_t currentCount = 0;
    if (replicaCount > 0) {
        resourceResponse->set_resourcetag(role);
    }
    string resourceName;
    if (!getFirstSpecifiedResource(resourceRequest,
                                   hippo::proto::Resource::TEXT, resourceName))
    {
        resourceName = DEFAULT_RESOURCE;
    }
    string exclusiveTag;
    if (!getFirstSpecifiedResource(resourceRequest,
                                   hippo::proto::Resource::EXCLUSIVE,
                                   exclusiveTag))
    {
        exclusiveTag.clear();
    }

    auto it = _assignedSlots.find(role);
    if (it != _assignedSlots.end()) {
        set<SlotId> &roleAssignedSlots = it->second;
        auto ipsIt = _ipSet.find(resourceName);
        for (auto slotIt = roleAssignedSlots.begin(); slotIt != roleAssignedSlots.end(); )
        {
            const string& ip = slotIt->slaveAddress;
            if (currentCount >= replicaCount // replica desc
                || ipsIt == _ipSet.end()  // resource name changed
                || ipsIt->second.count(ip) == 0) //machine has been deleted
            {
                clearIpIdx(ip, slotIt->id);
                clearIpExclusiveTag(ip, exclusiveTag);
                _assignedSlot2ExclusiveTags.erase(*slotIt);
                roleAssignedSlots.erase(slotIt++);
                continue;
            }
            proto::AssignedSlot* newSlot = resourceResponse->add_assignedslots();
            fillAssignedSlot(resourceRequest, *slotIt, newSlot);
            currentCount++;
            slotIt++;
        }
    }
    while (currentCount < replicaCount) {
        if (!assignNewSlot(resourceRequest, resourceResponse)) {
            return;
        }
        currentCount++;
    }
}

bool DefaultSlotAssigner::assignNewSlot(const proto::ResourceRequest &resourceRequest,
                                        proto::ResourceResponse *resourceResponse)
{
    string resourceName;
    if (!getFirstSpecifiedResource(resourceRequest,
                                   hippo::proto::Resource::TEXT,
                                   resourceName))
    {
        resourceName = DEFAULT_RESOURCE;
    }

    string exclusiveTag;
    if (!getFirstSpecifiedResource(resourceRequest,
                                   hippo::proto::Resource::EXCLUSIVE,
                                   exclusiveTag))
    {
        exclusiveTag.clear();
    }
    HIPPO_LOG(DEBUG, "alloc request with exclusive resource[%s], text resource[%s]",
              exclusiveTag.c_str(), resourceName.c_str());

    auto it = _ipInfos.find(resourceName);
    if (it == _ipInfos.end()) {
        HIPPO_LOG(ERROR, "invalid resource tag[%s]", resourceName.c_str());
        return false;
    }
    string ip;
    if (!getFreeNode(it->second, exclusiveTag, ip)) {
        HIPPO_LOG(ERROR, "candidate node of tag[%s] not enough", resourceName.c_str());
        return false;
    }
    int32_t slotIdx = getSlotIdx(ip);
    proto::AssignedSlot* newSlot = resourceResponse->add_assignedslots();
    SlotId slotId;
    slotId.slaveAddress = ip;
    slotId.id = slotIdx;
    slotId.declareTime = TimeUtility::currentTimeInSeconds();
    fillAssignedSlot(resourceRequest, slotId, newSlot);
    const string& role = resourceRequest.tag();
    _assignedSlots[role].insert(slotId);
    _assignedSlot2Role[slotId] = role;
    if (!exclusiveTag.empty()) {
        _ip2ExclusiveTags[ip].insert(exclusiveTag);
        _assignedSlot2ExclusiveTags[slotId] = exclusiveTag;
    }
    return true;
}

bool DefaultSlotAssigner::getFirstSpecifiedResource(
        const proto::ResourceRequest &resourceRequest,
        hippo::proto::Resource::Type resourceType,
        string &resourceName)
{
    for (int i = 0; i < resourceRequest.options_size(); ++i) {
        auto &slotResource = resourceRequest.options(i);
        for (int j = 0; j < slotResource.resources_size(); ++j) {
            auto& resource = slotResource.resources(j);
            if (resource.type() == resourceType) {
                resourceName = resource.name();
                return true;
            }
        }
    }
    return false;
}

bool DefaultSlotAssigner::getFreeNode(const vector<string> &ipVec,
                                      const string &exclusiveTag, string &ip)
{
    int32_t idx = -1;
    int32_t slotCount = MAX_SLOT_IDX;
    for (int32_t i = 0; i < ipVec.size(); ++i) {
        HIPPO_LOG(INFO, "begin check slot count for ip:%s", ipVec[i].c_str());
        if (!exclusiveTag.empty()) {
            auto tagIt = _ip2ExclusiveTags.find(ipVec[i]);
            if (tagIt == _ip2ExclusiveTags.end() || tagIt->second.count(exclusiveTag) == 0) {
                auto it = _ip2SlotIdxs.find(ipVec[i]);
                if (it == _ip2SlotIdxs.end()) {
                    idx = i;
                    break;
                } else if (it->second.size() < slotCount) {
                    slotCount = it->second.size();
                    idx = i;
                }
            }
        } else {
            auto it = _ip2SlotIdxs.find(ipVec[i]);
            if (it == _ip2SlotIdxs.end()) {
                idx = i;
                break;
            } else if (!_multiSlotsOneNode) {
                continue;
            } else if (it->second.size() < slotCount) {
                slotCount = it->second.size();
                idx = i;
            }
        }
    }
    if (idx >= 0) {
        ip = ipVec[idx];
        return true;
    }
    return false;
}

int32_t DefaultSlotAssigner::getSlotIdx(const string& ip) {
    static int32_t MIN_SLOT_IDX = 0;
    static int32_t MAX_SLOT_IDX = 1000000;

    int32_t idx = MIN_SLOT_IDX;
    set<int32_t> &slotIdxSet = _ip2SlotIdxs[ip];
    for (; idx < MAX_SLOT_IDX; ++idx) {
        if (slotIdxSet.count(idx) == 0) {
            break;
        }
    }
    assert(idx < MAX_SLOT_IDX);
    slotIdxSet.insert(idx);
    return idx;
}

void DefaultSlotAssigner::fillAssignedSlot(
        const proto::ResourceRequest &resourceRequest,
        const hippo::SlotId &slotIdIn,
        proto::AssignedSlot *slot)
{
    slot->set_applicationid(_applicationId);
    proto::SlotId* slotIdOut = slot->mutable_id();
    slotIdOut->set_slaveaddress(slotIdIn.slaveAddress);
    slotIdOut->set_id(slotIdIn.id);
    slotIdOut->set_declaretime(slotIdIn.declareTime);
    slot->set_launchsignature(resourceRequest.launchsignature());
    slot->set_packagechecksum(resourceRequest.packagechecksum());
    auto packageStatus = slot->mutable_packagestatus();
    packageStatus->set_status(hippo::proto::PackageStatus::IS_INSTALLED);
    proto::SlaveStatus* slaveStatus = slot->mutable_slavestatus();
    slaveStatus->set_status(proto::SlaveStatus::ALIVE);
    auto status = getProcessStatus(slotIdIn, resourceRequest.launchsignature());
    if (resourceRequest.has_launchtemplate()) {
        auto launchTemplate = resourceRequest.launchtemplate();
        for (int i = 0; i < launchTemplate.processes_size(); ++i) {
            auto process = launchTemplate.processes(i);
            auto processStatus = slot->add_processstatus();
            processStatus->set_isdaemon(process.isdaemon());
            processStatus->set_status(status);
            processStatus->set_processname(process.processname());
            processStatus->set_instanceid(process.instanceid());
        }
    }
    if (resourceRequest.options_size() > 0) {
        auto& requireResource = resourceRequest.options(0);
        auto slotResource = slot->mutable_slotresource();
        for (int i = 0; i < requireResource.resources_size(); ++i) {
            auto oneRes = requireResource.resources(i);
            auto resource = slotResource->add_resources();
            resource->set_type(oneRes.type());
            resource->set_name(oneRes.name());
            resource->set_amount(oneRes.amount());
        }
    }
}

proto::ProcessStatus::Status DefaultSlotAssigner::getProcessStatus(
        const hippo::SlotId &slotId,
        int64_t launchSignature)
{
    auto it = _launchedMetas.find(slotId);
    if (it != _launchedMetas.end())
    {
        return proto::ProcessStatus::PS_RUNNING;
    }
    return proto::ProcessStatus::PS_UNKNOWN;
}

void DefaultSlotAssigner::backUpAssignedResource() {
    string content;
    serializeAssignedResource(content);
    _assignedResSerializer->write(content);
}

void DefaultSlotAssigner::deserializeIpInfo(const string& ipInfoStr,
        map<string, vector<string> > &ipInfo)
{
    if (ipInfoStr.empty()) {
        return;
    }
    rapidjson::Document doc;
    if (!JsonUtil::fromJson(ipInfoStr, doc)) {
        HIPPO_LOG(ERROR, "deserialize ipInfo failed, invalid json[%s]",
                  ipInfoStr.c_str());
        return;
    }
    if (!doc.IsObject()) {
        HIPPO_LOG(ERROR, "deserialize ipInfo failed, [%s] is not object",
                  ipInfoStr.c_str());
        return;
    }
    for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
        string tag = it->name.GetString();
        const auto &ipsValue = it->value;
        if (!ipsValue.IsArray()) {
            continue;
        }
        for (auto iter = ipsValue.Begin(); iter != ipsValue.End(); ++iter) {
            string ip = iter->GetString();
            StringUtil::trim(ip);
            ipInfo[tag].push_back(ip);
        }
    }
}

void DefaultSlotAssigner::serializeAssignedResource(string& content) const
{
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    for (auto it = _assignedSlots.begin(); it != _assignedSlots.end(); ++it) {
        auto &slotIdSet = it->second;
        if (slotIdSet.empty()) {
            continue;
        }
        rapidjson::Value ipsValue;
        ipsValue.SetArray();
        for (auto &slotId : slotIdSet) {
            string exclusiveTag;
            getExclusiveTag(slotId, exclusiveTag);
            string slotIdStr;
            slotIdToString(slotId, exclusiveTag, slotIdStr);

            rapidjson::Value slotIdValue(slotIdStr.c_str(), slotIdStr.size(), allocator);
            ipsValue.PushBack(slotIdValue, allocator);
        }
        rapidjson::Value name(it->first.c_str(), it->first.size(), allocator);
        doc.AddMember(name, ipsValue, allocator);
    }
    JsonUtil::toJson(doc, content);
}

void DefaultSlotAssigner::deserializeAssignedResource(
        const string& assignedResource)
{
    if (assignedResource.empty()) {
        return;
    }
    HIPPO_LOG(INFO, "deserialize assigned resource:%s", assignedResource.c_str());
    rapidjson::Document doc;
    if (!JsonUtil::fromJson(assignedResource, doc)) {
        HIPPO_LOG(ERROR, "deserialize Slots failed, invalid json[%s]",
                  assignedResource.c_str());
        return;
    }
    if (!doc.IsObject()) {
        HIPPO_LOG(ERROR, "deserialize Slots failed, [%s] is not object",
                  assignedResource.c_str());
    }
    for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
        string role = it->name.GetString();
        const auto &ipsValue = it->value;
        if (!ipsValue.IsArray()) {
            continue;
        }
        for (auto iter = ipsValue.Begin(); iter != ipsValue.End(); ++iter) {
            string slotIdStr = iter->GetString();
            hippo::SlotId slotId;
            string exclusiveTag;
            slotIdFromString(slotIdStr, slotId, exclusiveTag);
            HIPPO_LOG(INFO, "set slot idx to buffer, address:%s id:%d tag:%s",
                      slotId.slaveAddress.c_str(), slotId.id, exclusiveTag.c_str());
            _ip2SlotIdxs[slotId.slaveAddress].insert(slotId.id);
            _assignedSlots[role].insert(slotId);
            _assignedSlot2Role[slotId] = role;
            if (!exclusiveTag.empty()) {
                _ip2ExclusiveTags[slotId.slaveAddress].insert(exclusiveTag);
                _assignedSlot2ExclusiveTags[slotId] = exclusiveTag;
            }
        }
    }
}

void DefaultSlotAssigner::getExclusiveTag(const hippo::SlotId &slotId,
        string &exclusiveTag) const
{
    auto it = _assignedSlot2ExclusiveTags.find(slotId);
    if (it != _assignedSlot2ExclusiveTags.end()) {
        exclusiveTag = it->second;
    }
}

void DefaultSlotAssigner::slotIdToString(const hippo::SlotId &slotId,
        const string &exclusiveTag,
        string &slotIdStr) const
{
    slotIdStr.clear();
    slotIdStr += slotId.slaveAddress + "&" + StringUtil::toString(slotId.id)
                 + "&" + StringUtil::toString(slotId.declareTime);
    if (!exclusiveTag.empty()) {
        slotIdStr += "&" + exclusiveTag;
    }
}

void DefaultSlotAssigner::slotIdFromString(string &slotIdStr,
        hippo::SlotId &slotId,
        string &exclusiveTag)

{
    vector<string> items = StringUtil::split(slotIdStr, "&");
    if (items.size() > 0) {
        slotId.slaveAddress = items[0];
    }

    if (items.size() > 1) {
        slotId.id = StringUtil::fromString<int32_t>(items[1]);
    }

    if (items.size() > 2) {
        slotId.declareTime = StringUtil::fromString<int64_t>(items[2]);
    }
    if (items.size() > 3) {
        exclusiveTag = items[3];
    }
}

void DefaultSlotAssigner::setIpInfo(const map<string, vector<string> > &ipInfos) {
    _ipSet.clear();
    _ipInfos.clear();
    for (const auto &ipInfo : ipInfos) {
        HIPPO_LOG(DEBUG, "set ip list:[%s], tag:[%s]",
                  StringUtil::join(ipInfo.second, ",").c_str(),
                  ipInfo.first.c_str());
        for (const auto &ip : ipInfo.second) {
            _ipSet[ipInfo.first].insert(ip);
        }
        if (!ipInfo.second.empty()) {
            _ipInfos[ipInfo.first] = ipInfo.second;
        }
    }
}

//no use
void DefaultSlotAssigner::recoverAssignedResource(
        const map<string, vector<string> > &role2Ips)
{
    //set when recover
    if (!_assignedSlots.empty()) {
        return;
    }
    for (const auto& role2Ip : role2Ips) {
        const string& role = role2Ip.first;
        for (const auto& ip : role2Ip.second) {
            hippo::SlotId slotId;
            slotId.slaveAddress = ip;
            slotId.id = getSlotIdx(ip);
            slotId.declareTime = TimeUtility::currentTimeInSeconds();
            HIPPO_LOG(INFO, "recover slot, slave address:%s slot id:%d",
                      slotId.slaveAddress.c_str(), slotId.id);
            _assignedSlots[role].insert(slotId);
            _assignedSlot2Role[slotId] = role;
        }
    }
}

void DefaultSlotAssigner::setLaunchedMetas(const map<hippo::SlotId, LaunchMeta> &launchedMetas)
{
    _launchedMetas = launchedMetas;
}

END_HIPPO_NAMESPACE(sdk);
