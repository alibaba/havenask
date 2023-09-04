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
#include "proto_helper/ResourceHelper.h"
#include "autil/StringUtil.h"
#include "autil/HashAlgorithm.h"
#include "autil/legacy/string_tools.h"
#include <regex>
using namespace std;
using namespace autil;

BEGIN_HIPPO_NAMESPACE(proto);
HIPPO_LOG_SETUP(master, ResourceHelper);

Resource ResourceHelper::prepareResource(const string &description,
        const string &separator)
{
    Resource resource;
    vector<string> v = StringUtil::split(description, separator);
    if (v.size() != 3) {
        HIPPO_LOG(ERROR, "ResourceDescription[%s] not macth format!",
                  description.c_str());
    }
    StringUtil::toLowerCase(v[0]);
    assert(v[0] == "scalar" || v[0] == "text" || v[0] == "exclusive"
           || v[0] == "exclude_text" || v[0] == "queue_name"
           || v[0] == "scalar_cmp" || v[0] == "prefer_text"
           || v[0] == "prohibit_text");
    if (v[0] == "scalar") {
        resource.set_type(Resource::SCALAR);
    } else if (v[0] == "text") {
        resource.set_type(Resource::TEXT);
    } else if (v[0] == "exclude_text") {
        resource.set_type(Resource::EXCLUDE_TEXT);
    } else if (v[0] == "prefer_text") {
        resource.set_type(Resource::PREFER_TEXT);
    } else if (v[0] == "prohibit_text") {
        resource.set_type(Resource::PROHIBIT_TEXT);
    } else if (v[0] == "scalar_cmp") {
        resource.set_type(Resource::SCALAR_CMP);
    } else if (v[0] == "exclusive") {
        resource.set_type(Resource::EXCLUSIVE);
    } else if (v[0] == "queue_name"){
        resource.set_type(Resource::QUEUE_NAME);
    } else {
        HIPPO_LOG(ERROR, "invalid resource type[%s]", v[0].c_str());
        abort();
    }
    resource.set_name(v[1]);
    int32_t amount;
    bool ret = StringUtil::strToInt32(v[2].c_str(), amount);
    assert(ret); (void)ret;
    resource.set_amount(amount);
    return resource;
}

SlaveResource ResourceHelper::prepareSlaveResource(const string &description,
        const string &separator)
{
    SlaveResource resource;
    vector<string> resourceDescription = StringUtil::split(description, "@");

    vector<string> v = StringUtil::split(resourceDescription[0], separator);
    for (size_t i = 0; i < v.size(); ++i) {
        Resource *res = resource.add_resources();
        (*res) = ResourceHelper::prepareResource(v[i]);
    }

    if (resourceDescription.size() < 2) {
        return resource;
    }
    v.clear();
    v = StringUtil::split(resourceDescription[1], separator);
    for (size_t i = 0; i < v.size(); ++i) {
        Resource *res = resource.add_reservedresources();
        (*res) = ResourceHelper::prepareResource(v[i]);
    }

    return resource;
}

ResourceMap ResourceHelper::prepareResourceMap(const string &description,
        const string &separator)
{
    ResourceMap resource;
    vector<string> v = StringUtil::split(description, separator);
    for (size_t i = 0; i < v.size(); ++i) {
        Resource res = ResourceHelper::prepareResource(v[i]);
        resource[res.name()] = res;
    }
    return resource;
}

SlotResource ResourceHelper::prepareSlotResource(const string &description,
        const string &separator)
{
    SlotResource resource;
    vector<string> v = StringUtil::split(description, separator);
    for (size_t i = 0; i < v.size(); ++i) {
        Resource *res = resource.add_resources();
        (*res) = ResourceHelper::prepareResource(v[i]);
    }
    return resource;
}

string ResourceHelper::normalizeResources(const ResourceMap &resources) {
    if (resources.size() == 0) {
         return DEFAULT_RESOURCE_GROUP;
    }
    string desc("");
    ResourceMap::const_iterator it = resources.begin();
    while (it != resources.end()) {
        desc += it->second.ShortDebugString();
        desc += ";";
        it++;
    }
    return desc;
 }

string ResourceHelper::normalizeResources(const proto::SlotResource &slotResource) {
    if (slotResource.resources_size() == 0) {
        return DEFAULT_RESOURCE_GROUP;
    }
    string desc("");
    for (int i = 0; i < slotResource.resources_size(); ++i) {
        desc += slotResource.resources(i).ShortDebugString() + ";";
    }
    return desc;
}

string ResourceHelper::ResourceOptionsToString(
        const ResourceOptions &slotResources) {
    string result("");
    for (int i = 0; i < slotResources.size(); ++i) {
        result += "(";
        result += normalizeResources(slotResources.Get(i));
        result += ")";
    }
    return result;
}

string ResourceHelper::toString(const proto::Resource &resource) {
    string type;
    if (!convertResourceType(resource.type(), &type, false)) {
        return "";
    }
    string amount = autil::StringUtil::toString(resource.amount());
    string res = type + "," + resource.name() + ","+ amount;
    return res;
}

string ResourceHelper::toString(const ResourceMap &resources) {
    ResourceMap::const_iterator it = resources.begin();
    string result = "{";
    for (; it != resources.end(); ++it) {
        string res = toString(it->second);
        if (result == "{") {
            result = result + res;
        } else {
            result = result + ";" + res;
        }
    }
    result += "}";
    return result;
}

string ResourceHelper::toString(const proto::SlotResource &slotResource) {
    string result = "{";
    for (int32_t i = 0; i < slotResource.resources_size(); ++i) {
        const proto::Resource &resource = slotResource.resources(i);
        string res = toString(resource);
        if (result == "{") {
            result = result + res;
        } else {
            result = result + ";" + res;
        }
    }
    result += "}";
    return result;
}

void ResourceHelper::logResource(const proto::Resource &resource) {
    string type = "text";
    if (!convertResourceType(resource.type(), &type, false)) {
        return;
    }
    HIPPO_LOG(INFO, "r: %s [%s, %d]", resource.name().c_str(),
              type.c_str(), resource.amount());
}

void ResourceHelper::logResourceMap(const ResourceMap &resourceMap) {
    ResourceMap::const_iterator it = resourceMap.begin();
    for (; it != resourceMap.end(); it++) {
        logResource(it->second);
    }
}

void ResourceHelper::logExclusiveResource(const StringSet &exclusives) {
    for (const auto &e : exclusives) {
        HIPPO_LOG(INFO, "r: %s [exclusive, 0]", e.c_str());
    }
}

void ResourceHelper::logDiskResources(const DiskResources &diskResources) {
    map<string,ResourceMap>::const_iterator it = diskResources.begin();
    for (; it != diskResources.end(); it++){
        HIPPO_LOG(INFO, "diskName: [%s]", it->first.c_str());
        ResourceMap::const_iterator it2 = it->second.begin();
        for (; it2 != it->second.end(); it2++) {
            logResource(it2->second);
        }
    }
}

void ResourceHelper::logSlotResource(const proto::SlotResource &slotResource) {
    for (int i = 0; i < slotResource.resources_size(); i++) {
        logResource(slotResource.resources(i));
    }
}

void ResourceHelper::logSlaveResource(const proto::SlaveResource &slaveResource) {
    for (int i = 0; i < slaveResource.resources_size(); i++) {
        logResource(slaveResource.resources(i));
    }
}

// resource judement functions
bool ResourceHelper::isDiskResource(const string &name) {
    if (name.compare(0, RES_NAME_DISK_PREFIX.size(),
                     RES_NAME_DISK_PREFIX) == 0)
    {
        return true;
    }
    return false;
}

bool ResourceHelper::isDiskSizeResource(const string &name) {
    if (name.compare(0, RES_NAME_DISK_SIZE.size(),
                     RES_NAME_DISK_SIZE) == 0) {
        return true;
    }
    return false;
}

bool ResourceHelper::isDiskPredictSizeResource(const string &name)
{
    return name.compare(0, RES_NAME_DISK_PREDICT_SIZE.size(),
                        RES_NAME_DISK_PREDICT_SIZE) == 0;
}

bool ResourceHelper::isDiskLimitSizeResource(const string &name)
{
    return name.compare(0, RES_NAME_DISK_LIMIT_SIZE.size(),
                        RES_NAME_DISK_LIMIT_SIZE) == 0;
}

bool ResourceHelper::isDiskIopsResource(const string &name) {
    if (name.compare(0, RES_NAME_DISK_IOPS_PREFIX.size(),
                     RES_NAME_DISK_IOPS_PREFIX) == 0)
    {
        return true;
    }
    return false;
}

bool ResourceHelper::isDiskRatioResource(const string &name) {
    if (name.compare(0, RES_NAME_DISK_RATIO_PREFIX.size(),
                     RES_NAME_DISK_RATIO_PREFIX) == 0)
    {
        return true;
    }
    return false;
}

bool ResourceHelper::isIpResource(const string &name) {
    if (name.compare(0, RES_NAME_IP_PREFIX.size(), RES_NAME_IP_PREFIX) == 0) {
        return true;
    }
    return false;
}

bool ResourceHelper::isSnResource(const string &name) {
    return name.find(RES_NAME_SN_PREFIX) == 0;
}

bool ResourceHelper::isGpuResource(const string &name) {
    if (name.compare(0, NVIDIA_DEVICE_PATH_PREFIX.size(),
                NVIDIA_DEVICE_PATH_PREFIX) == 0) {
        return true;
    }
    return false;
}

bool ResourceHelper::isFpgaResource(const string &name) {
    if (name.compare(0, XILINX_DEVICE_PATH_PREFIX.size(),
                     XILINX_DEVICE_PATH_PREFIX) == 0) {
        return true;
    }
    return false;
}

bool ResourceHelper::isPovResource(const string &name) {
    if (name.compare(0, ALIPOV_DEVICE_PATH_PREFIX.size(),
                     ALIPOV_DEVICE_PATH_PREFIX) == 0) {
        return true;
    }
    return false;
}

bool ResourceHelper::isNpuResource(const string &name) {
    if (name.compare(0, ALINPU_DEVICE_PATH_PREFIX.size(),
                     ALINPU_DEVICE_PATH_PREFIX) == 0) {
        return true;
    }
    return false;
}

bool ResourceHelper::isSlaveDetectedResource(const string &name) {
    if (name == RES_NAME_CPU ||
        name == RES_NAME_MEM ||
        name == RES_NAME_NETWORK_BANDWIDTH ||
        name == RES_NAME_CPU_MODEL_NUM ||
        name == RES_NAME_T4 ||
        name == RES_NAME_DOCKER ||
        name == RES_NAME_UNDERLAY ||
        (isDiskResource(name) && !isDiskLimitSizeResource(name) && !isDiskPredictSizeResource(name)) ||
        isGpuResource(name) ||
        isFpgaResource(name) ||
        isPovResource(name) ||
        isNpuResource(name))
    {
        return true;
    }
    return false;
}

bool ResourceHelper::isMasterReservedResource(const string &name) {
    if (name == RES_NAME_PREDICT_CPU ||
        name == RES_NAME_CPU_LIMIT ||
        name == RES_NAME_PREDICT_MEM ||
        name == RES_NAME_MEM_LIMIT ||
        name == RES_NAME_GPU ||
        name == RES_NAME_FPGA ||
        name == RES_NAME_POV ||
        name == RES_NAME_NPU ||
        name == RES_NAME_IP ||
        isDiskPredictSizeResource(name) ||
        isDiskLimitSizeResource(name))
    {
        return true;
    }
    return false;
}

bool ResourceHelper::isAnonymousResource(const string &resName) {
    return resName == RES_NAME_IP ||
        resName == RES_NAME_GPU ||
        resName == RES_NAME_FPGA ||
        resName == RES_NAME_POV ||
        resName == RES_NAME_NPU ||
        resName == RES_NAME_DISK_SIZE ||
        resName == RES_NAME_DISK_IOPS ||
        resName == RES_NAME_DISK_RATIO ||
        resName == RES_NAME_DISK_PREDICT_SIZE ||
        resName == RES_NAME_DISK_LIMIT_SIZE;
}

bool ResourceHelper::isInternalResource(const string &name) {
    if (isSlaveDetectedResource(name) || isMasterReservedResource(name)) {
        return true;
    }
    return false;
}

bool ResourceHelper::isInternalScalarResource(const string &name) {
    if (name == RES_NAME_CPU ||
        name == RES_NAME_PREDICT_CPU ||
        name == RES_NAME_MEM ||
        name == RES_NAME_PREDICT_MEM ||
        name == RES_NAME_IP ||
        name == RES_NAME_FPGA ||
        name == RES_NAME_GPU ||
        name == RES_NAME_POV ||
        name == RES_NAME_NPU ||
        name == RES_NAME_UNDERLAY ||
        (isDiskResource(name) && !isDiskLimitSizeResource(name)))
    {
        return true;
    }
    return false;
}

bool ResourceHelper::isScheduleResource(const proto::Resource &resource) {
    if (resource.type() == Resource::EXCLUSIVE ||
        resource.type() == Resource::PREFER_TEXT ||
        resource.type() == Resource::PROHIBIT_TEXT)
    {
        return true;
    }
    if (resource.name().find(RES_NAME_SN_PREFIX) == 0) {
        return true;
    }
    return false;
}

bool ResourceHelper::isTmpScheduleResource(const proto::Resource &resource) {
    if (resource.type() == Resource::PREFER_TEXT ||
        resource.type() == Resource::PROHIBIT_TEXT)
    {
        return true;
    }
    if (resource.name().find(RES_NAME_SN_PREFIX) == 0) {
        return true;
    }
    return false;
}

// convert resources
bool ResourceHelper::convertResourceType(const proto::Resource::Type &type,
        string *out, bool upperCase)
{
    string tmp;
    if (proto::Resource::SCALAR == type) {
        tmp = "scalar";
    } else if (proto::Resource::TEXT == type) {
        tmp = "text";
    } else if (proto::Resource::EXCLUDE_TEXT == type) {
        tmp = "exclude_text";
    } else if (proto::Resource::QUEUE_NAME == type) {
        tmp = "queue_name";
    } else if (proto::Resource::EXCLUSIVE == type) {
        tmp = "exclusive";
    } else if (proto::Resource::PREFER_TEXT == type) {
        tmp = "prefer_text";
    } else if (proto::Resource::PROHIBIT_TEXT == type) {
        tmp = "prohibit_text";
    } else if (proto::Resource::SCALAR_CMP == type) {
        tmp = "scalar_cmp";
    } else {
        *out = "NOT_SUPPORT";
        return false;
    }
    if (upperCase) {
        StringUtil::toUpperCase(tmp.c_str(), *out);
    } else {
        *out = tmp;
    }
    return true;
}

bool ResourceHelper::convertResourceType(const string &type,
        proto::Resource::Type *out)
{
    string conType;
    StringUtil::toUpperCase(type.c_str(), conType);
    if ("SCALAR" == conType) {
        *out = proto::Resource::SCALAR;
    } else if ("TEXT" == conType) {
        *out = proto::Resource::TEXT;
    } else if ("EXCLUDE_TEXT" == conType) {
        *out = proto::Resource::EXCLUDE_TEXT;
    } else if ("QUEUE_NAME" == conType) {
        *out = proto::Resource::QUEUE_NAME;
    } else if ("EXCLUSIVE" == conType) {
        *out =  proto::Resource::EXCLUSIVE;
    } else if ("PREFER_TEXT" == conType) {
        *out = proto::Resource::PREFER_TEXT;
    } else if ("PROHIBIT_TEXT" == conType) {
        *out =  proto::Resource::PROHIBIT_TEXT;
    } else {
        *out = proto::Resource::SCALAR;
        return true;
    }
    return true;
}

proto::PackageInfo::PackageType ResourceHelper::convertPackageType(const string &type) {
    string lowerType = type;
    StringUtil::toLowerCase(lowerType);
    if (lowerType == "file") {
        return proto::PackageInfo::FILE;
    } else if (lowerType == "dir") {
        return proto::PackageInfo::DIR;
    } else if (lowerType == "rpm") {
        return proto::PackageInfo::RPM;
    } else if (lowerType == "image") {
        return proto::PackageInfo::IMAGE;
    } else if (lowerType == "archive") {
        return proto::PackageInfo::ARCHIVE;
    } else {
        return proto::PackageInfo::UNSUPPORT;
    }
}

ResourceMap ResourceHelper::convertSlotResource(
        const proto::SlotResource &slotResource)
{
    ResourceMap resourceMap;
    for (int i = 0; i < slotResource.resources_size(); i++) {
        const string &name = slotResource.resources(i).name();
        resourceMap[name] = slotResource.resources(i);
    }
    return resourceMap;
}

ResourceMap ResourceHelper::convertSlotResourceWithAdjustType(
        const proto::SlotResource &slotResource,
        const ResourceMap &resources)
{
    ResourceMap resourceMap;
    for (int i = 0; i < slotResource.resources_size(); i++) {
        const string &name = slotResource.resources(i).name();
        resourceMap[name] = slotResource.resources(i);
        if (isScheduleResource(slotResource.resources(i))) {
            continue;
        }
        // amend request resource type consistent with slave resources
        ResourceMap::const_iterator it = resources.find(name);
        if (it != resources.end()) {
            resourceMap[name].set_type(it->second.type());
        }
    }
    return resourceMap;
}

ResourceMap ResourceHelper::convertSlotResourceWithAdjustType(
        const ResourceMap &require,
        const ResourceMap &resources)
{
    ResourceMap resourceMap;
    ResourceMap::const_iterator it = require.begin();
    for (; it != require.end(); ++it) {
        const string &name = it->first;
        resourceMap[name] = it->second;
        if (isScheduleResource(it->second)) {
            continue;
        }
        // amend request resource type consistent with slave resources
        ResourceMap::const_iterator it = resources.find(name);
        if (it != resources.end()) {
            resourceMap[name].set_type(it->second.type());
        }
    }
    return resourceMap;
}

void ResourceHelper::correctResourcesType(
        const ResourceMap &patterns,
        ResourceMap *resources)
{
    ResourceMap::iterator it = resources->begin();
    for (; it != resources->end(); ++it) {
        if (isScheduleResource(it->second)) {
            continue;
        }
        ResourceMap::const_iterator cpIt = patterns.find(it->first);
        if (cpIt != patterns.end()) {
            it->second.set_type(cpIt->second.type());
        }
    }
}

proto::Resource* ResourceHelper::findResource(proto::SlotResource *slotResource,
        const string &resourceName)
{
    if (isDiskResource(resourceName)) {
        return findDiskResource(slotResource, resourceName);
    }
    for (int32_t idx = 0; idx < slotResource->resources_size(); ++idx) {
        if (slotResource->resources(idx).name() == resourceName) {
            return slotResource->mutable_resources(idx);
        }
    }
    return NULL;
}

proto::Resource* ResourceHelper::findDiskResource(proto::SlotResource *slotResource,
        const string &resourceName)
{
    for (int32_t idx = 0; idx < slotResource->resources_size(); ++idx) {
        const Resource &resource = slotResource->resources(idx);
        const string &name = resource.name();
        if (isDiskResource(name)) {
            if (name.find(resourceName) == 0) {
                return slotResource->mutable_resources(idx);
            }
        }
    }
    return NULL;
}

const proto::Resource* ResourceHelper::findDiskResource(const proto::ResourceMap &resMap,
        const string &resourceName)
{
    for (auto &it : resMap) {
        const proto::Resource &res = it.second;
        const string &name = it.first;
        if (isDiskResource(name)) {
            if (name.find(resourceName) == 0) {
                return &res;
            }
        }
    }
    return NULL;
}

vector<proto::Resource> ResourceHelper::findAllDiskResource(const proto::ResourceMap &resMap,
        const string &resourceName)
{
    vector<proto::Resource> resVec;
    for (const auto &it : resMap) {
        const proto::Resource &res = it.second;
        const string &name = it.first;
        if (isDiskResource(name)) {
            if (name.find(resourceName) == 0) {
                resVec.push_back(res);
            }
        }
    }
    return resVec;
}

ResourceMap ResourceHelper::getDiskRequirement(const SlotResource &slotResource) {
    ResourceMap resourceMap;
    for (int i = 0; i < slotResource.resources_size(); ++i) {
        const Resource &resource = slotResource.resources(i);
        if (isDiskResource(resource.name())) {
            resourceMap[resource.name()] = resource;
        }
    }
    return resourceMap;
}

void ResourceHelper::getDiskAssignment(const SlotResource &slotResource,
                                       string *name, int32_t *size,
                                       int32_t *iops, int32_t *ratio)
{
    string diskName;
    for (int i = 0; i < slotResource.resources_size(); ++i) {
        const Resource &resource = slotResource.resources(i);
        if (!isDiskResource(resource.name())) {
            continue;
        }
        const string &diskResourceName = resource.name();
        if (diskResourceName.find(RES_NAME_DISK_SIZE_PREFIX) == 0) {
            string tmp = diskResourceName.substr(RES_NAME_DISK_SIZE_PREFIX.size());
            if (diskName.empty()) {
                diskName = tmp;
            } else {
                assert(diskName == tmp);
            }
            *size = resource.amount();
        } else if (diskResourceName.find(RES_NAME_DISK_IOPS_PREFIX) == 0) {
            string tmp = diskResourceName.substr(RES_NAME_DISK_IOPS_PREFIX.size());
            if (diskName.empty()) {
                diskName = tmp;
            } else {
                assert(diskName == tmp);
            }
            *iops = resource.amount();
        } else if (diskResourceName.find(RES_NAME_DISK_RATIO_PREFIX) == 0) {
            string tmp = diskResourceName.substr(RES_NAME_DISK_RATIO_PREFIX.size());
            if (diskName.empty()) {
                diskName = tmp;
            } else {
                assert(diskName == tmp);
            }
            *ratio = resource.amount();
        }
    }
    if (diskName.empty()) {
        *name = BOOT_DISK_NAME;
        return;
    }
    *name = diskName;
    return;
}

// If diskName is empty, return an annoymous disk resource's name
string ResourceHelper::generateDiskResourceName(const string &resourceName,
                                                const string &diskName)
{
    if (!isDiskResource(resourceName)) {
        return "";
    }
    string diskResourceSuffix = "";
    if (!diskName.empty()) {
        diskResourceSuffix = "_" + diskName;
    }
    return stripDiskName(resourceName) + diskResourceSuffix;
}

string ResourceHelper::generateDiskResourceNameIfEmpty(const string &resourceName,
                                                       const string &diskName)
{
    if (!isDiskResource(resourceName)) {
        return "";
    }
    string diskResourceSuffix = "";
    if (!diskName.empty()) {
        diskResourceSuffix = "_" + diskName;
    }
    string specifiedDisk = getDiskName(resourceName);
    if (specifiedDisk.empty()) {
        return resourceName + diskResourceSuffix;
    }
    return resourceName;
}

string ResourceHelper::getDiskName(const string &diskResourceName) {
    string diskName("");
    if (diskResourceName.compare(0, RES_NAME_DISK_SIZE_PREFIX.size(),
                RES_NAME_DISK_SIZE_PREFIX) == 0) {
        diskName = diskResourceName.substr(RES_NAME_DISK_SIZE_PREFIX.size());
    } else if (diskResourceName.compare(0, RES_NAME_DISK_RATIO_PREFIX.size(),
                RES_NAME_DISK_RATIO_PREFIX) == 0) {
        diskName = diskResourceName.substr(RES_NAME_DISK_RATIO_PREFIX.size());
    } else if (diskResourceName.compare(0, RES_NAME_DISK_READ_IOPS_PREFIX.size(),
                RES_NAME_DISK_READ_IOPS_PREFIX) == 0) {
        diskName = diskResourceName.substr(RES_NAME_DISK_READ_IOPS_PREFIX.size());
    } else if (diskResourceName.compare(0, RES_NAME_DISK_WRITE_IOPS_PREFIX.size(),
                RES_NAME_DISK_WRITE_IOPS_PREFIX) == 0) {
        diskName = diskResourceName.substr(RES_NAME_DISK_WRITE_IOPS_PREFIX.size());
    } else if (diskResourceName.compare(0, RES_NAME_DISK_READ_BPS_PREFIX.size(),
                RES_NAME_DISK_READ_BPS_PREFIX) == 0) {
        diskName = diskResourceName.substr(RES_NAME_DISK_READ_BPS_PREFIX.size());
    } else if (diskResourceName.compare(0, RES_NAME_DISK_WRITE_BPS_PREFIX.size(),
                RES_NAME_DISK_WRITE_BPS_PREFIX) == 0) {
        diskName = diskResourceName.substr(RES_NAME_DISK_WRITE_BPS_PREFIX.size());
    } else if (diskResourceName.compare(0, RES_NAME_DISK_BUFFER_WRITE_BPS_PREFIX.size(),
                RES_NAME_DISK_BUFFER_WRITE_BPS_PREFIX) == 0) {
        diskName = diskResourceName.substr(RES_NAME_DISK_BUFFER_WRITE_BPS_PREFIX.size());
    } else if (diskResourceName.compare(0, RES_NAME_DISK_IOPS_PREFIX.size(),
                RES_NAME_DISK_IOPS_PREFIX) == 0) {
        diskName = diskResourceName.substr(RES_NAME_DISK_IOPS_PREFIX.size());
    } else if (diskResourceName.compare(0, RES_NAME_DISK_PREDICT_SIZE_PREFIX.size(),
                RES_NAME_DISK_PREDICT_SIZE_PREFIX) == 0) {
        diskName = diskResourceName.substr(RES_NAME_DISK_PREDICT_SIZE_PREFIX.size());
    } else if (diskResourceName.compare(0, RES_NAME_DISK_LIMIT_SIZE_PREFIX.size(),
                RES_NAME_DISK_LIMIT_SIZE_PREFIX) == 0) {
        diskName = diskResourceName.substr(RES_NAME_DISK_LIMIT_SIZE_PREFIX.size());
    }
    int32_t value;
    if (StringUtil::strToInt32(diskName.c_str(), value)) {
        return diskName;
    }
    // for ut, check diskNmae is only consists of alphabet
    if (regex_match(diskName, regex("^[A-Za-z]+$"))) {
        return diskName;
    }
    return "";
}

string ResourceHelper::stripDiskName(const string &resourceType) {
    string newResourceType = resourceType;
    if (resourceType.compare(0, RES_NAME_DISK_SIZE_PREFIX.size(),
                RES_NAME_DISK_SIZE_PREFIX) == 0) {
        newResourceType = RES_NAME_DISK_SIZE;
    } else if (resourceType.compare(0, RES_NAME_DISK_PREDICT_SIZE_PREFIX.size(),
                     RES_NAME_DISK_PREDICT_SIZE_PREFIX) == 0) {
        newResourceType = RES_NAME_DISK_PREDICT_SIZE;
    } else if (resourceType.compare(0, RES_NAME_DISK_LIMIT_SIZE_PREFIX.size(),
                     RES_NAME_DISK_LIMIT_SIZE_PREFIX) == 0) {
        newResourceType = RES_NAME_DISK_LIMIT_SIZE;
    } else if (resourceType.compare(0, RES_NAME_DISK_RATIO_PREFIX.size(),
                RES_NAME_DISK_RATIO_PREFIX) == 0) {
        newResourceType = RES_NAME_DISK_RATIO;
    } else if (resourceType.compare(0, RES_NAME_DISK_READ_IOPS_PREFIX.size(),
                RES_NAME_DISK_READ_IOPS_PREFIX) == 0) {
        newResourceType = RES_NAME_DISK_READ_IOPS;
    } else if (resourceType.compare(0, RES_NAME_DISK_WRITE_IOPS_PREFIX.size(),
                RES_NAME_DISK_WRITE_IOPS_PREFIX) == 0) {
        newResourceType = RES_NAME_DISK_WRITE_IOPS;
    } else if (resourceType.compare(0, RES_NAME_DISK_READ_BPS_PREFIX.size(),
                RES_NAME_DISK_READ_BPS_PREFIX) == 0) {
        newResourceType = RES_NAME_DISK_READ_BPS;
    } else if (resourceType.compare(0, RES_NAME_DISK_WRITE_BPS_PREFIX.size(),
                RES_NAME_DISK_WRITE_BPS_PREFIX) == 0) {
        newResourceType = RES_NAME_DISK_WRITE_BPS;
    } else if (resourceType.compare(0, RES_NAME_DISK_BUFFER_WRITE_BPS_PREFIX.size(),
                RES_NAME_DISK_BUFFER_WRITE_BPS_PREFIX) == 0) {
        newResourceType = RES_NAME_DISK_BUFFER_WRITE_BPS;
    } else if (resourceType.compare(0, RES_NAME_DISK_IOPS_PREFIX.size(),
                RES_NAME_DISK_IOPS_PREFIX) == 0) {
        newResourceType = RES_NAME_DISK_IOPS;
    }
    return newResourceType;
}

// convertDiskResource
bool ResourceHelper::stripDiskName(Resource &resource, string &diskName) {
    const string &name = resource.name();
    diskName = getDiskName(name);
    if (name.compare(0, RES_NAME_DISK_SIZE_PREFIX.size(),
                     RES_NAME_DISK_SIZE_PREFIX) == 0) {
        resource.set_name(RES_NAME_DISK_SIZE);
    } else if (name.compare(0, RES_NAME_DISK_PREDICT_SIZE_PREFIX.size(),
                     RES_NAME_DISK_PREDICT_SIZE_PREFIX) == 0) {
        resource.set_name(RES_NAME_DISK_PREDICT_SIZE);
    } else if (name.compare(0, RES_NAME_DISK_LIMIT_SIZE_PREFIX.size(),
                     RES_NAME_DISK_LIMIT_SIZE_PREFIX) == 0) {
        resource.set_name(RES_NAME_DISK_LIMIT_SIZE);
    } else if (name.compare(0, RES_NAME_DISK_RATIO_PREFIX.size(),
                            RES_NAME_DISK_RATIO_PREFIX) == 0) {
        resource.set_name(RES_NAME_DISK_RATIO);
    } else if (name.compare(0, RES_NAME_DISK_READ_IOPS_PREFIX.size(),
                            RES_NAME_DISK_READ_IOPS_PREFIX) == 0) {
        resource.set_name(RES_NAME_DISK_READ_IOPS);
    } else if (name.compare(0, RES_NAME_DISK_WRITE_IOPS_PREFIX.size(),
                            RES_NAME_DISK_WRITE_IOPS_PREFIX) == 0) {
        resource.set_name(RES_NAME_DISK_WRITE_IOPS);
    } else if (name.compare(0, RES_NAME_DISK_READ_BPS_PREFIX.size(),
                            RES_NAME_DISK_READ_BPS_PREFIX) == 0) {
        resource.set_name(RES_NAME_DISK_READ_BPS);
    } else if (name.compare(0, RES_NAME_DISK_WRITE_BPS_PREFIX.size(),
                            RES_NAME_DISK_WRITE_BPS_PREFIX) == 0) {
        resource.set_name(RES_NAME_DISK_WRITE_BPS);
    } else if (name.compare(0, RES_NAME_DISK_BUFFER_WRITE_BPS_PREFIX.size(),
                            RES_NAME_DISK_BUFFER_WRITE_BPS_PREFIX) == 0) {
        resource.set_name(RES_NAME_DISK_BUFFER_WRITE_BPS);
    } else if (name.compare(0, RES_NAME_DISK_IOPS_PREFIX.size(),
                            RES_NAME_DISK_IOPS_PREFIX) == 0) {
        resource.set_name(RES_NAME_DISK_IOPS);
    } else {
        HIPPO_LOG(ERROR, "name [%s] is invalid", name.c_str());
        return false;
    }
    return true;
}

void ResourceHelper::stripDiskName(ResourceMap &resources, string &diskName) {
    ResourceMap resultResources;
    ResourceMap::iterator it = resources.begin();
    for (; it != resources.end(); ++it) {
        Resource resource = it->second;
        stripDiskName(resource, diskName);
        resultResources[resource.name()] = resource;
    }
    resources = resultResources;
}

// join all disk resource with disk name
void ResourceHelper::joinDiskName(ResourceMap *resMap,
                                  const string &diskName)
{
    ResourceMap tmp = *resMap;
    resMap->clear();
    for (ResourceMap::const_iterator it = tmp.begin(); it != tmp.end(); ++it) {
        if (!isDiskResource(it->first)) {
            (*resMap)[it->first] = it->second;
            continue;
        }
        string name = generateDiskResourceName(it->first, diskName);
        proto::Resource &res = (*resMap)[name];
        res = it->second;
        res.set_name(name);
    }
}

string ResourceHelper::getDiskLocation(const ResourceMap &resMap) {
    // multi-disk maybe in machine mode should return BOOT_DISK
    if (resMap.find(BOOT_DISK_SIZE_RES_NAME) != resMap.end() ||
        resMap.find(BOOT_DISK_RATIO_RES_NAME) != resMap.end())
    {
        return BOOT_DISK_NAME;
    }

    ResourceMap::const_iterator it = resMap.begin();
    for (; it != resMap.end(); ++it) {
        if (isDiskResource(it->first)) {
            string diskName = getDiskName(it->first);
            if (!diskName.empty()) {
                return diskName;
            }
        }
    }
    return "";
}

set<string> ResourceHelper::getDisks(const ResourceMap &resMap) {
    set<string> disks;
    ResourceMap::const_iterator it = resMap.begin();
    for (; it != resMap.end(); ++it) {
        if (isDiskResource(it->first)) {
            string diskName = getDiskName(it->first);
            if (!diskName.empty()) {
                disks.insert(diskName);
            }
        }
    }
    return disks;
}

string ResourceHelper::getDiskLocation(const proto::SlotResource &slotResource)
{
    for (int i = 0; i < slotResource.resources_size(); ++i) {
        const string &name = slotResource.resources(i).name();
        if (isDiskResource(name)) {
            string diskName = getDiskName(name);
            if (!diskName.empty()) {
                return diskName;
            }
        }
    }
    return "";
}

int32_t ResourceHelper::getResourceTotalAmount(const ResourceMap &resources)
{
    int32_t total = 0;
    for (auto &e : resources) {
        if (e.second.amount() < 0) {
            continue;
        }
        total += e.second.amount();
    }
    return total;
}

// ip resources
bool ResourceHelper::getIpInformation(const string &ipResourceName, int num, string *value) {
    assert(ipResourceName.find(RES_NAME_IP_PREFIX) == 0);
    vector<string> items = StringUtil::split(ipResourceName,
            RES_NAME_IP_SEPARATOR, false);
    if (items.size() < (size_t)(num + 1)) {
        return false;
    }
    *value = items[num];
    return true;
}

bool ResourceHelper::getIp(const string &ipResourceName,
                           string *value)
{
    string ipStr;
    if (ResourceHelper::getIpInformation(ipResourceName, 0, &ipStr)) {
        vector<string> items = StringUtil::split(ipStr, RES_NAME_IP_PREFIX, false);
        if (items.size() == 2) {
            *value = items[1];
            return true;
        }
    }
    return false;
}

bool ResourceHelper::getGateWay(const string &ipResourceName,
                                string *value)
{
    return ResourceHelper::getIpInformation(ipResourceName, 1, value);
}

bool ResourceHelper::getSubnetMask(const string &ipResourceName,
                                   string *value)
{
    return ResourceHelper::getIpInformation(ipResourceName, 2, value);
}


bool ResourceHelper::getHostName(const string &ipResourceName,
                                 string *value)
{
    return ResourceHelper::getIpInformation(ipResourceName, 3, value);
}

bool ResourceHelper::getHostNic(const string &ipResourceName,
                                string *value)
{
    assert(ipResourceName.find(RES_NAME_IP_PREFIX) == 0);
    uint32_t num = 4;
    vector<string> items = StringUtil::split(ipResourceName,
            RES_NAME_IP_SEPARATOR, false);
    if (items.size() < num + 1) {
        return false;
    }
    value->clear();
    for (uint32_t i = num; i < items.size(); ++i) {
        if (i != num) {
            *value += RES_NAME_IP_SEPARATOR;
        }
        *value += items[i];
    }
    return true;
}

string ResourceHelper::generateIpResourceName(const string &ip) {
    return RES_NAME_IP_PREFIX + ip;
}

set<string> ResourceHelper::getIpResourceNames(const ResourceMap &resMap) {
    set<string> ips;
    ResourceMap::const_iterator it = resMap.begin();
    for (; it != resMap.end(); ++it) {
        if (isIpResource(it->first)) {
            // for master, return all infos
            ips.insert(it->first);
        }
    }
    return ips;
}

ResourceMap ResourceHelper::getIpResources(const ResourceMap &resMap) {
    ResourceMap ips;
    ResourceMap::const_iterator it = resMap.begin();
    for (; it != resMap.end(); ++it) {
        if (isIpResource(it->first)) {
            // for master, return all infos
            ips[it->first] = it->second;
        }
    }
    return ips;
}

set<string> ResourceHelper::getGpuResourceNames(const ResourceMap &resMap) {
    set<string> gpus;
    ResourceMap::const_iterator it = resMap.begin();
    for (; it != resMap.end(); ++it) {
        if (isGpuResource(it->first)) {
            // for master, return all infos
            gpus.insert(it->first);
        }
    }
    return gpus;
}

ResourceMap ResourceHelper::getGpuResources(const ResourceMap &resMap) {
    ResourceMap gpus;
    ResourceMap::const_iterator it = resMap.begin();
    for (; it != resMap.end(); ++it) {
        if (isGpuResource(it->first)) {
            // for master, return all infos
            gpus[it->first] = it->second;
        }
    }
    return gpus;
}

ResourceMap ResourceHelper::getFpgaResources(const ResourceMap &resMap) {
    ResourceMap fpgas;
    ResourceMap::const_iterator it = resMap.begin();
    for (; it != resMap.end(); ++it) {
        if (isFpgaResource(it->first)) {
            // for master, return all infos
            fpgas[it->first] = it->second;
        }
    }
    return fpgas;
}

ResourceMap ResourceHelper::getPovResources(const ResourceMap &resMap) {
    ResourceMap povs;
    ResourceMap::const_iterator it = resMap.begin();
    for (; it != resMap.end(); ++it) {
        if (isPovResource(it->first)) {
            // for master, return all infos
            povs[it->first] = it->second;
        }
    }
    return povs;
}

ResourceMap ResourceHelper::getNpuResources(const ResourceMap &resMap) {
    ResourceMap npus;
    ResourceMap::const_iterator it = resMap.begin();
    for (; it != resMap.end(); ++it) {
        if (isNpuResource(it->first)) {
            // for master, return all infos
            npus[it->first] = it->second;
        }
    }
    return npus;
}

bool ResourceHelper::isDiskResourceMatch(const ResourceMap &resources,
        const ResourceMap &require, string *filterLable)
{
    ResourceMap::const_iterator it = require.begin();
    for (; it != require.end(); ++it) {
        if (it->second.type() == Resource::SCALAR ||
            it->second.type() == Resource::SCALAR_CMP) {
            ResourceMap::const_iterator it2 = resources.find(it->first);
            if (it2 == resources.end() ||
                it->second.amount() > it2->second.amount())
            {
                if (filterLable) {
                    *filterLable = it->first;
                }
                return false;
            }
        }
    }
    return true;
}

void ResourceHelper::classifyResources(
        const ResourceMap &asks,
        DiskResources &diskResources,
        std::map<std::string, ResourceMap> &devResources,
        ResourceMap &restResMap)
{
    for (auto it = asks.begin(); it != asks.end(); ++it) {
        if (isDiskResource(it->first)) {
            string diskName = "";
            proto::Resource diskResource = it->second;
            stripDiskName(diskResource, diskName);
            diskResources[diskName][diskResource.name()] = diskResource;
        } else if (RES_NAME_IP == it->first || isIpResource(it->first)) {
            devResources[RES_NAME_IP][it->first] = it->second;
        } else if (RES_NAME_GPU == it->first || isGpuResource(it->first)) {
            devResources[RES_NAME_GPU][it->first] = it->second;
        } else if (RES_NAME_FPGA == it->first || isFpgaResource(it->first)) {
            devResources[RES_NAME_FPGA][it->first] = it->second;
        } else if (RES_NAME_POV == it->first || isPovResource(it->first)) {
            devResources[RES_NAME_POV][it->first] = it->second;
        } else if (RES_NAME_NPU == it->first || isNpuResource(it->first)) {
            devResources[RES_NAME_NPU][it->first] = it->second;
        } else {
            restResMap[it->first] = it->second;
        }
    }
}

void ResourceHelper::filterOutDiskResource(
        const ResourceMap &resMap,
        DiskResources &diskResources)
{
    ResourceMap::const_iterator it = resMap.begin();
    for (; it != resMap.end(); ++it) {
        if (isDiskResource(it->first)) {
            string diskName = "";
            proto::Resource diskResource = it->second;
            bool convertResult = stripDiskName(diskResource, diskName);
            if (convertResult) {
                diskResources[diskName][diskResource.name()] = diskResource;
            }
        }
    }
}

void ResourceHelper::classifyResourcesByType(
        const ResourceMap &askResources,
        map<proto::Resource::Type, ResourceMap> &classifiedResources)
{
    for (auto &e : askResources) {
        const proto::Resource &resource = e.second;
        proto::Resource::Type type = resource.type();
        if (type == proto::Resource::EXCLUDE_TEXT) {
            type = proto::Resource::TEXT;   // exclude_text also classify to text
        }
        classifiedResources[type][resource.name()] = resource;
    }
}

proto::SlotResource ResourceHelper::getTypeResources(
        const proto::SlotResource &requirement,
        const proto::Resource::Type &type)
{
    proto::SlotResource slotRes;
    for (int32_t idx = 0; idx < requirement.resources_size(); ++idx) {
        const proto::Resource &resource = requirement.resources(idx);
        if(resource.type() == type) {
            proto::Resource * res = slotRes.add_resources();
            *res = resource;
        }
    }
    return slotRes;
}

proto::SlotResource ResourceHelper::getScoreResources(
        const proto::SlotResource &requirement)
{
    proto::SlotResource slotRes;
    for (int32_t idx = 0; idx < requirement.resources_size(); ++idx) {
        const proto::Resource &resource = requirement.resources(idx);
        if(resource.type() == proto::Resource::SCALAR ||
           resource.type() == proto::Resource::EXCLUSIVE) {
            *(slotRes.add_resources()) = resource;
        }
    }
    return slotRes;
}

void ResourceHelper::filterOutSpecialResources(
        const proto::SlotResource &requirement,
        ResourceMap &requiredDiskResMap,
        ResourceMap &requiredIpResMap,
        ResourceMap &requiredRestResMap)
{
    for (int32_t idx = 0; idx < requirement.resources_size(); ++idx) {
        const proto::Resource &resource = requirement.resources(idx);
        if (isDiskResource(resource.name())) {
            requiredDiskResMap[resource.name()] = resource;
        } else if (RES_NAME_IP == resource.name() ||
                   ResourceHelper::isIpResource(resource.name()))
        {
            requiredIpResMap[resource.name()] = resource;
        } else {
            requiredRestResMap[resource.name()] = resource;
        }
    }
}

void ResourceHelper::filterOutSpecialResources(
        const ResourceMap &requirement,
        ResourceMap &requiredDiskResMap,
        ResourceMap &requiredIpResMap,
        ResourceMap &requiredRestResMap)
{
    ResourceMap::const_iterator it = requirement.begin();
    for (; it != requirement.end(); ++it) {
        const proto::Resource &resource = it->second;
        if (isDiskResource(it->first)) {
            requiredDiskResMap[resource.name()] = resource;
        } else if (RES_NAME_IP == resource.name() ||
                   ResourceHelper::isIpResource(resource.name()))
        {
            requiredIpResMap[resource.name()] = resource;
        } else {
            requiredRestResMap[resource.name()] = resource;
        }
    }
}

void ResourceHelper::filterOutSpecialResources(
        const ResourceMap &requirement,
        ResourceMap &requiredDiskResMap,
        ResourceMap &requiredIpResMap,
        ResourceMap &requiredGpuResMap,
        ResourceMap &requiredFpgaResMap,
        ResourceMap &requiredRestResMap)
{
    ResourceMap::const_iterator it = requirement.begin();
    for (; it != requirement.end(); ++it) {
        const proto::Resource &resource = it->second;
        if (isDiskResource(it->first)) {
            requiredDiskResMap[resource.name()] = resource;
        } else if (RES_NAME_IP == resource.name() ||
                   isIpResource(resource.name()))
        {
            requiredIpResMap[resource.name()] = resource;
        } else if (RES_NAME_GPU == resource.name() ||
                   isGpuResource(resource.name()))
        {
            requiredGpuResMap[resource.name()] = resource;
        } else if (RES_NAME_FPGA == resource.name() ||
                   isFpgaResource(resource.name()))
        {
            requiredFpgaResMap[resource.name()] = resource;
        } else {
            requiredRestResMap[resource.name()] = resource;
        }
    }
}

void ResourceHelper::getDiskResources(
        const ResourceMap &requirement,
        ResourceMap &requiredDiskResMap)
{
    ResourceMap::const_iterator it = requirement.begin();
    for (; it != requirement.end(); ++it) {
        const proto::Resource &resource = it->second;
        if (isDiskResource(it->first)) {
            requiredDiskResMap[resource.name()] = resource;
        }
    }
}

string ResourceHelper::getDiskResourceType(const proto::Resource &resource)
{
    if (!isDiskResource(resource.name())) {
        return "";
    }
    return stripDiskName(resource.name());
}

void ResourceHelper::addSlotResource(
        const proto::SlotResource &slotResource,
        map<string, proto::Resource> *usedGrantedResource)

{
    for (int i = 0; i < slotResource.resources_size(); ++i) {
        const proto::Resource &resource = slotResource.resources(i);
        const string &name = resource.name();
        map<string, proto::Resource>::iterator it = usedGrantedResource->find(name);
        if (it != usedGrantedResource->end()) {
            proto::Resource::Type usedResourceType = it->second.type();
            proto::Resource::Type resourceType = resource.type();
            if (usedResourceType != resourceType) {
                HIPPO_LOG(DEBUG, "name:[%s], used resource type[%d] is "
                        "not equal the resource type[%d]. please check",
                        name.c_str(), usedResourceType, resourceType);
            }
            if (usedResourceType == proto::Resource::SCALAR) {
                it->second.set_amount(it->second.amount() + resource.amount());
            }
        } else {
            proto::Resource newUsedResource;
            newUsedResource.set_type(resource.type());
            newUsedResource.set_name(resource.name());
            newUsedResource.set_amount(resource.amount());
            (*usedGrantedResource)[resource.name()] = newUsedResource;
        }
    }
}

// just check cpu, predict_cpu and mem,  disk reserved for consideration
bool ResourceHelper::hasInternalResource(const ResourceMap &resources) {
    ResourceMap::const_iterator it = resources.begin();
    for (; it != resources.end(); ++it) {
        if (it->first == RES_NAME_CPU ||
            it->first == RES_NAME_PREDICT_CPU ||
            it->first == RES_NAME_MEM)
        {
            if (it->second.amount() != 0) {
                return true;
            }
        }
    }
    return false;
}

void ResourceHelper::adjustInternalScalarResource(proto::ResourceRequest *request)
{
    for (int idx = 0; idx < request->options_size(); ++idx) {
        proto::SlotResource *oldSlotResource = request->mutable_options(idx);
        for (int subIdx = 0; subIdx < oldSlotResource->resources_size(); ++subIdx) {
            proto::Resource *oldResource = oldSlotResource->mutable_resources(subIdx);
            if (proto::ResourceHelper::isInternalScalarResource(oldResource->name())
                && oldResource->type() != proto::Resource::SCALAR)
            {
                oldResource->set_type(proto::Resource::SCALAR);
            }
        }
    }
}

int32_t ResourceHelper::getResourceCountOfType(
        const ResourceMap &resources,
        const Resource::Type &type)
{
    int32_t count = 0;
    ResourceMap::const_iterator it = resources.begin();
    for (; it != resources.end(); ++it) {
        if (it->second.type() == type) {
            count++;
        }
    }
    return count;
}

ResourceMap ResourceHelper::getResourcesOfType(
        const ResourceMap &resources,
        const proto::Resource::Type &type)
{
    ResourceMap out;
    for (ResourceMap::const_iterator it = resources.begin();
         it != resources.end(); ++it)
    {
        if (it->second.type() == type) {
            out[it->first] = it->second;
        }
    }
    return out;
}

void ResourceHelper::cutInternalResources(const ResourceMap &cut,
                                        ResourceMap &resources)
{
    ResourceMap::const_iterator it = cut.begin();
    for (; it != cut.end(); ++it) {
        if (it->first == RES_NAME_PREDICT_CPU ||
            it->first == RES_NAME_MEM)
        {
            ResourceMap::iterator sIt = resources.find(it->first);
            if (sIt != resources.end()) {
                int32_t newAmount = sIt->second.amount() - it->second.amount();
                newAmount = newAmount > 0 ? newAmount : 0;
                sIt->second.set_amount(newAmount);
            } else {
                resources[it->first] = it->second;
            }
        }
    }
}

void ResourceHelper::addInternalResources(const ResourceMap &add,
                                        ResourceMap &resources)
{
    ResourceMap::const_iterator it = add.begin();
    for (; it != add.end(); ++it) {
        if (it->first == RES_NAME_PREDICT_CPU ||
            it->first == RES_NAME_MEM)
        {
            ResourceMap::iterator sIt = resources.find(it->first);
            if (sIt != resources.end()) {
                int32_t newAmount = sIt->second.amount() + it->second.amount();
                sIt->second.set_amount(newAmount);
            } else {
                resources[it->first] = it->second;
            }
        }
    }
}

void ResourceHelper::addResources(ResourceMap &resources,
                                  const ResourceMap &add)
{
    for (const auto &resIt : add) {
        auto findIt = resources.find(resIt.first);
        if (findIt == resources.end()) {
            resources[resIt.first] = resIt.second;
        } else if (resIt.second.type() == Resource::SCALAR) {
            int32_t newAmount = findIt->second.amount() + resIt.second.amount();
            findIt->second.set_amount(newAmount);
        }
    }
}

void ResourceHelper::cutResources(ResourceMap &resources,
                                  const ResourceMap &cut,
                                  const bool removeItem)
{
    for (const auto &resIt : cut) {
        auto findIt = resources.find(resIt.first);
        if (findIt == resources.end()) {
            continue;
        }
        if (findIt->second.type() == Resource::SCALAR) {
            int32_t newAmount = findIt->second.amount() - resIt.second.amount();
            if (newAmount < 0) {
                newAmount = 0;
            }
            findIt->second.set_amount(newAmount);
            if (removeItem && newAmount == 0) {
                resources.erase(findIt);
            }
        } else if (removeItem) {
            resources.erase(findIt);
        }
    }
}

bool ResourceHelper::isResourceMatch(const ResourceMap &resources,
                                     const ResourceMap &require)
{
    for (ResourceMap::const_iterator it = require.begin();
         it != require.end(); ++it)
    {
        if (it->second.type() == Resource::PREFER_TEXT) {
            continue;
        }
        if (it->second.type() == Resource::PROHIBIT_TEXT ||
            it->second.type() == Resource::EXCLUSIVE)
        {
            if (resources.find(it->first) != resources.end()) {
                HIPPO_LOG(INFO, "require resource [%s] conflict with"
                       " available resource", it->first.c_str());
                return false;
            }
            continue;
        }
        // text,exclude_text,scalar must found
        ResourceMap::const_iterator rIt = resources.find(it->first);
        if (rIt == resources.end()) {
            HIPPO_LOG(INFO, "require resource [%s] not found in"
                       " available resource", it->first.c_str());
            return false;
        }
        if (rIt->second.type() == Resource::SCALAR) {
            if (rIt->second.amount() < it->second.amount()) {
                HIPPO_LOG(INFO, "require resource [%s:%d] larger than"
                       " available resource [%d]", it->first.c_str(),
                       it->second.amount(), rIt->second.amount());
                return false;
            }
        }
    }
    return true;
}

void ResourceHelper::cutRequirement(ResourceMap &require,
                                    const ResourceMap &cut)
{
    ResourceMap::const_iterator it = cut.begin();
    for (; it != cut.end(); ++it) {
        ResourceMap::iterator it2 = require.find(it->first);
        if (it2 == require.end()) {
            if (isIpResource(it->first)) {
                it2 = require.find(RES_NAME_IP);
            } else if (isDiskResource(it->first)) {
                string resourceName = stripDiskName(it->first);
                it2 = require.find(resourceName);
            } else if (isGpuResource(it->first)) {
                it2 = require.find(RES_NAME_GPU);
            } else if (isFpgaResource(it->first)) {
                it2 = require.find(RES_NAME_FPGA);
            } else if (isPovResource(it->first)) {
                it2 = require.find(RES_NAME_POV);
            } else if (isNpuResource(it->first)) {
                it2 = require.find(RES_NAME_NPU);
            }
        }
        if (it2 != require.end()) {
            if (it->second.type() == Resource::SCALAR) {
                int32_t newAmount = it2->second.amount() - it->second.amount();
                if (newAmount <= 0) {
                    require.erase(it2);
                } else {
                    it2->second.set_amount(newAmount);
                }
            } else {
                require.erase(it2);
            }
        }
    }
    for (it = require.begin(); it != require.end();) {
        if (it->second.type() == Resource::SCALAR && it->second.amount() <= 0) {
            require.erase(it++);
        } else {
            ++it;
        }
    }
}

/*
  schedule resource: like exclusive, prefer_text, prohibit_text, scalar_cmp,
                     and sn resources, which not work as actually resources
 */
void ResourceHelper::extractScheduleResources(
        ResourceMap &needReclaimedResources,
        ResourceMap &exclusiveResources,
        ResourceMap &preferenceResources)
{
    ResourceMap::iterator it = needReclaimedResources.begin();
    while (it != needReclaimedResources.end()) {
        if (it->second.type() == Resource::EXCLUSIVE) {
            exclusiveResources[it->first] = it->second;
            needReclaimedResources.erase(it++);
        } else if (it->second.type() == Resource::PREFER_TEXT ||
                   it->second.type() == Resource::PROHIBIT_TEXT)
        {
            preferenceResources[it->first] = it->second;
            needReclaimedResources.erase(it++);
        } else {
            ++it;
        }
    }
}

ResourceMap ResourceHelper::filterOutSpecifyTypeResources(
        const ResourceMap &resources, const Resource::Type type)
{
    ResourceMap results;
    ResourceMap::const_iterator it = resources.begin();
    for (; it != resources.end(); ++it) {
        if (it->second.type() != type) {
            results[it->first] = it->second;
        }
    }
    return results;
}

void ResourceHelper::getSn(const string &snResourceName, string *value) {
    *value = snResourceName.substr(RES_NAME_SN_PREFIX.size());
}

bool ResourceHelper::isResourceExist(const proto::SlaveResource &slaveResource,
                                     const string &resourceName)
{
    for (int i = 0; i < slaveResource.resources_size(); i++) {
        if (slaveResource.resources(i).name() == resourceName) {
            return true;
        }
    }
    return false;
}

bool ResourceHelper::isReservedResourceExist(const proto::SlaveResource &slaveResource,
        const string &resourceName)
{
    for (int i = 0; i < slaveResource.reservedresources_size(); i++) {
        if (slaveResource.reservedresources(i).name() == resourceName) {
            return true;
        }
    }
    return false;
}

bool ResourceHelper::hasExcludeResource(const proto::SlaveResource &slaveResource) {
    for (int i = 0; i < slaveResource.resources_size(); i++) {
        if (slaveResource.resources(i).type() == proto::Resource::EXCLUDE_TEXT) {
            return true;
        }
    }
    return false;
}

bool ResourceHelper::hasQueueNameResource(const proto::SlaveResource &slaveResource) {
    for (int i = 0; i < slaveResource.resources_size(); i++) {
        if (slaveResource.resources(i).type() == proto::Resource::QUEUE_NAME) {
            return true;
        }
    }
    return false;
}

int32_t ResourceHelper::roundPredictAmount(const int32_t require,
        const double predictLoad, const double roundPercent, const int32_t minAmount)
{
    int32_t step = abs(int32_t(require * roundPercent));
    if (step == 0) {
        step = 1;
    }
    int32_t resAmount = round(require * predictLoad / step) * step;
    if (resAmount > require) {
        resAmount = require;
    }
    if (resAmount < minAmount) {
        resAmount = minAmount;
    }
    return resAmount;
}

int32_t ResourceHelper::getResourceAmount(const ResourceMap & resource,
            const string & namePrefix) {
    string name;
    int32_t amount = 0 ;
    ResourceMap::const_iterator it = resource.begin();
    while (it != resource.end()) {
        name = it->second.name();
        if(StringUtil::startsWith(name, namePrefix)) {
            amount = amount + it->second.amount();
        }
        it++;
    }
    return amount;
}

int32_t ResourceHelper::getResourceAmount(const SlotResource & resource,
            const string & namePrefix){
    string name;
    int32_t amount = 0 ;
    for (int i = 0; i < resource.resources_size(); ++i) {
         name = resource.resources(i).name();
         if (StringUtil::startsWith(name, namePrefix)) {
             amount = amount + resource.resources(i).amount();
         }
    }
    return amount;
}

string ResourceHelper::getMaxDiskName(const ResourceMap &resources) {
    string res = "";
    int32_t maxAmount = -1;
    for (auto &e : resources) {
        if (isDiskSizeResource(e.second.name()) && e.second.amount() > maxAmount) {
            res = getDiskName(e.second.name());
            maxAmount = e.second.amount();
        }
    }
    return res;
}

bool ResourceHelper::sameSpecialResource(const string &require, const string &real) {
    if (require == RES_NAME_IP) {
        return isIpResource(real);
    }
    if (require == RES_NAME_GPU) {
        return isGpuResource(real);
    }
    if (require == RES_NAME_FPGA) {
        return isFpgaResource(real);
    }
    if (require == RES_NAME_POV) {
        return isPovResource(real);
    }
    if (require == RES_NAME_NPU) {
        return isNpuResource(real);
    }
    return require == real;
}

END_HIPPO_NAMESPACE(proto);
