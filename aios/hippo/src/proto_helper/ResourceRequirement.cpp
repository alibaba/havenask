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
#include "proto_helper/ResourceRequirement.h"
#include "proto_helper/ResourceHelper.h"

using namespace std;
BEGIN_HIPPO_NAMESPACE(proto);

HIPPO_LOG_SETUP(proto_helper, ResourceRequirement);

ResourceRequirement::ResourceRequirement(const ResourceMap &target,
        const bool systemApp)
{
    _isExactlyMatch = false;
    _askForChangeMajorDisk = false;
    _requiredResources = target;
    _systemApp = systemApp;
    ResourceHelper::filterOutSpecialResources(target,
            _targetDiskResources, _targetIpResources,
            _targetGpuResources, _targetFpgaResources,
            _targetOtherResources);
    adjustDiskRequirement();
    _targetDiskLocation = ResourceHelper::getDiskLocation(_targetDiskResources);
}

void ResourceRequirement::adjustDiskRequirement() {
    if (_targetDiskResources.size() == (size_t)0) {
        // add default disk requirement
        _targetDiskResources[RES_NAME_DISK_RATIO].set_name(
                RES_NAME_DISK_RATIO);
        _targetDiskResources[RES_NAME_DISK_RATIO].set_type(
                Resource::SCALAR);
        if (!_systemApp) {
            _targetDiskResources[RES_NAME_DISK_RATIO].set_amount(
                    DEFAULT_DISK_RATIO_AMOUNT);
        } else {
            _targetDiskResources[RES_NAME_DISK_RATIO].set_amount(
                    SYSTEM_APP_DEFAULT_DISK_RATIO_AMOUNT);
        }
    }
}

void ResourceRequirement::resetRequirement(const ResourceMap &target)
{
    _isExactlyMatch = false;
    _askForChangeMajorDisk = false;
    _requiredResources = target;
    _targetDiskResources.clear();
    _targetIpResources.clear();
    _targetGpuResources.clear();
    _targetFpgaResources.clear();
    _targetOtherResources.clear();
    ResourceHelper::filterOutSpecialResources(target,
            _targetDiskResources, _targetIpResources,
            _targetGpuResources, _targetFpgaResources,
            _targetOtherResources);
    adjustDiskRequirement();
    _targetDiskLocation = ResourceHelper::getDiskLocation(_targetDiskResources);
}

ResourceRequirement::ResourceRequirement(const ResourceRequirement &rh) {
    *this = rh;
}

ResourceRequirement& ResourceRequirement::operator=(const ResourceRequirement &rh) {
    _targetDiskResources = rh._targetDiskResources;
    _targetDiskLocation = rh._targetDiskLocation;
    _targetIpResources = rh._targetIpResources;
    _targetGpuResources = rh._targetGpuResources;
    _targetFpgaResources = rh._targetFpgaResources;
    _targetOtherResources = rh._targetOtherResources;
    _requiredResources = rh._requiredResources;
    _isExactlyMatch = rh._isExactlyMatch;
    _askForChangeMajorDisk = rh._askForChangeMajorDisk;
    _needExtendResources = rh._needExtendResources;
    _needShrinkResources = rh._needShrinkResources;
    _systemApp = rh._systemApp;
    return *this;
}

ResourceRequirement::~ResourceRequirement() {
}

bool ResourceRequirement::askForChangeRuntime() const {
    return _needExtendResources.find(RES_NAME_T4) != _needExtendResources.end() ||
        _needExtendResources.find(RES_NAME_DOCKER) != _needExtendResources.end() ||
        _needShrinkResources.find(RES_NAME_T4) != _needShrinkResources.end() ||
        _needShrinkResources.find(RES_NAME_DOCKER) != _needShrinkResources.end();
}

bool ResourceRequirement::askForChangeNetwork() const {
    for (const auto &e : _needExtendResources) {
        if ((ResourceHelper::isIpResource(e.first) || e.first == RES_NAME_IP) &&
            e.second.amount() > 0)
        {
            return true;
        }
    }
    for (const auto &e : _needShrinkResources) {
        if ((ResourceHelper::isIpResource(e.first) || e.first == RES_NAME_IP) &&
            e.second.amount() > 0)
        {
            return true;
        }
    }
    return false;
}

bool ResourceRequirement::resourceCanUpdate() const {
    if (askForChangeMajorDisk()) {
        return false;
    }
    if (askForChangeRuntime()) {
        return false;
    }
    if (askForChangeNetwork()) {
        return false;
    }
    return true;
}

void ResourceRequirement::generateDiff(const ResourceMap &target,
                                       const ResourceMap &original)
{
    auto targetIt = target.begin();
    for (; targetIt != target.end(); ++targetIt) {
        const proto::Resource &targetRes = targetIt->second;
        if (ResourceHelper::isTmpScheduleResource(targetRes)) {
            continue;
        }
        const string &name = targetRes.name();
        auto originIt = original.find(name);
        if (originIt == original.end()) {
            _isExactlyMatch = false;
            _needExtendResources[name] = targetRes;
        } else if (originIt->second.type() == proto::Resource::SCALAR_CMP) {
            if (originIt->second.amount() == targetRes.amount()) {
                continue;
            } else {
                _isExactlyMatch = false;
                _needShrinkResources[name] = originIt->second;
                _needExtendResources[name] = targetRes;
            }
        } else if (originIt->second.type() == proto::Resource::SCALAR) {
            int amountDiff = originIt->second.amount() - targetRes.amount();
            if (amountDiff > 0) {
                _isExactlyMatch = false;
                _needShrinkResources[name] = originIt->second;
                _needShrinkResources[name].set_amount(amountDiff);
            } else if (amountDiff < 0) {
                _isExactlyMatch = false;
                _needExtendResources[name] = originIt->second;
                _needExtendResources[name].set_amount(-amountDiff);
            }
        } else if (originIt->second.type() == proto::Resource::EXCLUDE_TEXT) {
            // keep slot's EXCLUDE_TEXT resource for hasEnoughResource,
            // but not set _isExactlyMatch = false
            _needExtendResources[name] = originIt->second;
        }
    }
    ResourceMap::const_iterator it = original.begin();
    for (; it != original.end(); ++it) {
        if (ResourceHelper::isTmpScheduleResource(it->second)) {
            continue;
        }
        if (target.find(it->first) == target.end()) {
            _isExactlyMatch = false;
            _needShrinkResources[it->first] = it->second;
        }
    }
    HIPPO_LOG(DEBUG, "compare[%s] vs [%s]: needExtend[%s], needShrink[%s]",
              ResourceHelper::toString(target).c_str(),
              ResourceHelper::toString(original).c_str(),
              ResourceHelper::toString(_needExtendResources).c_str(),
              ResourceHelper::toString(_needShrinkResources).c_str());
}

void ResourceRequirement::dealWithDiskResource(const ResourceMap &oldDiskResources) {
    string oldDiskLocation = ResourceHelper::getDiskLocation(oldDiskResources);
    if (oldDiskLocation == "") {
        HIPPO_LOG(DEBUG, "old slot has no disk location found.");
        return;
    }
    ResourceMap convertedTargetDiskResources = _targetDiskResources;
    if (!_targetDiskLocation.empty() && _targetDiskLocation != oldDiskLocation) {
        // do nothing, forbid changing disk location(major disk)
        _askForChangeMajorDisk = true;
        _isExactlyMatch = false;
        return;
    }
    // use the allocated disk location as target
    ResourceHelper::joinDiskName(&convertedTargetDiskResources, oldDiskLocation);
    generateDiff(convertedTargetDiskResources, oldDiskResources);
}

void ResourceRequirement::dealWithIpResource(ResourceMap &oldIpResources) {
    auto it = oldIpResources.find(RES_NAME_IP);
    if (it != oldIpResources.end()) {
        _needShrinkResources[RES_NAME_IP] = it->second;
        oldIpResources.erase(it);
    }
    if (_targetIpResources.size() == (size_t)0 ||
        _targetIpResources.find(RES_NAME_IP) == _targetIpResources.end())
    {
        // not need ip anymore or specified ips
        generateDiff(_targetIpResources, oldIpResources);
    } else {
        // ip:2 (specify ip count)
        assert(_targetIpResources.size() == (size_t)1);
        int32_t targetIpAmount = _targetIpResources[RES_NAME_IP].amount();
        if (targetIpAmount > 0 && targetIpAmount > (int32_t)oldIpResources.size()) {
            // ask more ip
            int32_t amount = targetIpAmount - oldIpResources.size();
            _needExtendResources[RES_NAME_IP].set_name(RES_NAME_IP);
            _needExtendResources[RES_NAME_IP].set_type(Resource::SCALAR);
            _needExtendResources[RES_NAME_IP].set_amount(amount);
            _isExactlyMatch = false;
        } else if (targetIpAmount < (int32_t)oldIpResources.size()) {
            // should release some ip
            ResourceMap::const_iterator it = oldIpResources.begin();
            while (targetIpAmount-- > 0) {
                it++;
            }
            while (it != oldIpResources.end()) {
                // release ip
                _needShrinkResources[it->first] = it->second;
                it++;
            }
            _isExactlyMatch = false;
        } // else: targetIpAmount == oldIpResources.size()
    }
}

void ResourceRequirement::dealWithGpuResource(ResourceMap &oldGpuResources) {
    auto it = oldGpuResources.find(RES_NAME_GPU);
    if (it != oldGpuResources.end()) {
        _needShrinkResources[RES_NAME_GPU] = it->second;
        oldGpuResources.erase(it);
    }
    if (_targetGpuResources.size() == (size_t)0 ||
        _targetGpuResources.find(RES_NAME_GPU) == _targetGpuResources.end())
    {
        // not need gpu anymore or specified gpus
        generateDiff(_targetGpuResources, oldGpuResources);
    } else {
        // gpu:2 (specify gpu count)
        assert(_targetGpuResources.size() == (size_t)1);
        int32_t targetGpuAmount = _targetGpuResources[RES_NAME_GPU].amount();
        int32_t oldGpuAmount = 0;
        ResourceMap::const_iterator myIt = oldGpuResources.begin();
        for (; myIt != oldGpuResources.end(); ++myIt) {
            oldGpuAmount += myIt->second.amount();
        }
//        HIPPO_LOG(WARN, "targetGpuAmount %d, oldGpuAmount %d", targetGpuAmount, oldGpuAmount);
        if (targetGpuAmount > 0 && targetGpuAmount > oldGpuAmount) {
            // ask more gpu
            int32_t amount = targetGpuAmount - oldGpuAmount;
            _needExtendResources[RES_NAME_GPU].set_name(RES_NAME_GPU);
            _needExtendResources[RES_NAME_GPU].set_type(Resource::SCALAR);
            _needExtendResources[RES_NAME_GPU].set_amount(amount);
            _isExactlyMatch = false;
        } else if (targetGpuAmount < oldGpuAmount) {
            // should release some gpu
            ResourceMap::const_iterator it = oldGpuResources.begin();
            while (targetGpuAmount > 0) {
                targetGpuAmount -= it->second.amount();
                if (targetGpuAmount < 0 ) {
                    Resource res = it->second;
                    res.set_amount(-targetGpuAmount);
                    _needShrinkResources[it->first] = res;
                }
                ++it;
            }
            while (it != oldGpuResources.end()) {
                // release gpu
                _needShrinkResources[it->first] = it->second;
                it++;
            }
            _isExactlyMatch = false;
        } // else: targetGpuAmount == oldGpuAmount
    }
}

void ResourceRequirement::dealWithFpgaResource(ResourceMap &oldFpgaResources) {
    auto it = oldFpgaResources.find(RES_NAME_FPGA);
    if (it != oldFpgaResources.end()) {
        _needShrinkResources[RES_NAME_FPGA] = it->second;
        oldFpgaResources.erase(it);
    }
    if (_targetFpgaResources.size() == (size_t)0 ||
        _targetFpgaResources.find(RES_NAME_FPGA) == _targetFpgaResources.end())
    {
        // not need fpga anymore or specified fpgas
        generateDiff(_targetFpgaResources, oldFpgaResources);
    } else {
        // fpga:2 (specify fpga count)
        assert(_targetFpgaResources.size() == (size_t)1);
        int32_t targetFpgaAmount = _targetFpgaResources[RES_NAME_FPGA].amount();
        int32_t oldFpgaAmount = 0;
        ResourceMap::const_iterator myIt = oldFpgaResources.begin();
        for (; myIt != oldFpgaResources.end(); ++myIt) {
            oldFpgaAmount += myIt->second.amount();
        }
//        HIPPO_LOG(WARN, "targetFpgaAmount %d, oldFpgaAmount %d", targetFpgaAmount, oldFpgaAmount);
        if (targetFpgaAmount > 0 && targetFpgaAmount > oldFpgaAmount) {
            // ask more fpga
            int32_t amount = targetFpgaAmount - oldFpgaAmount;
            _needExtendResources[RES_NAME_FPGA].set_name(RES_NAME_FPGA);
            _needExtendResources[RES_NAME_FPGA].set_type(Resource::SCALAR);
            _needExtendResources[RES_NAME_FPGA].set_amount(amount);
            _isExactlyMatch = false;
        } else if (targetFpgaAmount < oldFpgaAmount) {
            // should release some fpga
            ResourceMap::const_iterator it = oldFpgaResources.begin();
            while (targetFpgaAmount > 0) {
                targetFpgaAmount -= it->second.amount();
                if (targetFpgaAmount < 0 ) {
                    Resource res = it->second;
                    res.set_amount(-targetFpgaAmount);
                    _needShrinkResources[it->first] = res;
                }
                ++it;
            }
            while (it != oldFpgaResources.end()) {
                // release fpga
                _needShrinkResources[it->first] = it->second;
                it++;
            }
            _isExactlyMatch = false;
        } // else: targetFpgaAmount == oldFpgaAmount
    }
}


void ResourceRequirement::compare(const ResourceMap &originalResources) {
    _isExactlyMatch = true;
    _askForChangeMajorDisk = false;
    _needExtendResources.clear();
    _needShrinkResources.clear();
    ResourceMap oldDiskResources, oldIpResources;
    ResourceMap oldGpuResources, oldFpgaResources, oldOtherResources;
    ResourceHelper::filterOutSpecialResources(originalResources,
            oldDiskResources, oldIpResources, oldGpuResources,
            oldFpgaResources, oldOtherResources);

    // 0. deal with disk resource
    dealWithDiskResource(oldDiskResources);

    // 1. deal with ip resource
    dealWithIpResource(oldIpResources);

    // 2. deal with gpu resource
    dealWithGpuResource(oldGpuResources);

    // 3. deal with fpga resource
    dealWithFpgaResource(oldFpgaResources);

    // 4. deal with other resource
    generateDiff(_targetOtherResources, oldOtherResources);
}

string ResourceRequirement::toString() const {
    return ResourceHelper::toString(_requiredResources);
}

END_HIPPO_NAMESPACE(proto);
