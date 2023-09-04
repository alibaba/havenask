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
#ifndef HIPPO_RESOURCEREQUIREMENT_H
#define HIPPO_RESOURCEREQUIREMENT_H

#include "util/Log.h"
#include "common/common.h"
#include "hippo/proto/Common.pb.h"

BEGIN_HIPPO_NAMESPACE(proto);

typedef std::map<std::string, proto::Resource> ResourceMap;
typedef std::map<std::string, ResourceMap> DiskResources;
typedef std::map<std::string, proto::Resource> IpResources;
typedef ::google::protobuf::RepeatedPtrField<proto::SlotResource> ResourceOptions;

class ResourceRequirement
{
public:
    ResourceRequirement(const ResourceMap &target, const bool systemApp = false);
    ~ResourceRequirement();
public:
    ResourceRequirement(const ResourceRequirement &);
    ResourceRequirement& operator=(const ResourceRequirement &);
    void resetRequirement(const ResourceMap &target);
public:
    void compare(const ResourceMap &resources);
    bool isExactlyMatch() const {
        return _isExactlyMatch;
    }
    const ResourceMap& needExtendResources() const {
        return _needExtendResources;
    }
    const ResourceMap& needShrinkResources() const {
        return _needShrinkResources;
    }
    const ResourceMap& requiredResources() const {
        return _requiredResources;
    }

    bool resourceCanUpdate() const;
    
    // majoy disk(first disk) change is forbidden
    bool askForChangeMajorDisk() const {
        return _askForChangeMajorDisk;
    }
    bool askForChangeRuntime() const;
    
    bool askForChangeNetwork() const;

    std::string toString() const;

    bool isSystem() const {
        return _systemApp;
    }
private:
    void generateDiff(const ResourceMap &target, const ResourceMap &original);
    void dealWithDiskResource(const ResourceMap &oldDiskResources);
    void dealWithIpResource(ResourceMap &oldIpResources);
    void dealWithGpuResource(ResourceMap &oldGpuResources);
    void dealWithFpgaResource(ResourceMap &oldFpgaResources);
    void adjustDiskRequirement();

private:
    ResourceMap _requiredResources;
    ResourceMap _targetDiskResources;
    std::string _targetDiskLocation;
    ResourceMap _targetIpResources;
    ResourceMap _targetGpuResources;
    ResourceMap _targetFpgaResources;
    ResourceMap _targetOtherResources;
    bool _isExactlyMatch;
    bool _askForChangeMajorDisk;
    ResourceMap _needExtendResources;
    ResourceMap _needShrinkResources;
    bool _systemApp;
private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(ResourceRequirement);

END_HIPPO_NAMESPACE(proto);

#endif //HIPPO_RESOURCEREQUIREMENT_H
