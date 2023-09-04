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
#include "hippo/HippoUtil.h"

#include "hippo/ProtoWrapper.h"
#include "proto_helper/ResourceHelper.h"
#include "util/PackageUtil.h"

using namespace std;
USE_HIPPO_NAMESPACE(util);

namespace hippo {

string HippoUtil::genPackageChecksum(const vector<PackageInfo> &packageInfos) {
    vector<proto::PackageInfo> protoPackages;
    ProtoWrapper::convert(packageInfos, &protoPackages);
    return PackageUtil::getPackageChecksum(protoPackages);
}

string HippoUtil::getIp(const SlotInfo &slotInfo) {
    // on c2
    if (slotInfo.slotId.k8sPodName != "") {
        if (slotInfo.ip != "") {
            return slotInfo.ip;
        }
    }
    const vector<Resource> &slotResources = slotInfo.slotResource.resources;
    for (size_t i = 0; i < slotResources.size(); ++i) {
        const Resource &res = slotResources[i];
        if (!hippo::proto::ResourceHelper::isIpResource(res.name)) {
            continue;
        }
        string realIp = "";
        bool ret = hippo::proto::ResourceHelper::getIp(res.name, &realIp);
        if (!ret) {
            continue;
        }
        return realIp;
    }

    const string &address = slotInfo.slotId.slaveAddress;
    size_t idx = address.find(":");
    if (idx == string::npos) {
        return address;
    }
    return address.substr(0, idx);
}

string HippoUtil::getIp(const proto::AssignedSlot &slotInfo) {
    const auto &slotResources = slotInfo.slotresource().resources();
    for (size_t i = 0; i < slotResources.size(); ++i) {
        const auto &res = slotResources[i];
        if (!hippo::proto::ResourceHelper::isIpResource(res.name())) {
            continue;
        }
        string realIp = "";
        bool ret = hippo::proto::ResourceHelper::getIp(res.name(), &realIp);
        if (!ret) {
            continue;
        }
        return realIp;
    }

    const string &address = slotInfo.id().slaveaddress();
    size_t idx = address.find(":");
    if (idx == string::npos) {
        return address;
    }
    return address.substr(0, idx);
}

} // namespace hippo
