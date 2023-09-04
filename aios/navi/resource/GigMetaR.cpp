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
#include "navi/resource/GigMetaR.h"

namespace navi {

const std::string GigMetaR::RESOURCE_ID = "navi.buildin.gig_meta.r";

GigMetaR::GigMetaR() {
}

GigMetaR::GigMetaR(const std::vector<multi_call::BizMetaInfo> &bizMetaInfos)
    : _bizMetaInfos(bizMetaInfos)
{
}

GigMetaR::~GigMetaR() {
}

void GigMetaR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_SNAPSHOT);
}

bool GigMetaR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode GigMetaR::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

bool GigMetaR::isSame(const GigMetaRPtr &otherPtr) const {
    if (!otherPtr) {
        return false;
    }
    const auto &other = *otherPtr;
    if (_bizMetaInfos.size() != other._bizMetaInfos.size()) {
        return false;
    }
    for (size_t bizIndex = 0; bizIndex < _bizMetaInfos.size(); bizIndex++) {
        const auto &left = _bizMetaInfos[bizIndex];
        const auto &right = other._bizMetaInfos[bizIndex];
        if (left.bizName != right.bizName) {
            return false;
        }
        if (left.versions.size() != right.versions.size()) {
            return false;
        }
        if (!(left.versions == right.versions)) {
            return false;
        }
    }
    return true;
}

void GigMetaR::log() const {
    for (const auto &info : _bizMetaInfos) {
        for (const auto &pair : info.versions) {
            const auto versionInfo = pair.second;
            NAVI_KERNEL_LOG(INFO,
                            "biz: %s, version: %ld, partCount: %lu, healthCount: %lu",
                            info.bizName.c_str(),
                            pair.first,
                            versionInfo.partCount,
                            versionInfo.healthCount);
        }
    }
}

REGISTER_RESOURCE(GigMetaR);

}

