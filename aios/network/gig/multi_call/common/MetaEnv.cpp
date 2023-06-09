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
#include "aios/network/gig/multi_call/common/MetaEnv.h"
#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include "aios/network/gig/multi_call/util/MetricUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, MetaEnv);

MetaEnv::MetaEnv() {}

MetaEnv::~MetaEnv() {}

bool MetaEnv::init() {
    _platform = getFromEnv(ENV_META_PLATFORM);
    _hippoCluster = getFromEnv(ENV_META_CLUSTER);
    _hippoApp = getFromEnv(ENV_META_APP);
    _c2role = getFromEnv(ENV_META_ROLE);
    _c2group = getFromEnv(ENV_META_GROUP);
    return valid();
}

bool MetaEnv::operator==(const MetaEnv &rhs) const {
    return _platform == rhs._platform && _hippoCluster == rhs._hippoCluster &&
           _hippoApp == rhs._hippoApp && _c2role == rhs._c2role &&
           _c2group == rhs._c2group;
}

std::map<std::string, std::string> MetaEnv::getEnvTags() const {
    std::map<std::string, std::string> tagEnv;
    tagEnv.emplace(GIG_TAG_PLATFORM, MetricUtil::normalizeEmpty(_platform));
    tagEnv.emplace(GIG_TAG_ROLE, MetricUtil::normalizeEmpty(_c2role));
    tagEnv.emplace(GIG_TAG_GROUP, MetricUtil::normalizeEmpty(_c2group));
    return tagEnv;
}

std::map<std::string, std::string> MetaEnv::getTargetTags() const {
    std::map<std::string, std::string> tagTarget;
    tagTarget.emplace(GIG_TAG_TARGET_PLATFORM,
                      MetricUtil::normalizeEmpty(_platform));
    tagTarget.emplace(GIG_TAG_TARGET_CLUSTER,
                      MetricUtil::normalizeEmpty(_hippoCluster));
    tagTarget.emplace(GIG_TAG_TARGET_APP,
                      MetricUtil::normalizeEmpty(_hippoApp));
    tagTarget.emplace(GIG_TAG_TARGET_ROLE, MetricUtil::normalizeEmpty(_c2role));
    tagTarget.emplace(GIG_TAG_TARGET_GROUP,
                      MetricUtil::normalizeEmpty(_c2group));
    return tagTarget;
}

bool MetaEnv::isUnknown(const std::string &env) {
    return (env.empty() || 0 == env.compare(GIG_UNKNOWN));
}

string MetaEnv::getFromEnv(const std::string &key) {
    const char *value = getenv(key.c_str());
    if (value) {
        return string(value);
    } else {
        return EMPTY_STRING;
    }
}

bool MetaEnv::valid() const {
    return !(MetaEnv::isUnknown(_platform) &&
             MetaEnv::isUnknown(_hippoCluster) &&
             MetaEnv::isUnknown(_hippoApp) && MetaEnv::isUnknown(_c2role) &&
             MetaEnv::isUnknown(_c2group));
}

void MetaEnv::setFromProto(const GigMetaEnv &gigMetaEnv) {
    _platform = gigMetaEnv.platform();
    _hippoCluster = gigMetaEnv.hippo_cluster();
    _hippoApp = gigMetaEnv.hippo_app();
    _c2role = gigMetaEnv.c2_role();
    _c2group = gigMetaEnv.c2_group();
}

void MetaEnv::fillProto(GigMetaEnv &gigMetaEnv) const {
    gigMetaEnv.set_platform(getPlatform());
    gigMetaEnv.set_hippo_cluster(getHippoCluster());
    gigMetaEnv.set_hippo_app(getHippoApp());
    gigMetaEnv.set_c2_role(getC2Role());
    gigMetaEnv.set_c2_group(getC2Group());
}

void MetaEnv::toString(std::string &ret) const {
    ret += "platform: " + _platform + "\n";
    ret += "hippo_cluster: " + _hippoCluster + "\n";
    ret += "hippo_app: " + _hippoApp + "\n";
    ret += "c2_role: " + _c2role + "\n";
    ret += "c2_group: " + _c2group + "\n";
}

} // namespace multi_call
