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
#include "master/SignatureGenerator.h"
#include "carbon/Log.h"
#include "autil/StringUtil.h"
#include "autil/HashAlgorithm.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, SignatureGenerator);

SignatureGenerator::SignatureGenerator() {
}

SignatureGenerator::~SignatureGenerator() {
}

roleguid_t SignatureGenerator::genRoleGUID(const groupid_t &groupId, const roleid_t roleId) {
    return groupId + "." + roleId;
}

tag_t SignatureGenerator::genRoleResourceTag(const roleid_t &roleId,
                                             const ResourcePlan &resourcePlan)
{
    /* assume all resource in ResourcePlan can be adjusted in place,
     * so we just use roleid as resourcetag, if some resource must use a new tag,
     * add it back to resource tag generate logic.
     */
    return roleId;
}

string SignatureGenerator::genHealthCheckerId(const roleid_t &roleId,
        const HealthCheckerConfig &config)
{
    return (roleId + "." + config.name + "." +
            SignatureGenerator::hashString(ToJsonString(config, true)));
}

string SignatureGenerator::genServiceAdapterId(const ServiceConfig &config) {
    VersionedServiceConfig verConfig(config);
    return config.name + "." + SignatureGenerator::hashString(
            ToJsonString(verConfig, true));
}

string SignatureGenerator::hashString(const string &str) {
    uint64_t v = HashAlgorithm::hashString64(str.c_str());
    char hexBuf[32];
    StringUtil::uint64ToHexStr(v, hexBuf, 32);
    return string(hexBuf);
}

int64_t SignatureGenerator::genProcInstanceId(
        const ProcessInfo &processInfo,
        const string &suffix)
{
    string str = ToJsonString(processInfo, true) + suffix;
    return HashAlgorithm::hashString64(str.c_str());
}

int64_t SignatureGenerator::genPackageCheckSum(
        const vector<PackageInfo> &packageInfos)
{
    string str = ToJsonString(packageInfos, true);
    return HashAlgorithm::hashString64(str.c_str());
}

END_CARBON_NAMESPACE(master);

