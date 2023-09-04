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
#ifndef CARBON_SIGNATUREGENERATOR_H
#define CARBON_SIGNATUREGENERATOR_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/RolePlan.h"
BEGIN_CARBON_NAMESPACE(master);

class SignatureGenerator
{
public:
    SignatureGenerator();
    ~SignatureGenerator();
private:
    SignatureGenerator(const SignatureGenerator &);
    SignatureGenerator& operator=(const SignatureGenerator &);
public:
    static roleguid_t genRoleGUID(const groupid_t &groupId, const roleid_t roldId);

    static tag_t genRoleResourceTag(const roleid_t &roleId,
                                    const ResourcePlan &resourcePlan);

    static std::string genHealthCheckerId(const roleid_t &roleId,
            const HealthCheckerConfig &config);

    static std::string genServiceAdapterId(const ServiceConfig &config);

    static std::string hashString(const std::string &str);

    static int64_t genProcInstanceId(
            const ProcessInfo &processInfo,
            const std::string &suffix = "");
    static int64_t genPackageCheckSum(const std::vector<PackageInfo> &packageInfos);
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(SignatureGenerator);

END_CARBON_NAMESPACE(master);

#endif //CARBON_SIGNATUREGENERATOR_H
