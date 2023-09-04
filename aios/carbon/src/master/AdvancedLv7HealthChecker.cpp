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
#include "master/AdvancedLv7HealthChecker.h"
#include "master/WorkerNode.h"
#include "master/GlobalVariable.h"
#include "master/Util.h"
#include "carbon/Log.h"
#include "carbon/RolePlan.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, AdvancedLv7HealthChecker);

AdvancedLv7HealthChecker::AdvancedLv7HealthChecker(const string &id) :
    Lv7HealthChecker(id)
{
}

AdvancedLv7HealthChecker::~AdvancedLv7HealthChecker() {
}

void AdvancedLv7HealthChecker::fillNodeMetas(
        const WorkerNodePtr &workerNodePtr,
        KVMap *metas)
{
    VersionedPlan plan = workerNodePtr->getCurPlan();
    VersionedPlan finalPlan = workerNodePtr->getFinalPlan();
    const string &processVersion = workerNodePtr->getProcessVersion();
    KVMap serviceMetas = workerNodePtr->getServiceInfoMetas();
    const string &signature = plan.signature;
    const string &customInfo = plan.customInfo;

    CARBON_LOG(DEBUG, "fill node metas, node:[%s], "
               "signature:[%s], customInfo:[%s].",
               workerNodePtr->getId().c_str(), signature.c_str(),
               customInfo.c_str());

    metas->clear();
    (*metas)[HEALTHCHECK_PAYLOAD_SIGNATURE] = signature;
    (*metas)[HEALTHCHECK_PAYLOAD_CUSTOMINFO] = customInfo;
    (*metas)[HEALTHCHECK_PAYLOAD_GLOBAL_CUSTOMINFO] =
        autil::legacy::ToJsonString(finalPlan, true);
    (*metas)[HEALTHCHECK_PAYLOAD_IDENTIFIER] =
        GlobalVariable::genUniqIdentifier(workerNodePtr->getId());
    (*metas)[HEALTHCHECK_PAYLOAD_PROCESS_VERSION] = processVersion;
    (*metas)[HEALTHCHECK_PAYLOAD_SCHEDULERINFO] =
        autil::legacy::ToJsonString(serviceMetas, true);
    (*metas)[HEALTHCHECK_PAYLOAD_PRELOAD] = finalPlan.preload ? "true" : "false";

    CARBON_LOG(DEBUG, "health check node metas: [%s].",
               autil::legacy::ToJsonString(metas, true).c_str());
}

void AdvancedLv7HealthChecker::getQueryDatas(
        const vector<CheckNode> &checkNodeVect, vector<string> *datas)
{
    datas->resize(checkNodeVect.size());
    for (size_t i = 0; i < checkNodeVect.size(); i++) {
        string &data = (*datas)[i];
        try {
            data = autil::legacy::ToJsonString(checkNodeVect[i].metas, true);
        } catch (const autil::legacy::ExceptionBase &e) {
            CARBON_LOG(ERROR, "invalid health check metas. "
                       "node:[%s], address:[%s], error:%s.",
                       checkNodeVect[i].nodeId.c_str(),
                       checkNodeVect[i].slotInfo.slotId.slaveAddress.c_str(),
                       e.what());
        }
    }
}

bool AdvancedLv7HealthChecker::parseHealthMetas(
        const string &content, KVMap *metas)
{
    metas->clear();
    if (content == "") {
        return true;
    }
    try {
        autil::legacy::FromJsonString(*metas, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "parse health metas failed, content:[%s]. error:[%s]",
                   content.c_str(), e.what());
        return false;
    }

    return true;
}

WorkerType AdvancedLv7HealthChecker::checkWorkerReady(
        const CheckNode &checkNode, const KVMap &metas)
{
    CARBON_LOG(DEBUG, "target node metas:[%s], cur node metas:[%s].",
               autil::legacy::ToJsonString(checkNode.metas, true).c_str(),
               autil::legacy::ToJsonString(metas, true).c_str());
    string targetSignature = Util::getValue(checkNode.metas,
            HEALTHCHECK_PAYLOAD_SIGNATURE);
    string curSignature = Util::getValue(metas,
            HEALTHCHECK_PAYLOAD_SIGNATURE);

    CARBON_LOG(DEBUG, "targetSignature:[%s], curSignature:[%s].",
               targetSignature.c_str(), curSignature.c_str());
    return (targetSignature == curSignature) ? WT_READY : WT_NOT_READY;
}

END_CARBON_NAMESPACE(master);

