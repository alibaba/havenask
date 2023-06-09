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
#include "ha3/summary/SummaryProfileManagerCreator.h"

#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/plugin/PlugInManager.h"
#include "ha3/config/SummaryProfileConfig.h"
#include "ha3/config/SummaryProfileInfo.h"
#include "ha3/summary/SummaryProfile.h"
#include "ha3/summary/SummaryProfileManager.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace build_service::plugin;
using namespace suez::turing;
using namespace isearch::config;

namespace isearch {
namespace summary {
AUTIL_LOG_SETUP(ha3, SummaryProfileManagerCreator);

SummaryProfileManagerPtr SummaryProfileManagerCreator::createFromString(
        const std::string &configStr)
{
    SummaryProfileConfig summaryProfileConfig;
    if (!configStr.empty()) {
        try {
            FromJsonString(summaryProfileConfig, configStr);
        } catch (const ExceptionBase &e) {
            AUTIL_LOG(ERROR, "Create SummaryProfileManager Failed. ConfigString[%s], Exception[%s]",
                    configStr.c_str(), e.what());
            return SummaryProfileManagerPtr();
        }
    }
    return create(summaryProfileConfig);
}


SummaryProfileManagerPtr SummaryProfileManagerCreator::create(
        const SummaryProfileConfig &summaryProfileConfig)
{
    SummaryProfileInfos summaryProfileInfos =
        summaryProfileConfig.getSummaryProfileInfos();
    SummaryProfileManagerPtr summaryProfileManagerPtr(
            new SummaryProfileManager(_resourceReaderPtr, _hitSummarySchema.get()));
    if (!summaryProfileManagerPtr->addModules(summaryProfileConfig.getModuleInfos())) {
        AUTIL_LOG(ERROR, "load to create summary modules : %s",
                ToJsonString(summaryProfileConfig.getModuleInfos()).c_str());
        return SummaryProfileManagerPtr();
    }

    for (size_t i = 0; i < summaryProfileInfos.size(); i++) {
        summaryProfileManagerPtr->addSummaryProfile(
                new SummaryProfile(summaryProfileInfos[i], _hitSummarySchema.get()));
    }
    if (!summaryProfileManagerPtr->init(_cavaPluginManager, _metricsReporter)) {
        AUTIL_LOG(ERROR, "Init summaryProfileManager fail!");
        return SummaryProfileManagerPtr();
    }
    summaryProfileManagerPtr->setRequiredAttributeFields(
            summaryProfileConfig.getRequiredAttributeFields());
    return summaryProfileManagerPtr;
}

} // namespace summary
} // namespace isearch
