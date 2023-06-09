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
#include "ha3/summary/SummaryProfileManager.h"

#include <cstddef>
#include <utility>

#include "autil/Log.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/ModuleFactory.h"
#include "build_service/plugin/ModuleInfo.h"
#include "ha3/summary/SummaryProfile.h"
#include "ha3/summary/SummaryModuleFactory.h"
#include "ha3/summary/CavaSummaryExtractor.h"
#include "ha3/summary/DefaultSummaryExtractor.h"
#include "ha3/summary/SummaryExtractorChain.h"
#include "ha3/summary/SummaryModuleFactory.h"

#ifndef AIOS_OPEN_SOURCE
#include "ha3/summary/AdapterSummary.h"
#endif

#include "suez/turing/expression/cava/common/CavaConfig.h"


namespace suez {
namespace turing {
class CavaPluginManager;
}  // namespace turing
}  // namespace suez

namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

using namespace std;
using namespace build_service::plugin;
using namespace suez::turing;
using namespace isearch::config;

namespace isearch {
namespace summary {
AUTIL_LOG_SETUP(ha3, SummaryProfileManager);

SummaryProfileManager::SummaryProfileManager(
        config::ResourceReaderPtr &resourceReaderPtr,
        config::HitSummarySchema *hitSummarySchema)
    : PlugInManager(resourceReaderPtr, MODULE_FUNC_SUMMARY)
    , _hitSummarySchema(hitSummarySchema)
{
}

SummaryProfileManager::~SummaryProfileManager() {
    clear();
}

const SummaryProfile* SummaryProfileManager::
getSummaryProfile(const std::string &summaryProfileName) const
{
    SummaryProfiles::const_iterator it = _summaryProfiles.find(summaryProfileName);
    if (it == _summaryProfiles.end()) {
        return nullptr;
    } else {
        return it->second;
    }
}

void SummaryProfileManager::addSummaryProfile(SummaryProfile *summaryProfile) {
    string profileName = summaryProfile->getProfileName();
    SummaryProfiles::const_iterator it = _summaryProfiles.find(profileName);
    if (it != _summaryProfiles.end()) {
        delete it->second;
    }
    _summaryProfiles[profileName] = summaryProfile;
}

bool SummaryProfileManager::init(const CavaPluginManager *cavaPluginManager,
                                 kmonitor::MetricsReporter *metricsReporter) {
    for (SummaryProfiles::iterator it = _summaryProfiles.begin();
         it != _summaryProfiles.end(); ++it)
    {
        if (!initSummaryProfile(cavaPluginManager, metricsReporter, it->second)) {
            AUTIL_LOG(ERROR, "init summary profile[%s] failed",
                      it->second->getProfileName().c_str());
            return false;
        }
    }
    return true;
}

bool SummaryProfileManager::initSummaryProfile(const CavaPluginManager *cavaPluginManager,
        kmonitor::MetricsReporter *metricsReporter, SummaryProfile *summaryProfile)
{
    bool ret = true;
    const auto &extractorInfos = summaryProfile->getSummaryProfileInfo()._extractorInfos;
    for (size_t i = 0; i < extractorInfos.size(); i++) {
        const ExtractorInfo &extractorInfo = extractorInfos[i];
        SummaryExtractor *extractor = createSummaryExtractor(extractorInfo,
                cavaPluginManager, metricsReporter);
        if (extractor == nullptr || !summaryProfile->addSummaryExtractor(extractor)) {
            AUTIL_LOG(ERROR, "create summary plugin failed from module [%s]",
                      extractorInfo._moduleName.c_str());
            ret = false;
            break;
        }
    }
    return ret;
}

SummaryExtractor* SummaryProfileManager::createSummaryExtractor(
        const config::ExtractorInfo &extractorInfo,
        const suez::turing::CavaPluginManager *cavaPluginManager,
        kmonitor::MetricsReporter *metricsReporter)
{
    SummaryExtractor *extractor = nullptr;
    if (PlugInManager::isBuildInModule(extractorInfo._moduleName)){
        if(extractorInfo._extractorName == "DefaultSummaryExtractor") {
            extractor = new DefaultSummaryExtractor();
        } else if(extractorInfo._extractorName == "CavaSummaryExtractor") {
            extractor = new CavaSummaryExtractor(extractorInfo._parameters,
                    cavaPluginManager, metricsReporter);
        } else if(extractorInfo._extractorName == "OneSummaryExtractor"){
#ifndef AIOS_OPEN_SOURCE
            extractor = new AdapterSummary(extractorInfo._parameters, _resourceReaderPtr.get(), _hitSummarySchema);
#else
            return nullptr;
#endif
        } else{
            AUTIL_LOG(ERROR, "failed to init SummaryProfile[%s] for BuildInModule",
                    extractorInfo._extractorName.c_str());
            return nullptr;
        }
    } else {
        Module *module = this->getModule(extractorInfo._moduleName);
        if (module == nullptr) {
            AUTIL_LOG(ERROR, "Get module [%s] failed1. module[%p]",
                    extractorInfo._moduleName.c_str(), module);
            return nullptr;
        }
        SummaryModuleFactory *factory =
            dynamic_cast<SummaryModuleFactory *>(module->getModuleFactory());
        if (!factory) {
            AUTIL_LOG(ERROR, "Get SummaryModuleFactory failed!");
            return nullptr;
        }
        extractor = factory->createSummaryExtractor(extractorInfo._extractorName.c_str(),
                extractorInfo._parameters, _resourceReaderPtr.get(),
                _hitSummarySchema);
    }
    return extractor;
}

void SummaryProfileManager::setRequiredAttributeFields(
        const vector<string> &fieldVec)
{
    _requiredAttributeFields = fieldVec;
}

void SummaryProfileManager::clear() {
    for (SummaryProfiles::iterator it = _summaryProfiles.begin();
         it != _summaryProfiles.end(); ++it)
    {
        delete it->second;
    }
    _summaryProfiles.clear();
}

} // namespace summary
} // namespace isearch
