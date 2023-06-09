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
#ifndef ISEARCH_BS_CUSTOMMERGERCREATOR_H
#define ISEARCH_BS_CUSTOMMERGERCREATOR_H

#include "build_service/common_define.h"
#include "build_service/config/MergePluginConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/custom_merger/CustomMerger.h"
#include "build_service/custom_merger/MergeResourceProvider.h"
#include "build_service/util/Log.h"

namespace build_service { namespace plugin {
class PlugInManager;
BS_TYPEDEF_PTR(PlugInManager);
}} // namespace build_service::plugin

namespace build_service { namespace custom_merger {

class CustomMergerCreator
{
public:
    CustomMergerCreator();
    ~CustomMergerCreator();

private:
    CustomMergerCreator(const CustomMergerCreator&);
    CustomMergerCreator& operator=(const CustomMergerCreator&);

public:
    bool init(const config::ResourceReaderPtr& resourceReaderPtr, indexlib::util::MetricProviderPtr metricProvider)
    {
        _resourceReaderPtr = resourceReaderPtr;
        _metricProvider = metricProvider;
        return true;
    }
    CustomMergerPtr create(const config::MergePluginConfig& mergeConfig, MergeResourceProviderPtr& resourceProvider,
                           uint32_t backupId = 0, const std::string& epochId = "");
    CustomMerger::CustomMergerInitParam getTestParam();
    CustomMerger::CustomMergerInitParam getTestParam2()
    {
        CustomMerger::CustomMergerInitParam param; // add index provider
        KeyValueMap parameters;
        // param.resourceProvider = resourceProvider;
        param.parameters = parameters;
        return param;
    }

private:
    config::ResourceReaderPtr _resourceReaderPtr;
    indexlib::util::MetricProviderPtr _metricProvider;
    plugin::PlugInManagerPtr _pluginManager;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CustomMergerCreator);

}} // namespace build_service::custom_merger

#endif // ISEARCH_BS_CUSTOMMERGERCREATOR_H
