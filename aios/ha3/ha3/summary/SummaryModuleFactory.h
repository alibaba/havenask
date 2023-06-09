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
#pragma once

#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "build_service/common_define.h"
#include "build_service/plugin/ModuleFactory.h"
#include "ha3/config/ResourceReader.h"

namespace isearch {
namespace config {
class HitSummarySchema;
}  // namespace config
namespace summary {
class SummaryExtractor;
}  // namespace summary
}  // namespace isearch

namespace isearch {
namespace summary {

static const std::string MODULE_FUNC_SUMMARY = "_Summary";
class SummaryModuleFactory : public build_service::plugin::ModuleFactory
{
public:
    SummaryModuleFactory() {}
    virtual ~SummaryModuleFactory() {}
public:
    virtual SummaryExtractor* createSummaryExtractor(const char *extractorName,
            const build_service::KeyValueMap &extractorParameters,
            config::ResourceReader *resourceReader,
            config::HitSummarySchema *hitSummarySchema) = 0;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryModuleFactory> SummaryModuleFactoryPtr;

} // namespace summary
} // namespace isearch

