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
#include "ha3/config/ResourceReader.h"
#include "ha3/search/OptimizerChainManager.h"

namespace isearch {
namespace config {
class SearchOptimizerConfig;
}  // namespace config
}  // namespace isearch

namespace isearch {
namespace search {

class OptimizerChainManagerCreator
{
public:
    OptimizerChainManagerCreator(const config::ResourceReaderPtr &resourceReaderPtr);
    ~OptimizerChainManagerCreator();
private:
    OptimizerChainManagerCreator(const OptimizerChainManagerCreator &);
    OptimizerChainManagerCreator& operator=(const OptimizerChainManagerCreator &);
private:
    void addBuildInOptimizers(OptimizerChainManagerPtr &optimizerChainManagerPtr);
public:
    OptimizerChainManagerPtr create(const config::SearchOptimizerConfig &optimizerConfig);
    OptimizerChainManagerPtr createFromString(const std::string &configStr);  //for test
private:
    config::ResourceReaderPtr _resourceReaderPtr;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<OptimizerChainManagerCreator> OptimizerChainManagerCreatorPtr;

} // namespace search
} // namespace isearch

