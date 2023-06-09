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

#include "suez/turing/expression/common.h"
#include "suez/turing/expression/plugin/CavaScorerAdapter.h"
#include "suez/turing/expression/plugin/Scorer.h"


namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor
namespace suez {
namespace turing {
class CavaPluginManager;
class ScoringProvider;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace rank {

class CavaScorerAdapter : public suez::turing::CavaScorerAdapter
{
public:
    CavaScorerAdapter(const suez::turing::KeyValueMap &scorerParameters,
                      const suez::turing::CavaPluginManager *cavaPluginManager,
                      kmonitor::MetricsReporter *metricsReporter);
    suez::turing::Scorer *clone() override;
private:
    bool callBeginRequestFunc(suez::turing::ScoringProvider *provider) override;
protected:
    std::string getConfigValue(suez::turing::ScoringProvider *provider,
                               const std::string &configKey) override;
};

typedef std::shared_ptr<CavaScorerAdapter> CavaScorerAdapterPtr;
} // namespace rank
} // namespace isearch
