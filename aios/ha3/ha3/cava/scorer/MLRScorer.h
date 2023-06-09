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

#include <vector>

#include "ha3/cava/scorer/CompoundScorerDef.h"
#include "ha3/rank/ScoringProvider.h"

namespace isearch {
namespace compound_scorer {

class MLRScorer {
public:
    MLRScorer(const std::string &sortFields);
    ~MLRScorer() {}
    bool initWorker(std::vector<WorkerType> &result,
                    isearch::rank::ScoringProvider *provider,
                    bool isASC = false);
private:
    bool parse(const std::string &sField, const std::string &sValue,
               isearch::rank::ScoringProvider *provider,
               std::vector<WorkerType> &result, score_t MLRWeight);
private:
    std::set<std::string> _setSortField;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace compound_scorer
} // namespace isearch

