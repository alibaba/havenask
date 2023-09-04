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

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {
class ScoringProvider;

struct ScorerParam {
    uint32_t scoreDocCount;
    matchdoc::MatchDoc *matchDocs;
    matchdoc::Reference<score_t> *reference;
    ScorerParam() {
        scoreDocCount = 0;
        matchDocs = NULL;
        reference = NULL;
    }
};

class Scorer {
public:
    Scorer(const std::string &name = "scorer") : _name(name) {}
    virtual ~Scorer() {}

public:
    // For each request, we will call clone to get a new scorer
    virtual Scorer *clone() = 0;
    // called before each request
    virtual bool beginRequest(ScoringProvider *provider) = 0;
    // called for each matched doc
    virtual score_t score(matchdoc::MatchDoc &matchDoc) = 0;
    // this is the default implementation, and can be overrided
    virtual void batchScore(ScorerParam &scorerParam) {
        for (size_t i = 0; i < scorerParam.scoreDocCount; ++i) {
            matchdoc::MatchDoc &matchDoc = scorerParam.matchDocs[i];
            scorerParam.reference->set(matchDoc, score(matchDoc));
        }
    }
    // called after each request
    virtual void endRequest() = 0;
    // call to recycle/delete this scorer
    virtual void destroy() = 0;

    void setWarningInfo(const std::string &info) { _warningInfo = info; }
    const std::string &getWarningInfo() { return _warningInfo; }
    const std::string &getScorerName() const { return _name; }

private:
    std::string _name;
    std::string _warningInfo;
};

} // namespace turing
} // namespace suez
