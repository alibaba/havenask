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
#include "suez/turing/expression/provider/MatchData.h"

#include <iosfwd>

#include "autil/Log.h"
#include "matchdoc/ReferenceTypesWrapper.h"

using namespace std;
namespace suez {
namespace turing {
AUTIL_LOG_SETUP(expression, MatchData);

MatchData::MatchData() : _numTerms(0), _maxNumTerms(0) {}

MatchData::~MatchData() { reset(); }

std::string MatchData::toString() const {
    std::stringstream ss;
    ss << "term_count = " << _numTerms << ", max_num_terms = " << _maxNumTerms << ", ";
    for (uint32_t i = 0; i < getNumTerms(); ++i) {
        const auto &tmd = getTermMatchData(i);
        ss << "isMatched = " << (tmd.isMatched() ? "true" : "false") << ",";
        ss << "tf=" << tmd.getTermFreq() << ",";
        ss << "doc_pay_load=" << tmd.getDocPayload() << "\t";
    }
    return ss.str();
}

REGISTER_MATCHDOC_TYPE(MatchData);

} // namespace turing
} // namespace suez
