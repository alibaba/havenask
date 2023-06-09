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
#include "ha3/summary/SummaryExtractorChain.h"

#include <iosfwd>

#include "ha3/summary/SummaryExtractor.h"
#include "autil/Log.h"

namespace isearch {
namespace common {
class SummaryHit;
}  // namespace common
namespace summary {
class SummaryExtractorProvider;
}  // namespace summary
}  // namespace isearch

using namespace std;
namespace isearch {
namespace summary {
AUTIL_LOG_SETUP(ha3, SummaryExtractorChain);

SummaryExtractorChain::SummaryExtractorChain() { 
}

SummaryExtractorChain::SummaryExtractorChain(const SummaryExtractorChain &other)
{
    for (ConstIterator it = other._extractors.begin(); 
         it != other._extractors.end(); it++)
    {
        _extractors.push_back((*it)->clone());
    }
}

SummaryExtractorChain::~SummaryExtractorChain() { 
    for (Iterator it = _extractors.begin(); it != _extractors.end(); it++) {
        (*it)->destory();
    }
    _extractors.clear();
}

void SummaryExtractorChain::addSummaryExtractor(SummaryExtractor *extractor) {
    _extractors.push_back(extractor);
}

SummaryExtractorChain* SummaryExtractorChain::clone() const {
    SummaryExtractorChain *chain = new SummaryExtractorChain(*this);
    return chain;
}

void SummaryExtractorChain::destroy() {
    delete this;
}

bool SummaryExtractorChain::beginRequest(SummaryExtractorProvider *provider) {
    for (Iterator it = _extractors.begin(); it != _extractors.end(); it++) {
        if (!(*it)->beginRequest(provider)) {
            return false;
        }
    }
    return true;
}

void SummaryExtractorChain::extractSummary(common::SummaryHit &summaryHit) {
    for (Iterator it = _extractors.begin(); it != _extractors.end(); it++) {
        (*it)->extractSummary(summaryHit);
    }
}
    
void SummaryExtractorChain::endRequest() {
    for (Iterator it = _extractors.begin(); it != _extractors.end(); it++) {
        (*it)->endRequest();
    }
}

} // namespace summary
} // namespace isearch

