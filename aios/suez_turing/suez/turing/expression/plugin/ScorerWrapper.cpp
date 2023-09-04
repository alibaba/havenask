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
#include "suez/turing/expression/plugin/ScorerWrapper.h"

#include <assert.h>
#include <stddef.h>

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, ScorerWrapper);

ScorerWrapper::ScorerWrapper(Scorer *scorer, matchdoc::Reference<score_t> *scoreRef)
    : _provider(NULL), _scorer(scorer), _scoreRef(scoreRef) {
    assert(_scorer);
}

ScorerWrapper::~ScorerWrapper() { _scorer->destroy(); }

bool ScorerWrapper::beginRequest(ScoringProvider *provider) {
    _provider = provider;
    auto ret = _scorer->beginRequest(provider);
    auto allocator = provider->getAllocator();
    allocator->extend();
    if (allocator->hasSubDocAllocator()) {
        allocator->extendSub();
    }
    return ret;
}

void ScorerWrapper::endRequest() { _scorer->endRequest(); }

} // namespace turing
} // namespace suez
