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
#include "turing_ops_util/util/DocTracer.h"

using namespace std;
using namespace autil;
using namespace matchdoc;

namespace suez {
namespace turing {
AUTIL_LOG_SETUP(turing, DocTracer);

DocTracer::DocTracer(matchdoc::MatchDocAllocator *allocator) {
    _traceRef = allocator->findReference<Tracer>(RANK_TRACER_NAME);
    if (_traceRef != nullptr) {
        return;
    }
    _traceRef = allocator->declare<Tracer>(RANK_TRACER_NAME, RANK_TRACER_NAME, SL_FRONTEND);
    if (_traceRef != nullptr) {
        allocator->extendGroup(RANK_TRACER_NAME);
    } else {
        AUTIL_LOG(WARN, "declare trace failed in debug mode");
    }
    return;
}

void DocTracer::appendTraceToDoc(matchdoc::MatchDoc doc, const std::string &docTrace) {
    if (unlikely(_traceRef != nullptr) && !docTrace.empty()) {
        _traceRef->getReference(doc).addTrace(docTrace);
    }
}

} // namespace turing

} // namespace suez