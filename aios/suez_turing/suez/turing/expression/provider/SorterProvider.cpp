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
#include "suez/turing/expression/provider/SorterProvider.h"

#include <iosfwd>

#include "autil/Log.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
} // namespace partition
} // namespace indexlib
namespace matchdoc {
class MatchDocAllocator;
} // namespace matchdoc
namespace suez {
namespace turing {
class SuezCavaAllocator;
class Tracer;
} // namespace turing
} // namespace suez

using namespace std;
using namespace indexlib::partition;

AUTIL_DECLARE_AND_SETUP_LOGGER(expression, SorterProvider);

namespace suez {
namespace turing {

SorterProvider::SorterProvider(matchdoc::MatchDocAllocator *allocator,
                               autil::mem_pool::Pool *pool,
                               SuezCavaAllocator *cavaAllocator,
                               Tracer *requestTracer,
                               PartitionReaderSnapshot *partitionReaderSnapshot,
                               const KeyValueMap *kvpairs,
                               kmonitor::MetricsReporter *metricsReporter,
                               DocTracer *docTracerPtr)
    : ProviderBase(allocator, pool, cavaAllocator, requestTracer, partitionReaderSnapshot, kvpairs, metricsReporter)
    , _docTracerPtr(docTracerPtr) {}

SorterProvider::~SorterProvider() {}

DocTracer *SorterProvider::getDocTracer() { return _docTracerPtr; }

} // namespace turing
} // namespace suez
