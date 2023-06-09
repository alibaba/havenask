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
#include "indexlib/util/memory_control/MemoryQuotaSynchronizer.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, MemoryQuotaSynchronizer);

MemoryQuotaSynchronizer::MemoryQuotaSynchronizer(const BlockMemoryQuotaControllerPtr& memController)
    : _simpleMemControl(memController)
    , _lastMemoryUse(0)
{
}

MemoryQuotaSynchronizer::~MemoryQuotaSynchronizer() {}

void MemoryQuotaSynchronizer::SyncMemoryQuota(int64_t memoryUse)
{
    ScopedLock lock(_lock);
    if (memoryUse == _lastMemoryUse) {
        return;
    }

    if (memoryUse > _lastMemoryUse) {
        _simpleMemControl.Allocate(memoryUse - _lastMemoryUse);
    } else {
        _simpleMemControl.Free(_lastMemoryUse - memoryUse);
    }
    _lastMemoryUse = memoryUse;
}
}} // namespace indexlib::util
