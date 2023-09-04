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
#include "aios/network/gig/multi_call/interface/SyncClosure.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, SyncClosure);

SyncClosure::SyncClosure() : _isFinished(false) {
}

SyncClosure::~SyncClosure() {
}

void SyncClosure::Run() {
    /**
     * Run may be called before wait. such as in local search situation
     */
    assert(!_isFinished);
    autil::ScopedLock lock(_cond);
    _isFinished = true;
    _cond.signal();
}

int SyncClosure::wait() {
    autil::ScopedLock lock(_cond);
    while (!_isFinished) {
        _cond.wait();
    }
    return 0;
}

} // namespace multi_call
