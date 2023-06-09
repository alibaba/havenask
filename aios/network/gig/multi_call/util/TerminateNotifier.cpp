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
#include "aios/network/gig/multi_call/util/TerminateNotifier.h"
#include "aios/network/gig/multi_call/interface/Closure.h"

using namespace std;
using namespace autil;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, TerminateNotifier);

TerminateNotifier::TerminateNotifier() : _partitionCount(0), _closure(NULL) {}

TerminateNotifier::~TerminateNotifier() { _closure = NULL; }

void TerminateNotifier::inc(int partIdIndex) {
    autil::ScopedLock lock(_mutex);
    incPartitionsByIndex(partIdIndex);
}

void TerminateNotifier::dec(int partIdIndex) {
    Closure *closure = NULL;
    {
        autil::ScopedLock lock(_mutex);
        decPartitionsByIndex(partIdIndex);
        if (canTerminate()) {
            if (_closure) {
                closure = _closure;
                _closure = NULL;
            }
        }
    }
    if (closure) {
        closure->Run();
    }
}

void TerminateNotifier::setClosure(Closure *closure) {
    assert(_closure == NULL);
    bool canRun = false;
    {
        autil::ScopedLock lock(_mutex);
        if (canTerminate()) {
            canRun = true;
        } else {
            _closure = closure;
        }
    }
    if (canRun && closure) {
        closure->Run();
    }
}

Closure *TerminateNotifier::stealClosure() {
    Closure *closure = NULL;
    {
        autil::ScopedLock lock(_mutex);
        closure = _closure;
        _closure = NULL;
    }
    return closure;
}

void TerminateNotifier::incPartitionsByIndex(int index) {
    if (index < 0) {
        return;
    }
    int actualSize = _partitions.size();
    int expectSize = index + 1;
    if (actualSize < expectSize) {
        _partitions.resize(expectSize, 0);
    }
    if (0 == _partitions[index]) {
        _partitionCount = _partitionCount + 1;
    }
    ++_partitions[index];
}

void TerminateNotifier::decPartitionsByIndex(int index) {
    if (index < 0 || index >= (int)_partitions.size()) {
        return;
    }
    --_partitions[index];
    if (0 == _partitions[index]) {
        _partitionCount = _partitionCount - 1;
        assert(_partitionCount >= 0);
    }
}

} // namespace multi_call
