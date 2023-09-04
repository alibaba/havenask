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
#include "swift/client/Notifier.h"

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, Notifier);

Notifier::Notifier() : _needNotify(false) {}

Notifier::~Notifier() {}

void Notifier::setNeedNotify(bool needNotify) {
    autil::ScopedLock _lock(_cond);
    _needNotify = needNotify;
}

void Notifier::notify() {
    autil::ScopedLock _lock(_cond);
    if (_needNotify) {
        _cond.broadcast();
        _needNotify = false;
    }
}

bool Notifier::wait(int64_t timeout) {
    if (timeout <= 0) {
        autil::ScopedLock _lock(_cond);
        while (_needNotify) {
            _cond.wait();
        }
        return true;
    } else {
        autil::ScopedLock _lock(_cond);
        if (!_needNotify) {
            return true;
        } else {
            _cond.wait(timeout);
            return (!_needNotify);
        }
    }
}

} // namespace client
} // namespace swift
