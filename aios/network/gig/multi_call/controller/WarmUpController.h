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
#ifndef ISEARCH_MULTI_CALL_WARMUPCONTROLLER_H
#define ISEARCH_MULTI_CALL_WARMUPCONTROLLER_H

#include "aios/network/gig/multi_call/controller/ControllerFeedBack.h"

namespace multi_call {

class WarmUpController
{
public:
    WarmUpController() : _isFinished(true) {
    }
    ~WarmUpController() {
    }

private:
    WarmUpController(const WarmUpController &);
    WarmUpController &operator=(const WarmUpController &);

public:
    float update(ControllerFeedBack &feedBack);
    bool needProbe() const {
        return !_isFinished;
    }

private:
    // for ut
    void setFinished(bool finished) {
        _isFinished = finished;
    }

private:
    volatile bool _isFinished;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(WarmUpController);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_WARMUPCONTROLLER_H
