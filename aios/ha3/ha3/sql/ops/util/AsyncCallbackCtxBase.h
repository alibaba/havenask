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
#pragma once

#include "autil/Lock.h"
#include "autil/ObjectTracer.h"
#include "ha3/isearch.h"
#include "navi/engine/Data.h"
#include "navi/log/NaviLogger.h" // IWYU pragma: keep

namespace isearch {
namespace sql {

class ActivationData : public navi::Data {
private:
    ActivationData();

public:
    ~ActivationData();
    ActivationData(const ActivationData &) = delete;
    ActivationData &operator=(const ActivationData &) = delete;

public:
    static const std::shared_ptr<ActivationData> &inst();
    // only used for async pipe activation, no payload
};

class AsyncCallbackCtxBase : autil::ObjectTracer<AsyncCallbackCtxBase, true>
{
public:
    AsyncCallbackCtxBase();
    virtual ~AsyncCallbackCtxBase();
    AsyncCallbackCtxBase(const AsyncCallbackCtxBase &) = delete;
    AsyncCallbackCtxBase& operator=(const AsyncCallbackCtxBase &) = delete;
public:
    void setTracePtr(void *tracePtr) {
        // only used for gdb
        _tracePtr = tracePtr;
    }

protected:
    void incStartVersion();
    int64_t incCallbackVersion();
    bool isInFlightNoLock() const;

protected:
    mutable autil::SpinLock _statLock;

private:
    void *_tracePtr = nullptr;
    size_t _startVersion = 0;
    size_t _callbackVersion = 0;
    int64_t _startTime = 0;

protected:
    DECLARE_LOGGER();
};

typedef std::shared_ptr<AsyncCallbackCtxBase> AsyncCallbackCtxBasePtr;

} //end namespace sql
} //end namespace isearch
