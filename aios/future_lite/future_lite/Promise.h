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
#ifndef FUTURE_LITE_PROMISE_H
#define FUTURE_LITE_PROMISE_H


#include "future_lite/Common.h"
#include "future_lite/Future.h"
#include <exception>

namespace future_lite {

template <typename T>
class Promise {
public:
    Promise()
        : _sharedState(new FutureState<T>())
        , _hasFuture(false)
    {
        _sharedState->attachPromise();
    }
    ~Promise() {
        if (_sharedState) {
            _sharedState->detachPromise();
        }
    }

    Promise(const Promise& other)
    {
        _sharedState = other._sharedState;
        _hasFuture = other._hasFuture;
        _sharedState->attachPromise();
    }
    // TODO: remove all copy constructor
    Promise& operator=(const Promise& other)
    {
        if (this == &other) {
            return *this;
        }
        this->~Promise();
        _sharedState = other._sharedState;
        _hasFuture = other._hasFuture;
        _sharedState->attachPromise();
        return *this;
    }

    Promise(Promise<T>&& other)
        : _sharedState(other._sharedState)
        , _hasFuture(other._hasFuture)
    {
        other._sharedState = nullptr;
        other._hasFuture = false;
    }
    Promise& operator=(Promise<T>&& other)
    {
        std::swap(_sharedState, other._sharedState);
        std::swap(_hasFuture, other._hasFuture);
        return *this;
    }

public:
    Future<T> getFuture()
    {
        logicAssert(valid(), "Promise is broken");
        logicAssert(!_hasFuture, "Promise already has a future");
        _hasFuture = true;
        return Future<T>(_sharedState);
    }
    bool valid() const
    {
        return _sharedState != nullptr;
    }
    // make the continuation back to origin context
    Promise& checkout() {
        if (_sharedState) {
            _sharedState->checkout();
        }
        return *this;
    }
    Promise& forceSched() {
        if (_sharedState) {
            _sharedState->setForceSched();
        }
        return *this;
    }

public:
    void setException(std::exception_ptr error) {
        logicAssert(valid(), "Promise is broken");
        _sharedState->setResult(Try<T>(error));
    }
    void setValue(T&& v) {
        logicAssert(valid(), "Promise is broken");
        _sharedState->setResult(Try<T>(std::forward<T>(v)));
    }
    void setValue(Try<T>&& t) {
        logicAssert(valid(), "Promise is broken");
        _sharedState->setResult(std::move(t));
    }
    Executor* getExecutor() {
        return _sharedState ? _sharedState->getExecutor() : nullptr;
    }
    void setExecutor(Executor* executor) {
        if (_sharedState) {
            _sharedState->setExecutor(executor);
        }
    }

private:

private:
    FutureState<T>* _sharedState;
    bool _hasFuture;
};

}

#endif //FUTURE_LITE_PROMISE_H
