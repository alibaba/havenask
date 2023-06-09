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
#ifndef FUTURE_LITE_MOVEWRAPPER_H
#define FUTURE_LITE_MOVEWRAPPER_H


#include "future_lite/Common.h"
#include <exception>

namespace future_lite {

// std::function requre copyConstructable, hence we provide MoveWrapper perform copy as move

template<typename T>
class MoveWrapper {
public:
    MoveWrapper() = default;
    MoveWrapper(T&& value)
        : _value(std::move(value))
    {
    }

    MoveWrapper(const MoveWrapper& other)
        : _value(std::move(other._value))
    {
    }
    MoveWrapper(MoveWrapper&& other)
        : _value(std::move(other._value))
    {
    }    

    MoveWrapper& operator=(const MoveWrapper& ) = delete;
    MoveWrapper& operator=(MoveWrapper&&) = delete;

    T& get()
    {
        return _value;
    }
    const T& get() const
    {
        return _value;
    }

    ~MoveWrapper() {}

private:
    mutable T _value;
};

}

#endif //FUTURE_LITE_VALUE_H
