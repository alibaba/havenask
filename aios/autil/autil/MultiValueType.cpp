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
#include "autil/MultiValueType.h"

#include <iomanip>

#include "autil/StringTokenizer.h"

using namespace std;

namespace autil {

template<typename T>
bool MultiValueType<T>::operator == (const std::string& fieldOfInput) const {
    autil::StringTokenizer st(fieldOfInput, std::string(1, MULTI_VALUE_DELIMITER),
                              autil::StringTokenizer::TOKEN_TRIM |
                              autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    if (size() == st.getNumTokens()) {
        for (size_t i = 0; i < st.getNumTokens(); ++i) {
            T valueOfInput;
            StrToT(st[i], valueOfInput);
            if (!CheckT(valueOfInput, (*this)[i])) {
                return false;
            }
        }
        return true;
    }
    return false;
}

template<typename T>
bool MultiValueType<T>::operator != (const std::string& fieldOfInput) const {
    return !(*this == fieldOfInput);
}

#define INST_OPERATOR(Type)                                             \
    template<>                                                          \
    bool Type::operator == (const std::string& fieldOfInput) const; \
    template<>                                                          \
    bool Type::operator != (const std::string& fieldOfInput) const;     \
    
MULTI_VALUE_TYPE_MACRO_HELPER(INST_OPERATOR)

std::ostream& stringMultiTypeStream(std::ostream& stream, MultiChar value) {
    stream << string(value.data(), value.size());
    return stream;
}

template<typename T>
std::ostream& normalMultiTypeStream(std::ostream& stream, T value) {
    for (size_t i = 0; i < value.size(); ++i) {
        if (i != 0) {
            stream << MULTI_VALUE_DELIMITER;
        }
        stream << value[i];
    }
    return stream;
}

template<typename T>
std::ostream& intMultiTypeStream(std::ostream& stream, T value) {
    for (size_t i = 0; i < value.size(); ++i) {
        if (i != 0) {
            stream << MULTI_VALUE_DELIMITER;
        }
        stream << (int)value[i];
    }
    return stream;
}

template<typename T>
std::ostream& floatMultiTypeStream(std::ostream& stream, T value) {
    for (size_t i = 0; i < value.size(); ++i) {
        if (i != 0) {
            stream << MULTI_VALUE_DELIMITER;
        }
        stream << setprecision(6) << value[i];
    }
    return stream;
}

template<typename T>
std::ostream& doubleMultiTypeStream(std::ostream& stream, T value) {
    for (size_t i = 0; i < value.size(); ++i) {
        if (i != 0) {
            stream << MULTI_VALUE_DELIMITER;
        }
        stream << setprecision(15) << value[i];
    }
    return stream;
}

#define STREAM_FUNC(func, type)                                         \
    std::ostream& operator <<(std::ostream& stream, type value) {       \
        return func(stream, value);                                     \
    }

STREAM_FUNC(intMultiTypeStream, MultiUInt8);
STREAM_FUNC(intMultiTypeStream, MultiInt8);
STREAM_FUNC(normalMultiTypeStream, MultiInt16);
STREAM_FUNC(normalMultiTypeStream, MultiUInt16);
STREAM_FUNC(normalMultiTypeStream, MultiInt32);
STREAM_FUNC(normalMultiTypeStream, MultiUInt32);
STREAM_FUNC(normalMultiTypeStream, MultiInt64);
STREAM_FUNC(normalMultiTypeStream, MultiUInt64);
STREAM_FUNC(normalMultiTypeStream, MultiUInt128);
STREAM_FUNC(floatMultiTypeStream, MultiFloat);
STREAM_FUNC(doubleMultiTypeStream, MultiDouble);
STREAM_FUNC(stringMultiTypeStream, MultiChar);
STREAM_FUNC(normalMultiTypeStream, MultiString);

}
