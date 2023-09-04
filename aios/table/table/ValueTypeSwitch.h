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

#include "matchdoc/ValueType.h"

namespace table {

class ValueTypeSwitch {
public:
    template <typename T>
    struct CppTypeTag {
        typedef T value_type;
    };

public:
    template <typename F1, typename F2>
    static bool switchType(matchdoc::ValueType vt, F1 func, F2 multiFunc);
    template <typename F1, typename F2>
    static bool switchType(matchdoc::BuiltinType bt, bool isMulti, F1 func, F2 multiFunc);
    template <typename F1, typename F2, typename F3, typename F4>
    static bool switchType(matchdoc::ValueType vt, F1 func, F2 multiFunc, F3 stringFunc, F4 multiStringFunc);
    template <typename F1, typename F2, typename F3, typename F4>
    static bool
    switchType(matchdoc::BuiltinType bt, bool isMulti, F1 func, F2 multiFunc, F3 stringFunc, F4 multiStringFunc);

private:
    AUTIL_LOG_DECLARE();
};

template <typename F1, typename F2>
bool ValueTypeSwitch::switchType(matchdoc::ValueType vt, F1 func, F2 multiFunc) {
    return switchType(vt.getBuiltinType(), vt.isMultiValue(), func, multiFunc);
}

template <typename F1, typename F2>
bool ValueTypeSwitch::switchType(matchdoc::BuiltinType bt, bool isMulti, F1 func, F2 multiFunc) {
    using matchdoc::MatchDocBuiltinType2CppType;
    switch (bt) {
#define CASE_MACRO(ft)                                                                                                 \
    case ft: {                                                                                                         \
        if (isMulti) {                                                                                                 \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;                                                  \
            return multiFunc(CppTypeTag<T>());                                                                         \
        } else {                                                                                                       \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                                 \
            return func(CppTypeTag<T>());                                                                              \
        }                                                                                                              \
        break;                                                                                                         \
    }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
    case matchdoc::BuiltinType::bt_bool: {
        if (isMulti) {
            AUTIL_LOG(ERROR, "multi bool type not supported");
            return false;
        } else {
            return func(CppTypeTag<bool>());
        }
    }
#undef CASE_MACRO
    default: {
        return false;
    }
    } // switch
    return true;
}

template <typename F1, typename F2, typename F3, typename F4>
bool ValueTypeSwitch::switchType(matchdoc::ValueType vt, F1 func, F2 multiFunc, F3 stringFunc, F4 multiStringFunc) {
    return switchType(vt.getBuiltinType(), vt.isMultiValue(), func, multiFunc, stringFunc, multiStringFunc);
}

template <typename F1, typename F2, typename F3, typename F4>
bool ValueTypeSwitch::switchType(
    matchdoc::BuiltinType bt, bool isMulti, F1 func, F2 multiFunc, F3 stringFunc, F4 multiStringFunc) {
    using matchdoc::MatchDocBuiltinType2CppType;
    switch (bt) {
#define CASE_MACRO(ft)                                                                                                 \
    case ft: {                                                                                                         \
        if (isMulti) {                                                                                                 \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;                                                  \
            return multiFunc(CppTypeTag<T>());                                                                         \
        } else {                                                                                                       \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                                 \
            return func(CppTypeTag<T>());                                                                              \
        }                                                                                                              \
        break;                                                                                                         \
    }
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    case matchdoc::BuiltinType::bt_string: {
        if (isMulti) {
            return multiStringFunc();
        } else {
            return stringFunc();
        }
        break;
    }
    case matchdoc::BuiltinType::bt_bool: {
        if (isMulti) {
            AUTIL_LOG(ERROR, "multi bool type not supported");
            return false;
        } else {
            return func(CppTypeTag<bool>());
        }
        break;
    }
    default: {
        return false;
    }
    } // switch
    return true;
}

} // namespace table
