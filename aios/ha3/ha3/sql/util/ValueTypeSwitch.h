
#pragma once
#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <matchdoc/ValueType.h>

BEGIN_HA3_NAMESPACE(sql);

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
    static bool switchType(matchdoc::BuiltinType bt, bool isMulti,
                           F1 func, F2 multiFunc);
    template <typename F1, typename F2, typename F3, typename F4>
    static bool switchType(matchdoc::ValueType vt,
                           F1 func, F2 multiFunc,
                           F3 stringFunc, F4 multiStringFunc);
    template <typename F1, typename F2, typename F3, typename F4>
    static bool switchType(matchdoc::BuiltinType bt, bool isMulti,
                           F1 func, F2 multiFunc,
                           F3 stringFunc, F4 multiStringFunc);
private:
    HA3_LOG_DECLARE();
};

template <typename F1, typename F2>
bool ValueTypeSwitch::switchType(matchdoc::ValueType vt, F1 func, F2 multiFunc) {
    return switchType(vt.getBuiltinType(), vt.isMultiValue(), func, multiFunc);
}

template <typename F1, typename F2>
bool ValueTypeSwitch::switchType(matchdoc::BuiltinType bt, bool isMulti,
                                 F1 func, F2 multiFunc)
{
    using matchdoc::MatchDocBuiltinType2CppType;
    switch (bt) {
#define CASE_MACRO(ft)                                                  \
        case ft: {                                                      \
            if (isMulti) {                                              \
                typedef MatchDocBuiltinType2CppType<ft, true>::CppType T; \
                return multiFunc(CppTypeTag<T>());                      \
            } else {                                                    \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType T; \
                return func(CppTypeTag<T>());                           \
            }                                                           \
            break;                                                      \
        }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
    case matchdoc::BuiltinType::bt_bool: {
        if (isMulti) {
            SQL_LOG(ERROR, "multi bool type not supported");
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
bool ValueTypeSwitch::switchType(matchdoc::ValueType vt,
                                 F1 func, F2 multiFunc,
                                 F3 stringFunc, F4 multiStringFunc)
{
    return switchType(vt.getBuiltinType(), vt.isMultiValue(),
                      func, multiFunc, stringFunc, multiStringFunc);
}

template <typename F1, typename F2, typename F3, typename F4>
bool ValueTypeSwitch::switchType(matchdoc::BuiltinType bt, bool isMulti,
                                 F1 func, F2 multiFunc,
                                 F3 stringFunc, F4 multiStringFunc)
{
    using matchdoc::MatchDocBuiltinType2CppType;
    switch (bt) {
#define CASE_MACRO(ft)                                                  \
        case ft: {                                                      \
            if (isMulti) {                                              \
                typedef MatchDocBuiltinType2CppType<ft, true>::CppType T; \
                return multiFunc(CppTypeTag<T>());                      \
            } else {                                                    \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType T; \
                return func(CppTypeTag<T>());                           \
            }                                                           \
            break;                                                      \
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
            SQL_LOG(ERROR, "multi bool type not supported");
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

END_HA3_NAMESPACE(sql);
