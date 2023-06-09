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
#include "expression/function/SubJoinProcessor.h"

using namespace std;
using namespace autil;
using namespace matchdoc;

namespace expression {

AUTIL_LOG_SETUP(expression, SubJoinVarProcessor);
SubJoinVarProcessor::ValueRefType 
SubJoinVarProcessor::getRefType(ReferenceBase *ref) {
    assert(ref);
    string typeName = ref->getVariableType();
    if (typeName == typeid(MultiInt8).name()) {
        return vrt_multi_int8;
    }

    if (typeName == typeid(MultiInt16).name()) {
        return vrt_multi_int16;
    }

    if (typeName == typeid(MultiInt32).name()) {
        return vrt_multi_int32;
    }

    if (typeName == typeid(MultiInt64).name()) {
        return vrt_multi_int64;
    }

    if (typeName == typeid(MultiUInt8).name()) {
        return vrt_multi_uint8;
    }

    if (typeName == typeid(MultiUInt16).name()) {
        return vrt_multi_uint16;
    }

    if (typeName == typeid(MultiUInt32).name()) {
        return vrt_multi_uint32;
    }

    if (typeName == typeid(MultiUInt64).name()) {
        return vrt_multi_uint64;
    }

    if (typeName == typeid(MultiFloat).name()) {
        return vrt_multi_float;
    }

    if (typeName == typeid(MultiDouble).name()) {
        return vrt_multi_double;
    }

    if (typeName == typeid(MultiChar).name()) {
        return vrt_multi_char;
    }
    if (typeName == typeid(MultiString).name()) {
        return vrt_multi_string;
    }
    return vrt_single;
}

string SubJoinVarProcessor::getStringValue(MatchDoc matchDoc) {
    assert(_ref);
    switch (_refType) {
    case vrt_single: {
        return StringUtil::toString(_ref->toString(matchDoc));
    }
    case vrt_multi_char: {
        Reference<MultiChar>* ref = static_cast<Reference<MultiChar>*>(_ref);
        assert(ref);
        MultiChar value = ref->get(matchDoc);
        return string(value.data(), value.size());
    }

    case vrt_multi_string: {
        Reference<MultiString>* ref = static_cast<Reference<MultiString>*>(_ref);
        assert(ref);
        MultiString value = ref->get(matchDoc);
        stringstream ss;
        for (size_t i = 0; i < value.size(); ++i) {
            if (i > 0) {
                ss << " ";
            }
            ss << string(value[i].data(), value[i].size());
        }
        return ss.str();
    }

#define GET_MULTI_STRING_VALUE(valueType, refType)                      \
        case refType: {                                                 \
            Reference<valueType>* ref = static_cast<Reference<valueType>*>(_ref); \
            assert(ref);                                                \
            valueType value = ref->get(matchDoc);                       \
            stringstream ss;                                            \
            for (size_t i = 0; i < value.size(); ++i) {                 \
                if (i > 0) {                                            \
                    ss << " ";                                          \
                }                                                       \
                ss << StringUtil::toString(value[i]);                   \
            }                                                           \
            return ss.str();                                            \
        }

        GET_MULTI_STRING_VALUE(MultiInt8,  vrt_multi_int8);
        GET_MULTI_STRING_VALUE(MultiInt16, vrt_multi_int16);
        GET_MULTI_STRING_VALUE(MultiInt32, vrt_multi_int32);
        GET_MULTI_STRING_VALUE(MultiInt64, vrt_multi_int64);
        GET_MULTI_STRING_VALUE(MultiUInt8, vrt_multi_uint8);
        GET_MULTI_STRING_VALUE(MultiUInt16, vrt_multi_uint16);
        GET_MULTI_STRING_VALUE(MultiUInt32, vrt_multi_uint32);
        GET_MULTI_STRING_VALUE(MultiUInt64, vrt_multi_uint64);
        GET_MULTI_STRING_VALUE(MultiFloat,  vrt_multi_float);
        GET_MULTI_STRING_VALUE(MultiDouble, vrt_multi_double);
#undef GET_MULTI_STRING_VALUE

    default:
        AUTIL_LOG(WARN, "unsupport refType : %d", (int)_refType);
        return string("unsupport!");
    }
    return string("");
}


}
