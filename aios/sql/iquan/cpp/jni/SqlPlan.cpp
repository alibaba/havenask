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
#include "iquan/jni/SqlPlan.h"

using namespace std;
using namespace autil::legacy::json;

namespace iquan {

AUTIL_LOG_SETUP(iquan, PlanOp);

bool PlanOp::PlanOpHandle::writeAny(const autil::legacy::Any &any,
                                    const std::string &expectedType,
                                    bool copy) {
    if (IsJsonBool(any) && expectedType == "boolean") {
        bool value = autil::legacy::AnyCast<bool>(any);
        return Bool(value);
    } else if (IsJsonString(any) && Utils::isStringType(expectedType)) {
        std::string value = autil::legacy::AnyCast<std::string>(any);
        return writer.String(value.c_str(), value.size(), copy);
    } else if (IsJsonString(any) && Utils::isNumberType(expectedType)) {
        std::string numberStr = autil::legacy::AnyCast<std::string>(any);
        if (numberStr.find(".") == std::string::npos) {
            int64_t value = 0;
            if (!autil::StringUtil::strToInt64(numberStr.c_str(), value)) {
                throw IquanException(std::string("failed to cast string to int64: " + numberStr));
            }
            return Int64(value);
        } else {
            double value = 0;
            if (!autil::StringUtil::strToDouble(numberStr.c_str(), value)) {
                throw IquanException(std::string("failed to cast string to double: " + numberStr));
            }
            return Double(value);
        }
        return true;
    } else if (IsJsonNumber(any) && Utils::isStringType(expectedType)) {
        JsonNumber jsonNumber = autil::legacy::AnyCast<JsonNumber>(any);
        std::string &numberStr = jsonNumber.AsString();
        return writer.String(numberStr.c_str(), numberStr.size(), copy);
    } else if (IsJsonNumber(any) && Utils::isNumberType(expectedType)) {
        JsonNumber jsonNumber = autil::legacy::AnyCast<JsonNumber>(any);
        std::string &numberStr = jsonNumber.AsString();
        if (numberStr.find(".") == std::string::npos) {
            int64_t value = 0;
            if (!autil::StringUtil::strToInt64(numberStr.c_str(), value)) {
                throw IquanException(std::string("failed to cast string to int64: " + numberStr));
            }
            return Int64(value);
        } else {
            double value = 0;
            if (!autil::StringUtil::strToDouble(numberStr.c_str(), value)) {
                throw IquanException(std::string("failed to cast string to double: " + numberStr));
            }
            return Double(value);
        }
        return true;
    }

    const std::type_info &type = any.GetType();
    if (Utils::isNumberType(expectedType)) {
#ifdef RETURN_TYPE_STRING
#undef RETURN_TYPE_STRING
#endif
#define RETURN_TYPE_STRING(TARGETTYPE, FUNC)                                                       \
    if (type == typeid(TARGETTYPE)) {                                                              \
        TARGETTYPE value = autil::legacy::AnyCast<TARGETTYPE>(any);                                \
        return FUNC(value);                                                                        \
    }

        RETURN_TYPE_STRING(int8_t, Int);
        RETURN_TYPE_STRING(uint8_t, Int);
        RETURN_TYPE_STRING(int16_t, Int);
        RETURN_TYPE_STRING(uint16_t, Int);
        RETURN_TYPE_STRING(int32_t, Int);
        RETURN_TYPE_STRING(uint32_t, Uint);
        RETURN_TYPE_STRING(int64_t, Int64);
        RETURN_TYPE_STRING(uint64_t, Uint64);
        RETURN_TYPE_STRING(float, Double);
        RETURN_TYPE_STRING(double, Double);

#undef RETURN_TYPE_STRING
    } else if (Utils::isStringType(expectedType)) {
#ifdef RETURN_TYPE_STRING
#undef RETURN_TYPE_STRING
#endif
#define RETURN_TYPE_STRING(TARGETTYPE)                                                             \
    if (type == typeid(TARGETTYPE)) {                                                              \
        TARGETTYPE value = autil::legacy::AnyCast<TARGETTYPE>(any);                                \
        std::string valueStr = autil::StringUtil::toString(value);                                 \
        return writer.String(valueStr.c_str(), valueStr.size(), copy);                             \
    }

        RETURN_TYPE_STRING(int8_t);
        RETURN_TYPE_STRING(uint8_t);
        RETURN_TYPE_STRING(int16_t);
        RETURN_TYPE_STRING(uint16_t);
        RETURN_TYPE_STRING(int32_t);
        RETURN_TYPE_STRING(uint32_t);
        RETURN_TYPE_STRING(int64_t);
        RETURN_TYPE_STRING(uint64_t);
        RETURN_TYPE_STRING(float);
        RETURN_TYPE_STRING(double);

#undef RETURN_TYPE_STRING
    }

    const std::string &typeName = GetJsonTypeName(any);
    throw IquanException(std::string("any type(" + typeName + ") cast to expect type("
                                     + expectedType + ") is not support"));
}

bool PlanOp::PlanOpHandle::parseDynamicParamsContent(autil::StringView &planStr,
                                                     const std::string &prefixStr,
                                                     const std::string &suffixStr,
                                                     std::string &keyStr,
                                                     std::string &typeStr) {
    // 1. find prefix of replae params
    size_t prefixPos = planStr.find(prefixStr, (size_t)0);
    if (unlikely(prefixPos == std::string::npos)) {
        return false;
    }
    assert(prefixPos == 0);

    // 2. find suffix of replace params
    size_t suffixPos = planStr.find(suffixStr, prefixPos + prefixStr.size());
    if (unlikely(suffixPos == std::string::npos)) {
        return false;
    }

    // 3. get the string of the dynamic params, and check it
    autil::StringView contentStr
        = planStr.substr(prefixPos + prefixStr.size(), suffixPos - prefixPos - prefixStr.size());

    // 4. get key and type
    size_t indexOfSepPos = contentStr.find(IQUAN_DYNAMIC_PARAMS_SEPARATOR);
    if (unlikely(indexOfSepPos == std::string::npos)) {
        throw IquanException("can not find " + IQUAN_DYNAMIC_PARAMS_SEPARATOR + " in "
                             + contentStr.to_string());
    }
    keyStr = contentStr.substr(0, indexOfSepPos).to_string();
    if (unlikely(keyStr.empty())) {
        throw IquanException("key is empty, " + contentStr.to_string());
    }
    typeStr = contentStr.substr(indexOfSepPos + IQUAN_DYNAMIC_PARAMS_SEPARATOR.size()).to_string();
    if (unlikely(typeStr.empty())) {
        throw IquanException("type is empty, " + contentStr.to_string());
    }

    return true;
}

// the replace parameters's format is like:
//     [replace_params[[key#string]]replace_params]
// \"[replace_params[[                  --> prefix
// key                                  --> key of replace parameter
// #                                    --> separator
// string                               --> field type
// ]]replace_params]\"                  --> suffix
bool PlanOp::PlanOpHandle::doReplaceParams(autil::StringView &planStr, bool copy) {
    std::string keyStr;
    std::string typeStr;
    if (!parseDynamicParamsContent(
            planStr, IQUAN_REPLACE_PARAMS_PREFIX, IQUAN_REPLACE_PARAMS_SUFFIX, keyStr, typeStr)) {
        return false;
    }

    // write replace params
    const autil::legacy::Any &value = innerDynamicParams->at(keyStr);
    std::string typeStr2(typeStr.c_str(), typeStr.size());
    writeAny(value, typeStr2, copy);
    return true;
}

// the dynamic parameters's format is like:
//     [dynamic_params[[?0#string]]dynamic_params]
// \"[dynamic_params[[                  --> prefix
// ?0                                   --> index of dynamic parameter
// #                                    --> separator
// string                               --> field type
// ]]dynamic_params]\"                  --> suffix
bool PlanOp::PlanOpHandle::doReplaceDynamicParams(autil::StringView &planStr, bool copy) {
    std::string keyStr;
    std::string typeStr;
    if (!parseDynamicParamsContent(
            planStr, IQUAN_DYNAMIC_PARAMS_PREFIX, IQUAN_DYNAMIC_PARAMS_SUFFIX, keyStr, typeStr)) {
        return false;
    }
    uint32_t index = -1;
    std::string indexStr2(keyStr.c_str(), keyStr.size());
    if (unlikely(!autil::StringUtil::strToUInt32(indexStr2.c_str(), index))) {
        throw IquanException("indexStr " + indexStr2 + " can not cast to uint32");
    }

    // write dynamic params
    const autil::legacy::Any &value = dynamicParams->at(index);
    std::string typeStr2(typeStr.c_str(), typeStr.size());
    writeAny(value, typeStr2, copy);
    return true;
}

// the hint parameters's format is like:
//     [hint_params[key]hint_params]
// \"[hint_params[                  --> prefix
// key                              --> key of hint parameter
// ]hint_params]\"                  --> suffix
bool PlanOp::PlanOpHandle::doReplaceHintParams(autil::StringView &planStr, bool copy) {
    // 1. find prefix of hint params
    size_t prefixPos = planStr.find(IQUAN_HINT_PARAMS_PREFIX, (size_t)0);
    if (unlikely(prefixPos == std::string::npos)) {
        return false;
    }
    assert(prefixPos == 0);

    // 2. find suffix of hint params
    size_t suffixPos
        = planStr.find(IQUAN_HINT_PARAMS_SUFFIX, prefixPos + IQUAN_HINT_PARAMS_PREFIX.size());
    if (unlikely(suffixPos == std::string::npos)) {
        return false;
    }

    // 3. get the key of the HINT params, and check it
    autil::StringView contentStr
        = planStr.substr(prefixPos + IQUAN_HINT_PARAMS_PREFIX.size(),
                         suffixPos - prefixPos - IQUAN_HINT_PARAMS_PREFIX.size());

    std::string keyStr(contentStr.data(), contentStr.size());

    if (unlikely(!dynamicParams->findHintParam(keyStr))) {
        throw IquanException("key [" + keyStr + "] not found in HintParams");
    }

    // 4. write hint params
    std::string valueStr = dynamicParams->getHintParam(keyStr);
    writer.String(valueStr.c_str(), valueStr.size(), copy);
    return true;
}

std::string PlanOp::jsonAttr2Str(const DynamicParams *dynamicParams,
                                 const DynamicParams *innerDynamicParams,
                                 autil::legacy::RapidValue *attr) const {
    rapidjson::StringBuffer buf;
    autil::legacy::RapidWriter writer(buf);
    PlanOpHandle handle(writer, dynamicParams, innerDynamicParams);
    if (attr == jsonAttrs) {
        if (!attr->IsObject()) {
            return "{}";
        }
        writer.StartObject();
        std::set<std::string> patchKeys;
        patchJson(writer, patchKeys);
        for (auto iter = attr->MemberBegin(); iter != attr->MemberEnd(); ++iter) {
            string key(iter->name.GetString(), iter->name.GetStringLength());
            if (patchKeys.count(key) == 0u) {
                writer.Key(key.c_str(), key.size());
                iter->value.Accept(handle);
            }
        }
        writer.EndObject();
    } else {
        attr->Accept(handle);
    }
    return buf.GetString();
}

void PlanOp::patchJson(autil::legacy::RapidWriter &writer, std::set<std::string> &patchKeys) const {
    patchJson(writer, patchStrAttrs, patchKeys);
    patchJson(writer, patchDoubleAttrs, patchKeys);
    patchJson(writer, patchInt64Attrs, patchKeys);
    patchJson(writer, patchBoolAttrs, patchKeys);
}

} // namespace iquan
