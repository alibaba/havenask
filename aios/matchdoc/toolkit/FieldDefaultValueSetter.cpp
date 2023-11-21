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
#include "matchdoc/toolkit/FieldDefaultValueSetter.h"

#include "autil/MultiValueCreator.h"
#include "autil/StringUtil.h"
#include "matchdoc/MountInfo.h"
#include "matchdoc/ValueType.h"

namespace matchdoc {

using namespace std;
using namespace autil;

AUTIL_LOG_SETUP(matchdoc, FieldDefaultValueSetter);

bool FieldDefaultValueSetter::init(const string &defaultValue, char *mountBuffer) {
    char *data = FieldDefaultValueSetter::constructTypedBuffer(_pool, _refBase, defaultValue);
    if (nullptr == data) {
        AUTIL_LOG(ERROR,
                  "create data buffer for [%s] with default value [%s] failed.",
                  _refBase->getName().c_str(),
                  defaultValue.c_str());
        return false;
    }
    if (_refBase->isMount()) {
        uint64_t mountOffset = _refBase->getMountOffset();
        autil::PackOffset pOffset;
        pOffset.fromUInt64(mountOffset);
        assert(!pOffset.isImpactFormat());
        memcpy(mountBuffer + pOffset.getOffset(), data, _refBase->getAllocateSize());
        _dataBuffer = mountBuffer;
    } else {
        _dataBuffer = data;
    }
    return true;
}

void FieldDefaultValueSetter::setDefaultValue(matchdoc::MatchDoc doc) const {
    if (_refBase->isMount()) {
        _refBase->mount(doc, _dataBuffer);
    } else {
        setNonMountValue(doc);
    }
}

char *FieldDefaultValueSetter::constructTypedBuffer(autil::mem_pool::Pool *pool,
                                                    ReferenceBase *refBase,
                                                    const string &defaultValue) {
    char *data = nullptr;
    ValueType valueType = refBase->getValueType();
    if (valueType.isMultiValue()) {
        AUTIL_LOG(WARN, "field [%s] with multi_value type not support default value.", refBase->getName().c_str());
        return nullptr;
    }
    BuiltinType builtinType = valueType.getBuiltinType();
    switch (builtinType) {
#define CASE_MACRO(bt)                                                                                                 \
    case bt: {                                                                                                         \
        typedef BuiltinType2CppType<bt>::CppType T;                                                                    \
        T value = StringUtil::numberFromString<T>(defaultValue);                                                       \
        data = (char *)(new (pool->allocate(sizeof(T))) T(value));                                                     \
        return data;                                                                                                   \
    } break;
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
    default: {
        AUTIL_LOG(WARN,
                  "field [%s] with type [%s] not support default value.",
                  refBase->getName().c_str(),
                  builtinTypeToString(builtinType).c_str());
        return nullptr;
    } break;
    }
    return nullptr;
#undef CASE_MACRO
}

void FieldDefaultValueSetter::setNonMountValue(matchdoc::MatchDoc doc) const {
    ValueType valueType = _refBase->getValueType();
    BuiltinType builtinType = valueType.getBuiltinType();
    assert(!valueType.isMultiValue());
    switch (builtinType) {
#define CASE_FUNC(bt)                                                                                                  \
    case bt: {                                                                                                         \
        typedef BuiltinType2CppType<bt>::CppType T;                                                                    \
        Reference<T> *typedRef = static_cast<Reference<T> *>(_refBase);                                                \
        typedRef->set(doc, *(T *)(_dataBuffer));                                                                       \
    } break;
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_FUNC);
    default: {
        AUTIL_LOG(WARN,
                  "field [%s] with type [%s] not support default value.",
                  _refBase->getName().c_str(),
                  builtinTypeToString(builtinType).c_str());
    } break;
    }
}

} // namespace matchdoc
