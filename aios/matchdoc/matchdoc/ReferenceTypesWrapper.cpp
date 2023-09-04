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
#include "matchdoc/ReferenceTypesWrapper.h"

#include <unordered_map>

#include "autil/Hyperloglog.h"
#include "autil/Lock.h"
#include "autil/LongHashValue.h"
#include "autil/MultiValueType.h"
#include "autil/StringView.h"
#include "matchdoc/MatchDoc.h"

namespace matchdoc {

struct ReferenceTypesWrapper::Impl {
    std::unordered_map<std::string, std::unique_ptr<ReferenceBase>> referenceTypes;
    mutable autil::ReadWriteLock lock;
};

ReferenceTypesWrapper::ReferenceTypesWrapper() : _impl(std::make_unique<Impl>()) { registerBuiltinTypes(); }

ReferenceTypesWrapper::~ReferenceTypesWrapper() = default;

ReferenceTypesWrapper *ReferenceTypesWrapper::getInstance() {
    static ReferenceTypesWrapper instance;
    return &instance;
}

void ReferenceTypesWrapper::registerBuiltinTypes() {
    registerType<uint8_t>();
    registerType<uint16_t>();
    registerType<uint32_t>();
    registerType<uint64_t>();
    registerType<autil::uint128_t>();

    registerType<int8_t>();
    registerType<int16_t>();
    registerType<int32_t>();
    registerType<int64_t>();
    registerType<char>();

    registerType<double>();
    registerType<float>();

    registerType<bool>();
    registerType<std::string>();
    registerType<autil::StringView>();
    registerType<MatchDoc>();
    registerType<autil::HllCtx>();

    registerType<autil::MultiInt8>();
    registerType<autil::MultiUInt8>();
    registerType<autil::MultiInt16>();
    registerType<autil::MultiUInt16>();
    registerType<autil::MultiInt32>();
    registerType<autil::MultiUInt32>();
    registerType<autil::MultiInt64>();
    registerType<autil::MultiUInt64>();
    registerType<autil::MultiFloat>();
    registerType<autil::MultiDouble>();
    registerType<autil::MultiChar>();
    registerType<autil::MultiString>();
    registerType<autil::MultiValueType<bool>>();
    registerType<autil::MultiValueType<autil::uint128_t>>();

    registerType<std::vector<int8_t>>();
    registerType<std::vector<int16_t>>();
    registerType<std::vector<int32_t>>();
    registerType<std::vector<int64_t>>();
    registerType<std::vector<uint8_t>>();
    registerType<std::vector<uint16_t>>();
    registerType<std::vector<uint32_t>>();
    registerType<std::vector<uint64_t>>();
    registerType<std::vector<autil::uint128_t>>();
    registerType<std::vector<float>>();
    registerType<std::vector<double>>();
    registerType<std::vector<bool>>();
    registerType<std::vector<char>>();
    registerType<std::vector<std::string>>();
}

void ReferenceTypesWrapper::registerTypeImpl(std::unique_ptr<ReferenceBase> refBase) {
    assert(refBase);
    autil::ScopedWriteLock lock(_impl->lock);
    if (_impl->referenceTypes.count(refBase->getVariableType()) > 0) {
        return;
    }
    _impl->referenceTypes.emplace(refBase->getVariableType(), std::move(refBase));
}

void ReferenceTypesWrapper::unregisterTypeImpl(const std::string &typeName) {
    autil::ScopedWriteLock lock(_impl->lock);
    _impl->referenceTypes.erase(typeName);
}

const ReferenceBase *ReferenceTypesWrapper::lookupType(const std::string &typeName) const {
    autil::ScopedReadLock lock(_impl->lock);
    auto it = _impl->referenceTypes.find(typeName);
    return it == _impl->referenceTypes.end() ? nullptr : it->second.get();
}

} // namespace matchdoc
