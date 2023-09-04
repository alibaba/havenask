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
#include "matchdoc/FieldGroupBuilder.h"

#include "autil/Demangle.h"
#include "autil/Log.h"
#include "matchdoc/FieldGroup.h"
#include "matchdoc/ReferenceTypesWrapper.h"

namespace matchdoc {
AUTIL_DECLARE_AND_SETUP_LOGGER(matchdoc, FieldGroupBuilder);

FieldGroupBuilder::FieldGroupBuilder(const std::string &groupName, autil::mem_pool::Pool *pool)
    : FieldGroupBuilder(groupName, autil::mem_pool::Pool::maybeShared(pool)) {}

FieldGroupBuilder::FieldGroupBuilder(const std::string &groupName, const std::shared_ptr<autil::mem_pool::Pool> &pool)
    : _pool(pool), _groupName(groupName) {
    _referenceSet = std::make_unique<ReferenceSet>();
}

FieldGroupBuilder::~FieldGroupBuilder() = default;

ReferenceBase *FieldGroupBuilder::declare(const std::string &name, const ReferenceMeta &meta) {
    ReferenceBase *ref = _referenceSet->get(name);
    if (ref != nullptr) {
        return ref;
    }

    const ReferenceBase *base = ReferenceTypesWrapper::getInstance()->lookupType(meta.vt);
    if (base == nullptr) {
        AUTIL_LOG(ERROR, "unsupported type: %s", autil::demangle(meta.vt.c_str()).c_str());
        return nullptr;
    }
    ref = base->copyWith(meta, nullptr, _docSize, INVALID_OFFSET, _groupName);
    ref->setName(name);
    _referenceSet->put(ref);
    _docSize += meta.allocateSize;
    return ref;
}

ReferenceBase *FieldGroupBuilder::declareMount(const std::string &name,
                                               const ReferenceMeta &meta,
                                               uint32_t mountId,
                                               uint64_t mountOffset) {
    ReferenceBase *ref = _referenceSet->get(name);
    if (ref != nullptr) {
        return ref;
    }

    const ReferenceBase *base = ReferenceTypesWrapper::getInstance()->lookupType(meta.vt);
    if (base == nullptr) {
        return nullptr;
    }
    uint32_t offset = _docSize;
    auto it = _mountIdOffsets.find(mountId);
    if (it != _mountIdOffsets.end()) {
        offset = it->second;
    } else {
        _mountIdOffsets.emplace(mountId, _docSize);
        _docSize += sizeof(char *);
    }
    ref = base->copyWith(meta, nullptr, offset, mountOffset, _groupName);
    ref->setName(name);
    // TODO: remove this hack
    ref->setIsMountReference();
    _referenceSet->put(ref);
    return ref;
}

std::unique_ptr<FieldGroup> FieldGroupBuilder::finalize() {
    assert(_pool != nullptr);
    auto storage = std::make_unique<VectorStorage>(_pool, _docSize);
    return FieldGroup::make(_groupName, std::move(storage), std::move(_referenceSet));
}

std::unique_ptr<FieldGroup> FieldGroupBuilder::fromBuffer(char *data, size_t docSize, size_t count) {
    if (_docSize != docSize) {
        return nullptr;
    }
    auto storage = VectorStorage::fromBuffer(data, docSize, count, _pool);
    return FieldGroup::make(_groupName, std::move(storage), std::move(_referenceSet));
}

bool FieldGroupBuilder::equals(const FieldGroupBuilder &other) const {
    return _referenceSet->equals(*other._referenceSet);
}

} // namespace matchdoc
