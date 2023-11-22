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
#include "matchdoc/FieldGroup.h"

#include <algorithm>

#include "alog/Logger.h"
#include "autil/DataBuffer.h"
#include "autil/legacy/exception.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace autil::mem_pool;
using namespace autil;

namespace matchdoc {

AUTIL_LOG_SETUP(matchdoc, FieldGroup);

FieldGroup::FieldGroup(const std::string &groupName,
                       std::shared_ptr<VectorStorage> storage,
                       std::unique_ptr<ReferenceSet> refSet)
    : _groupName(groupName)
    , _storage(std::move(storage))
    , _referenceSet(std::move(refSet))
    , _needConstruct(false)
    , _needDestruct(false) {
    init();
}

FieldGroup::~FieldGroup() = default;

void FieldGroup::resetSerializeLevel(uint8_t level) {
    _referenceSet->for_each([&level](auto r) { r->setSerializeLevel(level); });
}

void FieldGroup::resetSerializeLevelAndAlias(uint8_t serializeLevel) {
    _referenceSet->for_each([&serializeLevel](auto r) {
        r->setSerializeLevel(serializeLevel);
        r->resetSerialAliasName();
    });
}

bool FieldGroup::needSerialize(uint8_t serializeLevel) const {
    bool ret = false;
    _referenceSet->for_each([&](auto *r) { ret = ret || r->getSerializeLevel() >= serializeLevel; });
    return ret;
}

bool FieldGroup::isSame(const FieldGroup &other) const { return _referenceSet->equals(*other._referenceSet); }

void FieldGroup::cloneDoc(FieldGroup *other, const MatchDoc &newDoc, const MatchDoc &cloneDoc) {
    _referenceSet->for_each([&](auto src) {
        const auto &name = src->getName();
        auto dst = other->_referenceSet->get(name);
        assert(dst);
        src->cloneConstruct(newDoc, cloneDoc, dst);
    });
}

void FieldGroup::appendGroup(FieldGroup *other) { _storage->append(*other->_storage); }

void FieldGroup::truncateGroup(uint32_t size) { _storage->truncate(size); }

void FieldGroup::swapDocStorage(FieldGroup &other) {
    swap(_storage, other._storage);
    _referenceSet->for_each([this](auto r) { r->attachStorage(_storage.get()); });
    other._referenceSet->for_each([&](auto r) { r->attachStorage(other._storage.get()); });
}

void FieldGroup::maybeResetMount(const MatchDoc &doc) {
    for (auto mountOffset : _mountOffsets) {
        char *addr = _storage->get(doc.getIndex()) + mountOffset;
        *(char **)addr = nullptr;
    }
}

void FieldGroup::maybeResetMount(const MatchDoc *docs, size_t count) {
    for (auto mountOffset : _mountOffsets) {
        for (size_t i = 0; i < count; ++i) {
            char *addr = _storage->get(docs[i].getIndex()) + mountOffset;
            *(char **)addr = nullptr;
        }
    }
}

void FieldGroup::init() {
    std::set<uint32_t> mountOffsets;
    for (auto ref : _referenceSet->referenceVec) {
        ref->attachStorage(_storage.get());
        _needConstruct = _needConstruct || ref->needConstructMatchDoc();
        _needDestruct = _needDestruct || ref->needDestructMatchDoc();
        if (ref->isMount()) {
            mountOffsets.insert(ref->getOffset());
        }
    }
    _mountOffsets.insert(_mountOffsets.end(), mountOffsets.begin(), mountOffsets.end());
    _needConstruct = _needConstruct || !_mountOffsets.empty();
}

std::unique_ptr<FieldGroup> FieldGroup::make(const std::string &groupName,
                                             std::shared_ptr<VectorStorage> storage,
                                             std::unique_ptr<ReferenceSet> refSet) {
    return std::make_unique<FieldGroup>(groupName, std::move(storage), std::move(refSet));
}

std::unique_ptr<FieldGroup> FieldGroup::makeSingleFieldGroup(matchdoc::ReferenceBase *ref) {
    if (!ref->mutableStorage()) {
        return nullptr;
    }
    std::shared_ptr<VectorStorage> storage;
    try {
        storage = ref->mutableStorage()->shared_from_this();
    } catch (std::bad_weak_ptr &e) { return nullptr; }
    auto refSet = std::make_unique<ReferenceSet>();
    refSet->put(ref);
    return std::make_unique<FieldGroup>(ref->getName(), std::move(storage), std::move(refSet));
}

} // namespace matchdoc
