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

#include "alog/Logger.h"
#include "autil/DataBuffer.h"
#include "autil/legacy/exception.h"
#include <algorithm>

#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;
using namespace autil::mem_pool;
using namespace autil;

namespace matchdoc {

AUTIL_LOG_SETUP(matchdoc, FieldGroup);

FieldGroup::FieldGroup(Pool *pool, ReferenceTypesWrapper *referenceTypesWrapper,
                       const string &groupName)
    : _pool(pool),
      _storage(new DocStorage(pool)),
      _referenceTypesWrapper(referenceTypesWrapper),
      _groupName(groupName),
      _needConstruct(false),
      _needDestruct(false) {}

FieldGroup::FieldGroup(Pool *pool, const string &name, ReferenceTypesWrapper *referenceTypesWrapper,
                       char *data, uint32_t docSize, uint32_t docCount, uint32_t capacity)
    : _pool(pool),
      _storage(new DocStorage(pool, data, docSize, docCount, capacity)),
      _referenceTypesWrapper(referenceTypesWrapper),
      _groupName(name),
      _needConstruct(false),
      _needDestruct(false) {}

FieldGroup::~FieldGroup() {
    for (auto ref : _referenceVec) {
        delete ref;
    }
    _referenceVec.clear();
    _referenceMap.clear();
    if (_storage) {
        delete _storage;
        _storage = NULL;
    }
}

void FieldGroup::sortReference() {
    sort(_referenceVec.begin(), _referenceVec.end(), [](ReferenceBase *l, ReferenceBase *r)
         {return l->getName() < r->getName();});
}

void FieldGroup::serializeMeta(DataBuffer &dataBuffer, uint8_t serializeLevel) const {
    ReferenceVector refVec;
    getAllSerializeElements(refVec, serializeLevel);
    dataBuffer.write(refVec.size());

    for (auto ref : refVec) {
        dataBuffer.write(ref->getReferenceMeta());
    }
}

void FieldGroup::deserializeMeta(DataBuffer &dataBuffer) {
    size_t size = 0;
    dataBuffer.read(size);
    for (size_t i = 0; i < size; i++) {
        ReferenceMeta meta;
        dataBuffer.read(meta, _pool);
        ReferenceTypes::const_iterator it;
        bool ret = _referenceTypesWrapper->find(meta.vt, it);
        if (ret) {
            ReferenceBase *ref = it->second.first->getTypedRef(NULL, _storage,
                    meta.allocateSize, _groupName, meta.valueType.needConstruct());
            ref->setSerializeLevel(meta.serializeLevel);
            ref->setName(meta.name);
            ref->setValueType(meta.valueType);
            _referenceVec.push_back(ref);
            _referenceMap[meta.name] = ref;
            if (!_needConstruct && ref->needConstructMatchDoc())
            {
                _needConstruct = true;
            }
            if (!_needDestruct && ref->needDestructMatchDoc())
            {
                _needDestruct = true;
            }
        } else {
            throw legacy::ExceptionBase("unknown type " + meta.vt);
        }
    }
}

void FieldGroup::serialize(DataBuffer &dataBuffer, const MatchDoc &doc,
                           uint8_t serializeLevel)
{
    for (auto ref : _referenceVec) {
        if (ref->getSerializeLevel() < serializeLevel) {
            continue;
        }
        ref->serialize(doc, dataBuffer);
    }
}

void FieldGroup::deserialize(autil::DataBuffer &dataBuffer, const MatchDoc &doc) {
    for (auto ref : _referenceVec) {
        ref->deserialize(doc, dataBuffer, _pool);
    }
}

void FieldGroup::setCurrentRef(Reference<MatchDoc> *current) {
    for (auto ref : _referenceVec) {
        ref->setCurrentRef(current);
    }
}

const ReferenceMap& FieldGroup::getReferenceMap() const {
    return _referenceMap;
}

ReferenceBase* FieldGroup::cloneReference(const ReferenceBase *reference,
                                          const string &name)
{
    uint32_t allocateSize = reference->getAllocateSize();
    bool needConstruct = reference->getValueType().needConstruct();
    ReferenceBase* ref = reference->getTypedRef(NULL, _storage, allocateSize,
            _groupName, needConstruct);
    ref->setSerializeLevel(reference->getSerializeLevel());
    ref->setName(name);
    ref->setValueType(reference->getValueType());
    _referenceVec.push_back(ref);
    _referenceMap[name] = ref;
    if (!_needConstruct && ref->needConstructMatchDoc())
    {
        _needConstruct = true;
    }
    if (!_needDestruct && ref->needDestructMatchDoc())
    {
        _needDestruct = true;
    }
    return ref;
}

ReferenceBase* FieldGroup::cloneMountedReference(const ReferenceBase *reference,
                                                 const std::string &name,
                                                 uint32_t mountId)
{
    assert(reference->isMount());
    auto mountIdOffset = getSharedOffset(mountId);
    if (mountIdOffset == INVALID_OFFSET) {
        mountIdOffset = _storage->getDocSize();
        _storage->incDocSize(sizeof(void *));
        setSharedOffset(mountId, mountIdOffset);
    }
    auto *ref = reference->getTypedMountRef(NULL, _storage, mountIdOffset, reference->getMountOffset(),
                                            reference->getAllocateSize(), _groupName);
    ref->setSerializeLevel(reference->getSerializeLevel());
    ref->setName(name);
    ref->setValueType(reference->getValueType());
    _referenceVec.push_back(ref);
    _referenceMap[name] = ref;
    _needConstruct = true;
    return ref;
}

void FieldGroup::getAllSerializeElements(
        ReferenceVector &vec, uint8_t serializeLevel) const {
    vec.reserve(_referenceVec.size());
    for (auto ref : _referenceVec) {
        if (ref->getSerializeLevel() < serializeLevel) {
            continue;
        }
        vec.push_back(ref);
    }
}

void FieldGroup::getAllSerializeElements(ReferenceMap& map, uint8_t serializeLevel) const {
    ReferenceMap::const_iterator iter = _referenceMap.begin();
    for (; iter != _referenceMap.end(); iter++) {
        if (iter->second->getSerializeLevel() < serializeLevel) {
            continue;
        }
        map.insert(*iter);
    }
}

void FieldGroup::getRefBySerializeLevel(
        ReferenceVector &vec, uint8_t serializeLevel) const {
    vec.reserve(_referenceVec.size());
    for (auto ref : _referenceVec) {
        if (ref->getSerializeLevel() != serializeLevel) {
            continue;
        }
        vec.push_back(ref);
    }
}

void FieldGroup::resetSerializeLevel(uint8_t level) {
    for (auto ref : _referenceVec) {
        ref->setSerializeLevel(level);
    }
}

void FieldGroup::resetSerializeLevelAndAlias(uint8_t serializeLevel) {
    for (size_t i = 0; i < _referenceVec.size(); ++i) {
        _referenceVec[i]->setSerializeLevel(serializeLevel);
        _referenceVec[i]->resetSerialAliasName();
    }
}

bool FieldGroup::needSerialize(uint8_t serializeLevel) const {
    for (auto ref : _referenceVec) {
        if (ref->getSerializeLevel() >= serializeLevel) {
            return true;
        }
    }
    return false;
}

void FieldGroup::check() const {
    auto iter = _referenceMap.begin();
    for (; iter != _referenceMap.end(); iter++) {
        ReferenceBase* ref = iter->second;
        if (ref->getName() != iter->first) {
            throw autil::legacy::ExceptionBase("ref name not match!");
        }

        if (ref->getGroupName() != _groupName) {
            throw autil::legacy::ExceptionBase("group name not match!");
        }
    }
}

bool FieldGroup::isSame(const FieldGroup &other) const {
    if (_referenceMap.size() != other._referenceMap.size()) {
        return false;
    }
    for (const auto &refPair : _referenceMap) {
        auto it = other._referenceMap.find(refPair.first);
        if (other._referenceMap.end() == it) {
            return false;
        }
        if (refPair.second->getReferenceMeta() != it->second->getReferenceMeta() ||
            refPair.second->getOffset() != it->second->getOffset() || 
            refPair.second->getMountOffset() != it->second->getMountOffset())
        {
            AUTIL_LOG(WARN, "meta for reference %s not same", refPair.first.c_str());
            return false;
        }
    }
    return true;
}

void FieldGroup::cloneDoc(FieldGroup *other,
                          const MatchDoc &newDoc,
                          const MatchDoc &cloneDoc)
{
    for (auto ref : _referenceVec) {
        const string &name = ref->getName();
        ReferenceBase *otherRef = other->_referenceMap[name];
        ref->cloneConstruct(newDoc, cloneDoc, otherRef);
    }
}

void FieldGroup::getAllReferenceNames(ReferenceNameSet &names) const {
    for (const auto &ref : _referenceVec) {
        names.insert(ref->getName());
    }
}

void FieldGroup::appendGroup(FieldGroup *other) {
    _storage->addChunk(*other->_storage);
}

void FieldGroup::truncateGroup(uint32_t size) {
    _storage->truncate(size);
}

void FieldGroup::swapDocStorage(FieldGroup &other) {
    swap(_storage, other._storage);
    for (size_t i = 0; i < _referenceVec.size(); i++) {
        _referenceVec[i]->replaceStorage(_storage);
        other._referenceVec[i]->replaceStorage(other._storage);
    }
}

}
