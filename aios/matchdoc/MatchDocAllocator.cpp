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
#include "matchdoc/MatchDocAllocator.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <ostream>
#include <type_traits>

#include "autil/DataBuffer.h"
#include "autil/Lock.h"
#include "autil/MurmurHash.h"
#include "autil/StringUtil.h"
#include "matchdoc/FieldGroupSerdes.h"
#include "matchdoc/SubDocAccessor.h"
#include "matchdoc/VectorStorage.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace matchdoc {

const std::string BUILD_IN_REF_GROUP = "_@_build_in_grp";
const std::string FIRST_SUB_REF = "matchdoc_buildin_subdoc_first";
const std::string CURRENT_SUB_REF = "matchdoc_buildin_subdoc_current";
const std::string NEXT_SUB_REF = "matchdoc_buildin_subdoc_next";
const std::string DEFAULT_GROUP = "default";

AUTIL_LOG_SETUP(matchdoc, MatchDocAllocator);

MatchDocAllocator::MatchDocAllocator(const std::shared_ptr<Pool> &poolPtr, bool useSub, const MountInfoPtr &mountInfo)
    : _poolPtr(poolPtr)
    , _size(0)
    , _capacity(0)
    , _metaSignature(0)
    , _mountInfo(mountInfo)
    , _subAllocator(nullptr)
    , _firstSub(nullptr)
    , _currentSub(nullptr)
    , _nextSub(nullptr)
    , _subAccessor(nullptr)
    , _defaultGroupNameCounter(0)
    , _merged(false)
    , _mountInfoIsPrivate(false)
    , _sortRefFlag(true) {
    if (useSub) {
        initSub();
    }
}

MatchDocAllocator::MatchDocAllocator(autil::mem_pool::Pool *pool, bool useSub, const MountInfoPtr &mountInfo)
    : MatchDocAllocator(autil::mem_pool::Pool::maybeShared(pool), useSub, mountInfo) {}

MatchDocAllocator::~MatchDocAllocator() {
    if (!_merged) {
        deallocateAllMatchDocs();
    }
    clearFieldGroups();

    DELETE_AND_SET_NULL(_subAllocator);
    DELETE_AND_SET_NULL(_subAccessor);
}

bool MatchDocAllocator::mergeMountInfo(const MountInfoPtr &mountInfo) {
    if (nullptr == mountInfo) {
        return true;
    }
    uint32_t nextMountId = 0;
    autil::ScopedSpinLock lock(_lock);
    MountInfoPtr newMountInfo;
    if (nullptr != _mountInfo) {
        for (const auto &name2MountMeta : _mountInfo->getMountMap()) {
            nextMountId = std::max(name2MountMeta.second.mountId, nextMountId);
        }
        newMountInfo.reset(new MountInfo(*_mountInfo.get()));
    } else {
        newMountInfo.reset(new MountInfo());
    }
    ++nextMountId;
    for (const auto &name2MountMeta : mountInfo->getMountMap()) {
        const string &fieldName = name2MountMeta.first;
        if (_aliasMap && _aliasMap->find(fieldName) != _aliasMap->end()) {
            continue;
        }
        if (findReferenceByName(fieldName) != nullptr) {
            continue;
        }
        const MountMeta &meta = name2MountMeta.second;
        const auto existedMeta = newMountInfo->get(fieldName);
        if (existedMeta) {
            if (existedMeta->mountOffset == meta.mountOffset) {
                continue;
            } else {
                AUTIL_LOG(WARN,
                          "insert mount field [%s] failed. mount offset [%lu] vs [%lu]",
                          fieldName.c_str(),
                          meta.mountOffset,
                          existedMeta->mountOffset);
                return false;
            }
        } else {
            newMountInfo->insert(fieldName, meta.mountId + nextMountId, meta.mountOffset);
        }
    }
    _mountInfo = newMountInfo;
    _mountInfoIsPrivate = true;
    return true;
}

bool MatchDocAllocator::mergeMountInfo(const MountInfoPtr &mountInfo, const std::set<std::string> &mergeFields) {
    if (mountInfo == nullptr) {
        return true;
    }
    MountInfoPtr newMountInfo;
    if (mergeFields.empty()) {
        newMountInfo = mountInfo;
    } else {
        newMountInfo.reset(new MountInfo());
        for (const auto &field : mergeFields) {
            const MountMeta *meta = mountInfo->get(field);
            if (meta == nullptr) {
                continue;
            }
            if (!newMountInfo->insert(field, meta->mountId, meta->mountOffset)) {
                AUTIL_LOG(ERROR, "insert mount meta failed [%s]", field.c_str());
                return false;
            }
        }
    }
    return mergeMountInfo(newMountInfo);
}

bool MatchDocAllocator::mergeMountInfo(const MatchDocAllocator *allocator) {
    if (unlikely(allocator == nullptr)) {
        return true;
    }
    return mergeMountInfo(allocator->getMountInfo());
}

bool MatchDocAllocator::mergeMountInfo(const MatchDocAllocator *allocator, const std::set<std::string> &mergeFields) {
    if (unlikely(allocator == nullptr)) {
        return true;
    }
    return mergeMountInfo(allocator->getMountInfo(), mergeFields);
}

void MatchDocAllocator::clearFieldGroups() {
    _referenceMap.clear();
    for (auto &it : _fieldGroups) {
        delete it.second;
    }
    _fieldGroups.clear();
    _fieldGroupBuilders.clear();
}

uint32_t MatchDocAllocator::getAllocatedCount() const {
    autil::ScopedSpinLock lock(_lock);
    return _size;
}

uint32_t MatchDocAllocator::getCapacity() const {
    autil::ScopedSpinLock lock(_lock);
    return _capacity;
}

const std::vector<uint32_t> &MatchDocAllocator::getDeletedIds() const {
    autil::ScopedSpinLock lock(_lock);
    return _deletedIds;
}

MatchDoc MatchDocAllocator::getOne() {
    uint32_t id;
    if (!_deletedIds.empty()) {
        id = _deletedIds.back();
        _deletedIds.pop_back();
    } else {
        if (_capacity <= _size) {
            grow();
        }
        id = _size++;
    }
    MatchDoc doc(id);
    return doc;
}

void MatchDocAllocator::clean(const unordered_set<string> &keepFieldGroups) {
    deallocateAllMatchDocs();
    extend();
    FieldGroups::iterator it = _fieldGroups.begin();
    for (; it != _fieldGroups.end();) {
        if (keepFieldGroups.find(it->first) == keepFieldGroups.end()) {
            const auto &refMap = it->second->getReferenceMap();
            for (const auto &ref : refMap) {
                auto it2 = _referenceMap.find(ref.first);
                if (_referenceMap.end() != it2) {
                    _referenceMap.erase(it2);
                }
            }
            delete it->second;
            _fieldGroups.erase(it++);
        } else {
            it->second->clearContent();
            it->second->resetSerializeLevelAndAlias();
            it++;
        }
    }
    if (_subAllocator) {
        _subAllocator->clean(keepFieldGroups);
        _firstSub = declare<MatchDoc>(FIRST_SUB_REF, DEFAULT_GROUP);
        _currentSub = declare<MatchDoc>(CURRENT_SUB_REF, DEFAULT_GROUP);
        for (const auto &entry : _subAllocator->getFastReferenceMap()) {
            entry.second->setCurrentRef(_currentSub);
        }
        _nextSub = _subAllocator->declare<MatchDoc>(NEXT_SUB_REF, DEFAULT_GROUP);
        _nextSub->setCurrentRef(nullptr);
        DELETE_AND_SET_NULL(_subAccessor);
        _subAccessor = new SubDocAccessor(_subAllocator, _firstSub, _currentSub, _nextSub);
    }
    _deletedIds.clear();
    _size = 0;
    _capacity = 0;
    _merged = false;
    _aliasMap.reset();
    _defaultGroupNameCounter = 0;
}

void MatchDocAllocator::truncateSubDoc(MatchDoc parent, MatchDoc subdoc) const {
    auto &current = _currentSub->getReference(parent);
    current = subdoc;
    if (matchdoc::INVALID_MATCHDOC == subdoc) {
        auto &first = _firstSub->getReference(parent);
        first = subdoc;
    } else {
        _nextSub->set(subdoc, matchdoc::INVALID_MATCHDOC);
    }
}

void MatchDocAllocator::deallocateSubDoc(MatchDoc subdoc) const {
    assert(_subAllocator);
    _subAllocator->deallocate(subdoc);
}

MatchDoc MatchDocAllocator::allocate(int32_t docid) {
    if (!_fieldGroupBuilders.empty()) {
        extend();
    }
    MatchDoc doc = getOne();
    for (auto &it : _fieldGroups) {
        it.second->constructDoc(doc);
    }
    doc.setDocId(docid);
    return doc;
}

class ConstDocIdGenerator {
public:
    ConstDocIdGenerator(size_t count, int32_t id) : _count(count), _id(id) {}
    ~ConstDocIdGenerator() {}

    size_t size() const { return _count; }
    int32_t element(size_t i) { return _id; }

private:
    size_t _count;
    int32_t _id;
};

std::vector<matchdoc::MatchDoc> MatchDocAllocator::batchAllocate(size_t count, int32_t docid, bool adviseNotConstruct) {
    std::vector<matchdoc::MatchDoc> retVec;
    batchAllocate(count, docid, retVec, adviseNotConstruct);
    return retVec;
}

void MatchDocAllocator::batchAllocate(std::vector<matchdoc::MatchDoc> &matchDocs,
                                      size_t count,
                                      int32_t docid,
                                      bool adviseNotConstruct) {
    ConstDocIdGenerator generator(count, docid);
    return innerBatchAllocate<ConstDocIdGenerator>(generator, matchDocs, adviseNotConstruct);
}

void MatchDocAllocator::batchAllocate(size_t count,
                                      int32_t docid,
                                      std::vector<matchdoc::MatchDoc> &retVec,
                                      bool adviseNotConstruct) {
    ConstDocIdGenerator generator(count, docid);
    return innerBatchAllocate<ConstDocIdGenerator>(generator, retVec, adviseNotConstruct);
}

class VectorDocIdGenerator {
public:
    VectorDocIdGenerator(const std::vector<int32_t> &docIds) : _docIds(docIds) {}
    ~VectorDocIdGenerator() {}

    size_t size() const { return _docIds.size(); }
    int32_t element(size_t i) { return _docIds[i]; }

private:
    const std::vector<int32_t> &_docIds;
};

std::vector<matchdoc::MatchDoc> MatchDocAllocator::batchAllocate(const std::vector<int32_t> &docIds,
                                                                 bool adviseNotConstruct) {
    std::vector<matchdoc::MatchDoc> retVec;
    batchAllocate(docIds, retVec, adviseNotConstruct);
    return retVec;
}

void MatchDocAllocator::batchAllocate(const std::vector<int32_t> &docIds,
                                      std::vector<matchdoc::MatchDoc> &retVec,
                                      bool adviseNotConstruct) {
    VectorDocIdGenerator generator(docIds);
    return innerBatchAllocate<VectorDocIdGenerator>(generator, retVec, adviseNotConstruct);
}

matchdoc::MatchDoc
MatchDocAllocator::batchAllocateSubdoc(size_t count, std::vector<matchdoc::MatchDoc> &subdocVec, int32_t docid) {
    MatchDoc doc = allocate(docid);
    vector<int32_t> docIds(count, -1);
    if (_subAllocator) {
        _subAllocator->batchAllocate(docIds, subdocVec);
    }
    return doc;
}

MatchDoc MatchDocAllocator::allocateAndClone(const MatchDoc &cloneDoc) {
    if (!_fieldGroupBuilders.empty()) {
        extend();
    }
    MatchDoc newDoc = getOne();
    for (auto &it : _fieldGroups) {
        it.second->cloneDoc(newDoc, cloneDoc);
    }
    newDoc.setDocId(cloneDoc.getDocId());
    return newDoc;
}

MatchDoc MatchDocAllocator::allocateAndCloneWithSub(const MatchDoc &cloneDoc) {
    MatchDoc newDoc = allocateAndClone(cloneDoc);
    if (hasSubDocAllocator()) {
        _firstSub->set(newDoc, INVALID_MATCHDOC);
        _currentSub->set(newDoc, INVALID_MATCHDOC);
        SubCloner subCloner(_currentSub, this, _subAllocator, newDoc);
        _subAccessor->foreach (cloneDoc, subCloner);
        auto firstNewSubDoc = _firstSub->get(newDoc);
        _currentSub->set(newDoc, firstNewSubDoc);
    }
    return newDoc;
}

MatchDoc MatchDocAllocator::allocateAndClone(MatchDocAllocator *other, const MatchDoc &matchDoc) {
    if (!_fieldGroupBuilders.empty()) {
        extend();
    }
    MatchDoc retDoc = getOne();
    for (auto it = _fieldGroups.begin(); it != _fieldGroups.end(); it++) {
        FieldGroup *srcGroup = (other->_fieldGroups)[it->first];
        it->second->cloneDoc(srcGroup, retDoc, matchDoc);
    }
    return retDoc;
}

MatchDoc MatchDocAllocator::allocateSub(const MatchDoc &parent, int32_t docid) {
    MatchDoc doc = _subAllocator->allocate(docid);
    addSub(parent, doc);
    return doc;
}

MatchDoc &MatchDocAllocator::allocateSubReturnRef(const MatchDoc &parent, int32_t docid) {
    MatchDoc doc = _subAllocator->allocate(docid);
    return addSub(parent, doc);
}

MatchDoc &MatchDocAllocator::addSub(const MatchDoc &parent, const MatchDoc &doc) {
    MatchDoc &first = _firstSub->getReference(parent);
    MatchDoc &current = _currentSub->getReference(parent);
    MatchDoc *ret = nullptr;
    if (first == INVALID_MATCHDOC) {
        first = doc;
        ret = &first;
    } else {
        auto &nextSub = _nextSub->getReference(current);
        nextSub = doc;
        ret = &nextSub;
    }
    current = doc;
    return *ret;
}

void MatchDocAllocator::dropField(const std::string &name, bool dropMountMetaIfExist) {
    const std::string &refName = getReferenceName(name);
    autil::ScopedSpinLock lock(_lock);
    auto ref = findMainReference(refName);
    if (ref) {
        _referenceMap.erase(refName);
        deallocateAllMatchDocs(ref);
        const auto &groupName = ref->getGroupName();
        auto bit = _fieldGroupBuilders.find(groupName);
        if (bit != _fieldGroupBuilders.end()) {
            auto &builder = bit->second;
            if (builder->dropField(name)) {
                _fieldGroupBuilders.erase(bit);
            }
        } else {
            auto iter = _fieldGroups.find(groupName);
            if (iter != _fieldGroups.end()) {
                auto fieldGroup = iter->second;
                if (fieldGroup->dropField(refName)) {
                    delete fieldGroup;
                    _fieldGroups.erase(iter);
                }
            }
        }
    } else {
        if (_subAllocator) {
            _subAllocator->dropField(name);
        }
    }
    if (dropMountMetaIfExist && _mountInfo && _mountInfo->get(refName) != nullptr) {
        if (!_mountInfoIsPrivate) {
            _mountInfo.reset(new MountInfo(*_mountInfo.get()));
            _mountInfoIsPrivate = true;
        }
        _mountInfo->erase(refName);
    }
}

void MatchDocAllocator::reserveFields(uint8_t serializeLevel, const std::unordered_set<std::string> &extraFields) {
    ReferenceNameSet needReserveFieldSet;
    for (auto *ref : getAllNeedSerializeReferences(serializeLevel)) {
        needReserveFieldSet.insert(ref->getName());
    }
    for (auto *ref : getAllSubNeedSerializeReferences(serializeLevel)) {
        needReserveFieldSet.insert(ref->getName());
    }
    if (!extraFields.empty()) {
        needReserveFieldSet.insert(extraFields.begin(), extraFields.end());
    }
    ReferenceNameSet allFields = getAllReferenceNames();
    if (_subAllocator != nullptr) {
        allFields.erase(FIRST_SUB_REF);
        allFields.erase(CURRENT_SUB_REF);
        allFields.erase(NEXT_SUB_REF);
    }
    for (const auto &field : allFields) {
        if (needReserveFieldSet.find(field) == needReserveFieldSet.end()) {
            dropField(field);
        }
    }
}

bool MatchDocAllocator::renameField(const std::string &name, const std::string &dstName) {
    autil::ScopedSpinLock lock(_lock);
    if (findReferenceByName(getReferenceName(name)) == nullptr) {
        return false;
    }
    if (dstName == name) {
        AUTIL_LOG(WARN, "skip renaming reference [%s] to [%s].", name.c_str(), dstName.c_str());
        return true;
    }
    if (findReferenceByName(getReferenceName(dstName)) != nullptr) {
        AUTIL_LOG(WARN,
                  "skip renaming reference [%s] to [%s], since [%s] already exists.",
                  name.c_str(),
                  dstName.c_str(),
                  dstName.c_str());
        return false;
    }
    renameFieldImpl(name, dstName);
    return true;
}

bool MatchDocAllocator::renameFieldImpl(const std::string &name, const std::string &dstName) {
    const std::string &refName = getReferenceName(name);
    auto ref = findMainReference(refName);
    if (ref) {
        _referenceMap.erase(refName);
        _referenceMap.emplace(dstName, ref);
        const auto &groupName = ref->getGroupName();
        auto bit = _fieldGroupBuilders.find(groupName);
        if (bit != _fieldGroupBuilders.end()) {
            bit->second->renameField(refName, dstName);
        } else {
            auto iter = _fieldGroups.find(groupName);
            if (iter != _fieldGroups.end()) {
                auto fieldGroup = iter->second;
                fieldGroup->renameField(refName, dstName);
            }
        }
    } else {
        assert(_subAllocator != nullptr);
        _subAllocator->renameFieldImpl(name, dstName);
    }
    if (_mountInfo && _mountInfo->get(refName) != nullptr) {
        if (!_mountInfoIsPrivate) {
            _mountInfo.reset(new MountInfo(*_mountInfo.get()));
            _mountInfoIsPrivate = true;
        }
        {
            const MountMeta *mountMeta = _mountInfo->get(refName);
            assert(mountMeta != nullptr);
            _mountInfo->insert(dstName, mountMeta->mountId, mountMeta->mountOffset);
        }
        _mountInfo->erase(refName);
    }
    return true;
}

void MatchDocAllocator::deallocateAllMatchDocs(ReferenceBase *ref) {
    if (ref && !ref->needDestructMatchDoc()) {
        return;
    }
    if (_deletedIds.size() == _size) {
        return;
    }
    sort(_deletedIds.begin(), _deletedIds.end());
    _deletedIds.push_back(_size);
    for (uint32_t id = 0, k = 0; id < _size; ++id) {
        if (id != _deletedIds[k]) {
            MatchDoc doc(id);
            if (unlikely((bool)ref)) {
                ref->destruct(doc);
            } else {
                _deallocate(doc);
            }
        } else {
            k++;
        }
    }
    _deletedIds.pop_back();
}

void MatchDocAllocator::_deallocate(const MatchDoc &doc) {
    if (_subAllocator) {
        SubDeleter deleter(_currentSub, _subAllocator);
        _subAccessor->foreach (doc, deleter);
    }
    FieldGroups::iterator it = _fieldGroups.begin();
    FieldGroups::iterator itEnd = _fieldGroups.end();
    for (; it != itEnd; ++it) {
        it->second->destructDoc(doc);
    }
}

void MatchDocAllocator::_deallocate(const MatchDoc *docs, uint32_t count) {
    if (_subAllocator) {
        SubDeleter deleter(_currentSub, _subAllocator);
        for (uint32_t i = 0; i < count; i++) {
            _subAccessor->foreach (docs[i], deleter);
        }
    }
    FieldGroups::iterator it = _fieldGroups.begin();
    FieldGroups::iterator itEnd = _fieldGroups.end();
    for (; it != itEnd; ++it) {
        it->second->destructDoc(docs, count);
    }
}

void MatchDocAllocator::deallocate(const MatchDoc &doc) {
    _deallocate(doc);
    _deletedIds.push_back(doc.getIndex());
}

void MatchDocAllocator::deallocate(const MatchDoc *docs, uint32_t count) {
    _deallocate(docs, count);
    for (uint32_t i = 0; i < count; i++) {
        _deletedIds.push_back(docs[i].getIndex());
    }
}

void MatchDocAllocator::grow() {
    _capacity += VectorStorage::CHUNKSIZE;
    for (const auto &pair : _fieldGroups) {
        pair.second->growToSize(_capacity);
    }
}

void MatchDocAllocator::extendGroup(FieldGroup *fieldGroup, bool needConstruct) {
    fieldGroup->growToSize(_capacity);
    if (needConstruct && fieldGroup->needConstruct()) {
        for (uint32_t i = 0; i < _size; i++) {
            fieldGroup->constructDoc(MatchDoc(i));
        }
    }
    if (fieldGroup->needDestruct()) {
        for (size_t i = 0; i < _deletedIds.size(); i++) {
            fieldGroup->destructDoc(MatchDoc(_deletedIds[i]));
        }
    }
}

void MatchDocAllocator::extend() {
    autil::ScopedSpinLock lock(_lock);
    if (_fieldGroupBuilders.empty()) {
        return;
    }
    for (auto &it : _fieldGroupBuilders) {
        auto &builder = it.second;
        auto group = builder->finalize();
        addGroupLocked(std::move(group), false, true);
    }
    _fieldGroupBuilders.clear();
}

void MatchDocAllocator::extendSub() {
    if (_subAllocator) {
        _subAllocator->extend();
    }
}

void MatchDocAllocator::extendGroup(const std::string &groupName) {
    autil::ScopedSpinLock lock(_lock);
    auto it = _fieldGroupBuilders.find(groupName);
    if (it == _fieldGroupBuilders.end()) {
        return;
    }
    auto group = it->second->finalize();
    extendGroup(group.get());
    _fieldGroups[it->first] = group.release();
    _fieldGroupBuilders.erase(it->first);
}

void MatchDocAllocator::extendSubGroup(const std::string &group) { _subAllocator->extendGroup(group); }

ReferenceBase *MatchDocAllocator::findMainReference(const std::string &name) const {
    auto it = _referenceMap.find(name);
    if (_referenceMap.end() == it) {
        return nullptr;
    }
    return it->second;
}

ReferenceBase *MatchDocAllocator::findSubReference(const std::string &name) const {
    if (_subAllocator) {
        return _subAllocator->findReferenceWithoutType(name);
    }
    return nullptr;
}

ReferenceBase *MatchDocAllocator::findReferenceByName(const std::string &name) const {
    ReferenceBase *mainRef = findMainReference(name);
    if (mainRef) {
        return mainRef;
    }
    if (unlikely(_subAllocator != nullptr)) {
        mainRef = findSubReference(name);
    }
    return mainRef;
}

void MatchDocAllocator::serializeMeta(DataBuffer &dataBuffer, uint8_t serializeLevel, bool ignoreUselessGroupMeta) {
    extend(); // extend group for zero matchdoc
    // make sure FieldGroup is ordered, serialize use this to cal signature
    // 1. serialize main
    size_t groupCount = _fieldGroups.size();
    if (ignoreUselessGroupMeta) {
        std::for_each(_fieldGroups.begin(), _fieldGroups.end(), [&](const auto &it) {
            if (!it.second->needSerialize(serializeLevel)) {
                --groupCount;
            }
        });
    }
    dataBuffer.write(groupCount);
    std::for_each(_fieldGroups.begin(), _fieldGroups.end(), [&](auto &it) mutable {
        bool needSerialize = it.second->needSerialize(serializeLevel);
        if (ignoreUselessGroupMeta && !needSerialize) {
            return;
        }
        dataBuffer.write(needSerialize);
        if (needSerialize) {
            FieldGroupSerdes::serializeMeta(it.second, dataBuffer, serializeLevel, _sortRefFlag);
        }
    });

    // 2. serialize sub
    if (_subAllocator) {
        dataBuffer.write(true);
        _subAllocator->serializeMeta(dataBuffer, serializeLevel, ignoreUselessGroupMeta);
    } else {
        dataBuffer.write(false);
    }
}

void MatchDocAllocator::serializeMatchDoc(DataBuffer &dataBuffer,
                                          const MatchDoc &matchdoc,
                                          int32_t docid,
                                          uint8_t serializeLevel) {
    // 1. serialize main
    dataBuffer.write(docid); // store docid
    std::for_each(_fieldGroups.begin(), _fieldGroups.end(), [&](const auto &it) {
        FieldGroupSerdes::serializeData(it.second, dataBuffer, matchdoc, serializeLevel);
    });

    // 2. deserialize sub
    if (_subAllocator) {
        SubCounter counter;
        _subAccessor->foreach (matchdoc, counter);
        dataBuffer.write(uint32_t(counter.get()));

        assert(_currentSub);
        MatchDoc &current = _currentSub->getReference(matchdoc);
        Reference<MatchDoc> *nextSubRef = _subAllocator->findReference<MatchDoc>(NEXT_SUB_REF);
        assert(nextSubRef);

        MatchDoc originCurrent = current;
        MatchDoc next = _firstSub->get(matchdoc);
        while (next != INVALID_MATCHDOC) {
            current = next;
            next = nextSubRef->get(current);
            _subAllocator->serializeMatchDoc(dataBuffer, matchdoc, current.getDocId(), serializeLevel);
        }
        current = originCurrent;
    }
}

void MatchDocAllocator::serialize(DataBuffer &dataBuffer,
                                  const vector<MatchDoc> &matchdocs,
                                  uint8_t serializeLevel,
                                  bool ignoreUselessGroupMeta) {
    serializeImpl(dataBuffer, matchdocs, serializeLevel, ignoreUselessGroupMeta);
}

void MatchDocAllocator::serialize(DataBuffer &dataBuffer,
                                  const autil::mem_pool::PoolVector<MatchDoc> &matchdocs,
                                  uint8_t serializeLevel,
                                  bool ignoreUselessGroupMeta) {
    serializeImpl(dataBuffer, matchdocs, serializeLevel, ignoreUselessGroupMeta);
}

void MatchDocAllocator::deserializeMeta(DataBuffer &dataBuffer, Reference<MatchDoc> *currentSub) {
    // 1. serialize main
    size_t fieldGroupSize = 0;
    dataBuffer.read(fieldGroupSize);
    clearFieldGroups();
    for (size_t i = 0; i < fieldGroupSize; i++) {
        bool grpNeedSerialize = false;
        dataBuffer.read(grpNeedSerialize);
        if (!grpNeedSerialize) {
            continue;
        }

        auto fieldGroup = FieldGroupSerdes::deserializeMeta(getSessionPool(), dataBuffer);
        assert(fieldGroup);
        if (currentSub != nullptr) {
            const auto &refSet = fieldGroup->getReferenceSet();
            refSet.for_each([&](auto ref) { ref->setCurrentRef(currentSub); });
        }
        addGroupLocked(std::move(fieldGroup), true, true);
    }

    // 2. deserialize sub
    bool useSub = false;
    dataBuffer.read(useSub);
    if (useSub) {
        DELETE_AND_SET_NULL(_subAccessor);
        DELETE_AND_SET_NULL(_subAllocator);
        _subAllocator = new MatchDocAllocator(_poolPtr);
        _firstSub = declare<MatchDoc>(FIRST_SUB_REF, BUILD_IN_REF_GROUP);
        _currentSub = declare<MatchDoc>(CURRENT_SUB_REF, BUILD_IN_REF_GROUP);

        _subAllocator->deserializeMeta(dataBuffer, this->_currentSub);
        _nextSub = _subAllocator->declare<MatchDoc>(NEXT_SUB_REF, BUILD_IN_REF_GROUP);
        _nextSub->setCurrentRef(nullptr);

        _subAccessor = new SubDocAccessor(_subAllocator, _firstSub, _currentSub, _nextSub);
    }
}

void MatchDocAllocator::deserializeOneMatchDoc(DataBuffer &dataBuffer, const MatchDoc &doc) {
    std::for_each(_fieldGroups.begin(), _fieldGroups.end(), [&](auto &it) mutable {
        FieldGroupSerdes::deserializeData(it.second, dataBuffer, doc);
    });
}

MatchDoc MatchDocAllocator::deserializeMatchDoc(DataBuffer &dataBuffer) {
    // 1. deserialize main
    int32_t docid = -1;
    dataBuffer.read(docid);

    MatchDoc doc = allocate(docid);
    deserializeOneMatchDoc(dataBuffer, doc);

    // 2. deserialize sub
    if (_subAllocator) {
        _firstSub->set(doc, INVALID_MATCHDOC);
        uint32_t subcount = 0;
        dataBuffer.read(subcount);
        for (uint32_t i = 0; i < subcount; i++) {
            int32_t subDocid = -1;
            dataBuffer.read(subDocid);
            MatchDoc childDoc = allocateSub(doc, subDocid);
            _subAllocator->deserializeOneMatchDoc(dataBuffer, doc);
            _nextSub->set(childDoc, INVALID_MATCHDOC);
        }
    }

    return doc;
}

void MatchDocAllocator::deserialize(DataBuffer &dataBuffer, vector<MatchDoc> &matchdocs) {
    deserializeImpl(dataBuffer, matchdocs);
}

void MatchDocAllocator::deserialize(DataBuffer &dataBuffer, autil::mem_pool::PoolVector<MatchDoc> &matchdocs) {
    deserializeImpl(dataBuffer, matchdocs);
}

bool MatchDocAllocator::deserializeAndAppend(DataBuffer &dataBuffer, vector<MatchDoc> &matchdocs) {
    return deserializeAndAppendImpl(dataBuffer, matchdocs);
}

bool MatchDocAllocator::deserializeAndAppend(DataBuffer &dataBuffer, autil::mem_pool::PoolVector<MatchDoc> &matchdocs) {
    return deserializeAndAppendImpl(dataBuffer, matchdocs);
}

// allocator other will be destroyed, any valid doc
// belonging to it has to be in matchDocs
bool MatchDocAllocator::mergeAllocator(MatchDocAllocator *other,
                                       const std::vector<MatchDoc> &matchDocs,
                                       std::vector<MatchDoc> &newMatchDocs) {
    if (other->_merged) {
        return false;
    }
    extend();
    other->extend();
    if (other == this || matchDocs.empty()) {
        newMatchDocs.insert(newMatchDocs.end(), matchDocs.begin(), matchDocs.end());
        return true;
    }
    if (!canMerge(other)) {
        return false;
    }
    auto beginIndex = newMatchDocs.size();
    std::vector<MatchDoc> subMatchDocs;
    if (_subAllocator) {
        for (auto doc : matchDocs) {
            other->_subAccessor->getSubDocs(doc, subMatchDocs);
        }
    }
    mergeDocs(other, matchDocs, newMatchDocs);
    if (_subAllocator) {
        std::vector<MatchDoc> subNewMatchDocs;
        _subAllocator->mergeAllocator(other->_subAllocator, subMatchDocs, subNewMatchDocs);
        size_t beginPos = 0;
        for (auto i = beginIndex; i < newMatchDocs.size(); i++) {
            _subAccessor->setSubDocs(newMatchDocs[i], subNewMatchDocs, beginPos);
        }
    }
    other->_merged = true;
    return true;
}

bool MatchDocAllocator::mergeAllocatorOnlySame(MatchDocAllocator *other,
                                               const std::vector<MatchDoc> &matchDocs,
                                               std::vector<MatchDoc> &newMatchDocs) {
    if (other->_merged) {
        return false;
    }
    extend();
    other->extend();
    if (other == this || matchDocs.empty()) {
        newMatchDocs.insert(newMatchDocs.end(), matchDocs.begin(), matchDocs.end());
        return true;
    }
    if (!isSame(*other)) {
        return false;
    }
    auto beginIndex = newMatchDocs.size();
    std::vector<MatchDoc> subMatchDocs;
    if (_subAllocator) {
        for (auto doc : matchDocs) {
            other->_subAccessor->getSubDocs(doc, subMatchDocs);
        }
    }
    appendDocs(other, matchDocs, newMatchDocs);
    if (_subAllocator) {
        std::vector<MatchDoc> subNewMatchDocs;
        _subAllocator->mergeAllocatorOnlySame(other->_subAllocator, subMatchDocs, subNewMatchDocs);
        size_t beginPos = 0;
        for (auto i = beginIndex; i < newMatchDocs.size(); i++) {
            _subAccessor->setSubDocs(newMatchDocs[i], subNewMatchDocs, beginPos);
        }
    }
    other->_merged = true;
    return true;
}

bool MatchDocAllocator::canMerge(MatchDocAllocator *other) const {
    if (!other) {
        return false;
    }
    if (_subAllocator && !_subAllocator->canMerge(other->_subAllocator)) {
        return false;
    }
    for (const auto &refPair : _referenceMap) {
        const auto &refName = refPair.first;
        auto ref = refPair.second;
        if (!ref->supportClone()) {
            return false;
        }
        auto otherRef = other->findMainReference(refName);
        if (!otherRef || ref->getReferenceMeta() != otherRef->getReferenceMeta()) {
            if (otherRef) {
                AUTIL_LOG(DEBUG,
                          "ref not equal: [%s] -> [%s]",
                          ref->toDebugString().c_str(),
                          otherRef->toDebugString().c_str());
            } else {
                AUTIL_LOG(DEBUG, "ref not equal: name[%s] -> [%s]", refName.c_str(), ref->toDebugString().c_str());
            }
            return false;
        }
    }
    return true;
}

void MatchDocAllocator::mergeDocs(MatchDocAllocator *other,
                                  const std::vector<MatchDoc> &matchDocs,
                                  std::vector<MatchDoc> &newMatchDocs) {
    std::vector<std::pair<FieldGroup *, FieldGroup *>> appendGroups;
    std::vector<FieldGroup *> copyGroups;
    auto otherFieldGroups = other->_fieldGroups;
    for (const auto &groupPair : _fieldGroups) {
        auto group = groupPair.second;
        const auto &refVec = group->getReferenceVec();
        assert(!refVec.empty());
        auto otherGroup = getReferenceGroup(other, refVec[0]->getName());
        assert(otherGroup);
        if (group->isSame(*otherGroup)) {
            appendGroups.emplace_back(group, otherGroup);
            otherFieldGroups.erase(otherGroup->getGroupName());
        } else {
            copyGroups.push_back(group);
        }
    }
    newMatchDocs.reserve(newMatchDocs.size() + matchDocs.size());
    if (!appendGroups.empty()) {
        for (auto index : other->_deletedIds) {
            _deletedIds.push_back(index + _capacity);
        }
        for (auto index = _capacity; index > _size;) {
            _deletedIds.push_back(--index);
        }
        for (const auto &groupPair : appendGroups) {
            groupPair.first->appendGroup(groupPair.second);
        }
        auto newCapacity = _capacity + other->_capacity;
        for (auto copyGroup : copyGroups) {
            copyGroup->growToSize(newCapacity);
        }
        copyGroupDocFields(matchDocs, newMatchDocs, copyGroups, other, false);
        _size = _capacity + other->_size;
        _capacity = newCapacity;
    } else {
        copyGroupDocFields(matchDocs, newMatchDocs, copyGroups, other, true);
    }
    for (auto doc : matchDocs) {
        for (const auto &group : otherFieldGroups) {
            group.second->destructDoc(doc);
        }
    }
}

void MatchDocAllocator::appendDocs(MatchDocAllocator *other,
                                   const std::vector<MatchDoc> &matchDocs,
                                   std::vector<MatchDoc> &newMatchDocs) {
    std::vector<std::pair<FieldGroup *, FieldGroup *>> appendGroups;
    auto &otherFieldGroups = other->_fieldGroups;
    for (const auto &groupPair : _fieldGroups) {
        const string &groupName = groupPair.first;
        auto group = groupPair.second;
        auto otherGroup = otherFieldGroups[groupName];
        assert(otherGroup);
        appendGroups.emplace_back(group, otherGroup);
    }
    newMatchDocs.reserve(newMatchDocs.size() + matchDocs.size());
    if (!appendGroups.empty()) {
        for (auto index : other->_deletedIds) {
            _deletedIds.push_back(index + _capacity);
        }
        for (auto index = _capacity; index > _size;) {
            _deletedIds.push_back(--index);
        }
        for (const auto &groupPair : appendGroups) {
            groupPair.first->appendGroup(groupPair.second);
        }
    }
    for (auto doc : matchDocs) {
        newMatchDocs.emplace_back(_capacity + doc.getIndex(), doc.getDocId());
    }
    auto newCapacity = _capacity + other->_capacity;
    _size = _capacity + other->_size;
    _capacity = newCapacity;
}

bool MatchDocAllocator::copyGroupDocFields(const vector<MatchDoc> &matchDocs,
                                           vector<MatchDoc> &newMatchDocs,
                                           const vector<FieldGroup *> &copyGroups,
                                           MatchDocAllocator *other,
                                           bool genNewDoc) {
    for (auto doc : matchDocs) {
        MatchDoc newDoc;
        if (genNewDoc) {
            newDoc = getOne();
            newDoc.setDocId(doc.getDocId());
        } else {
            newDoc = MatchDoc(_capacity + doc.getIndex(), doc.getDocId());
        }
        for (auto copyGroup : copyGroups) {
            for (auto ref : copyGroup->getReferenceVec()) {
                auto otherRef = other->findMainReference(ref->getName());
                assert(otherRef);
                ref->cloneConstruct(newDoc, doc, otherRef);
            }
        }
        newMatchDocs.push_back(newDoc);
    }
    return true;
}

FieldGroup *MatchDocAllocator::getReferenceGroup(MatchDocAllocator *allocator, const std::string &refName) {
    auto newRef = allocator->findMainReference(refName);
    assert(newRef);
    const auto &newGroupName = newRef->getGroupName();
    const auto &fieldGroups = allocator->_fieldGroups;
    auto it = fieldGroups.find(newGroupName);
    assert(it != fieldGroups.end());
    return it->second;
}

uint32_t MatchDocAllocator::getDocSize() const {
    uint32_t docSize = 0;
    FieldGroups::const_iterator itEnd = _fieldGroups.end();
    FieldGroups::const_iterator it = _fieldGroups.begin();
    it = _fieldGroups.begin();
    for (; it != itEnd; ++it) {
        docSize += it->second->getDocSize();
    }
    return docSize;
}

uint32_t MatchDocAllocator::getSubDocSize() const {
    if (_subAllocator) {
        return _subAllocator->getDocSize();
    }
    return 0;
}

uint32_t MatchDocAllocator::getReferenceCount() const { return _referenceMap.size(); }

uint32_t MatchDocAllocator::getSubReferenceCount() const {
    if (_subAllocator) {
        return _subAllocator->getReferenceCount();
    }
    return 0;
}

void MatchDocAllocator::initSub() {
    _subAllocator = new MatchDocAllocator(_poolPtr, false, _mountInfo);
    _firstSub = declare<MatchDoc>(FIRST_SUB_REF, DEFAULT_GROUP);
    _currentSub = declare<MatchDoc>(CURRENT_SUB_REF, DEFAULT_GROUP);
    _nextSub = _subAllocator->declare<MatchDoc>(NEXT_SUB_REF, DEFAULT_GROUP);
    _subAccessor = new SubDocAccessor(_subAllocator, _firstSub, _currentSub, _nextSub);
}

const MatchDocAllocator::FieldGroups &MatchDocAllocator::getFieldGroups() const { return _fieldGroups; }

ReferenceBase *MatchDocAllocator::cloneReference(const ReferenceBase *reference,
                                                 const std::string &name,
                                                 const std::string &groupName,
                                                 const bool cloneWithMount) {
    // already declared
    ReferenceBase *ref = findReferenceWithoutType(name);
    if (ref) {
        AUTIL_LOG(ERROR, "duplicate reference name: %s", name.c_str());
        return nullptr;
    }
    if (reference->isSubDocReference() && _subAllocator != nullptr) {
        ReferenceBase *cloneRef = _subAllocator->cloneReference(reference, name, groupName, cloneWithMount);
        cloneRef->setCurrentRef(_currentSub);
        _referenceMap[name] = cloneRef;
        return cloneRef;
    }
    // declare in allocated FieldGroup
    if (_fieldGroups.count(groupName) > 0) {
        AUTIL_LOG(ERROR, "clone in allocated FieldGroup: %s", groupName.c_str());
        return nullptr;
    }

    const MountMeta *mountMeta = nullptr;
    if (cloneWithMount && reference->isMount()) {
        mountMeta = _mountInfo->get(reference->getName());
    }
    FieldGroupBuilder *builder = findOrCreate(groupName);
    if (mountMeta == nullptr) {
        ref = builder->declare(name, reference->getReferenceMeta());
    } else {
        ref = builder->declareMount(name, reference->getReferenceMeta(), mountMeta->mountId, mountMeta->mountOffset);
    }
    _referenceMap[name] = ref;
    return ref;
}

std::string MatchDocAllocator::getDefaultGroupName() {
    autil::ScopedSpinLock lock(_lock);
    return getDefaultGroupNameInner();
}

std::string MatchDocAllocator::getDefaultGroupNameInner() {
    if (!_defaultGroupName.empty()) {
        return _defaultGroupName;
    }
    if (!_fieldGroupBuilders.empty()) {
        return _fieldGroupBuilders.begin()->first;
    }
    while (true) {
        string groupName = "_default_" + autil::StringUtil::toString(_defaultGroupNameCounter) + "_";
        if (_fieldGroups.find(groupName) == _fieldGroups.end()) {
            return groupName;
        }
        ++_defaultGroupNameCounter;
    }
    return "";
}

bool MatchDocAllocator::isSame(const MatchDocAllocator &other) const {
    if (this == &other) {
        return true;
    }
    if (!isSame(_fieldGroups, other._fieldGroups)) {
        AUTIL_LOG(WARN, "fieldGroups not same");
        return false;
    }
    // compare builders
    if (_fieldGroupBuilders.size() != other._fieldGroupBuilders.size()) {
        return false;
    }
    for (auto it1 = _fieldGroupBuilders.begin(), it2 = other._fieldGroupBuilders.begin();
         it1 != _fieldGroupBuilders.end() && it2 != other._fieldGroupBuilders.end();
         ++it1, ++it2) {
        if (it1->first != it2->first) {
            return false;
        }
        if (!it1->second->equals(*(it2->second))) {
            return false;
        }
    }
    if ((_subAllocator == nullptr && other._subAllocator != nullptr) ||
        (_subAllocator != nullptr && other._subAllocator == nullptr)) {
        AUTIL_LOG(WARN, "sub allocate not same");
        return false;
    }
    return _subAllocator ? _subAllocator->isSame(*(other._subAllocator)) : true;
}

bool MatchDocAllocator::isSame(const FieldGroups &src, const FieldGroups &dst) const {
    if (src.size() != dst.size()) {
        AUTIL_LOG(WARN, "field group count not same, src[%lu], dst[%lu]", src.size(), dst.size());
        return false;
    }
    for (auto it1 = src.begin(), it2 = dst.begin(); it1 != src.end() && it2 != dst.end(); it1++, it2++) {
        if (it1->first != it2->first) {
            AUTIL_LOG(WARN, "group name not equal, src[%s], dst[%s]", it1->first.c_str(), it2->first.c_str());
            return false;
        }
        FieldGroup *g1 = it1->second;
        FieldGroup *g2 = it2->second;
        if ((g1 == nullptr && g2 != nullptr) || (g1 != nullptr && g2 == nullptr)) {
            return false;
        }
        if (g1 != nullptr && !g1->isSame(*g2)) {
            AUTIL_LOG(WARN, "group[%s] not equal", it1->first.c_str());
            return false;
        }
    }
    return true;
}

ReferenceNameSet MatchDocAllocator::getAllReferenceNames() const {
    autil::ScopedSpinLock lock(_lock);
    unordered_set<string> names;
    std::for_each(_fieldGroups.begin(), _fieldGroups.end(), [&](const auto &it) {
        const auto &refSet = it.second->getReferenceSet();
        refSet.for_each([&](auto ref) { names.insert(ref->getName()); });
    });
    std::for_each(_fieldGroupBuilders.begin(), _fieldGroupBuilders.end(), [&](const auto &it) {
        const auto &refSet = it.second->getReferenceSet();
        refSet.for_each([&](auto ref) { names.insert(ref->getName()); });
    });
    if (_subAllocator) {
        auto subNames = _subAllocator->getAllReferenceNames();
        names.insert(subNames.begin(), subNames.end());
    }
    return names;
}

ReferenceVector MatchDocAllocator::getAllNeedSerializeReferences(uint8_t serializeLevel) const {
    ReferenceVector refVec;
    std::for_each(_fieldGroups.begin(), _fieldGroups.end(), [&](const auto &it) {
        const auto &refSet = it.second->getReferenceSet();
        refSet.for_each([&](auto ref) {
            if (ref->getSerializeLevel() >= serializeLevel) {
                refVec.push_back(ref);
            }
        });
    });
    std::for_each(_fieldGroupBuilders.begin(), _fieldGroupBuilders.end(), [&](const auto &it) {
        const auto &refSet = it.second->getReferenceSet();
        refSet.for_each([&](auto ref) {
            if (ref->getSerializeLevel() >= serializeLevel) {
                refVec.push_back(ref);
            }
        });
    });
    return refVec;
}

ReferenceVector MatchDocAllocator::getAllSubNeedSerializeReferences(uint8_t serializeLevel) const {
    if (!_subAllocator) {
        return ReferenceVector();
    }
    return _subAllocator->getAllNeedSerializeReferences(serializeLevel);
}

ReferenceVector MatchDocAllocator::getRefBySerializeLevel(uint8_t serializeLevel) const {
    ReferenceVector refVec;
    auto fn = [&](ReferenceBase *ref) {
        if (ref->getSerializeLevel() == serializeLevel) {
            refVec.push_back(ref);
        }
    };
    std::for_each(_fieldGroups.begin(), _fieldGroups.end(), [&](const auto &it) {
        const auto &refSet = it.second->getReferenceSet();
        refSet.for_each(fn);
    });
    std::for_each(_fieldGroupBuilders.begin(), _fieldGroupBuilders.end(), [&](const auto &it) {
        const auto &refSet = it.second->getReferenceSet();
        refSet.for_each(fn);
    });
    return refVec;
}

FieldGroupBuilder *MatchDocAllocator::findOrCreate(const std::string &name) {
    auto it = _fieldGroupBuilders.find(name);
    if (it != _fieldGroupBuilders.end()) {
        return it->second.get();
    } else {
        auto builderPtr = std::make_unique<FieldGroupBuilder>(name, _poolPtr);
        FieldGroupBuilder *builder = builderPtr.get();
        _fieldGroupBuilders.emplace(name, std::move(builderPtr));
        return builder;
    }
}

std::string MatchDocAllocator::toDebugString() const {
    stringstream ss;
    for (const auto &group : _fieldGroups) {
        const auto &refMap = group.second->getReferenceMap();
        for (const auto &ref : refMap) {
            ss << "ref: " << ref.first << ' ' << ref.second->toDebugString();
        }
    }
    return ss.str();
}

std::string
MatchDocAllocator::toDebugString(uint8_t serializeLevel, const std::vector<MatchDoc> &matchDocs, bool onlySize) const {
    stringstream ss;
    auto refs = getAllNeedSerializeReferences(serializeLevel);
    if (onlySize) {
        DataBuffer dataBuffer;
        int64_t last = 0;
        for (const auto &ref : refs) {
            for (size_t i = 0; i < matchDocs.size(); i++) {
                ref->serialize(matchDocs[i], dataBuffer);
            }
            ss << ref->getName() << " : " << dataBuffer.getDataLen() - last << ", ";
            last = dataBuffer.getDataLen();
        }
    } else {
        for (const auto &ref : refs) {
            ss << "ref: " << ref->getName() << ' ' << ref->toDebugString();
            for (size_t i = 0; i < matchDocs.size(); i++) {
                ss << i << ':' << ref->toString(matchDocs[i]) << ", ";
            }
        }
    }
    return ss.str();
}

MatchDocAllocator *MatchDocAllocator::cloneAllocatorWithoutData(const std::shared_ptr<autil::mem_pool::Pool> &pool) {
    auto poolInUse = pool;
    if (!pool) {
        poolInUse = _poolPtr;
    }
    DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, poolInUse.get());
    setSortRefFlag(false);
    this->serializeMeta(dataBuffer, 0);
    auto allocator = new MatchDocAllocator(poolInUse);
    allocator->deserializeMeta(dataBuffer, nullptr);
    return allocator;
}

bool MatchDocAllocator::swapDocStorage(MatchDocAllocator &other,
                                       std::vector<MatchDoc> &otherMatchDocs,
                                       std::vector<MatchDoc> &myMatchDocs) {
    if (swapDocStorage(other)) {
        otherMatchDocs.swap(myMatchDocs);
        return true;
    } else {
        return false;
    }
}

bool MatchDocAllocator::swapDocStorage(MatchDocAllocator &other) {
    if (_mountInfo) {
        return false;
    }
    this->extend();
    this->extendSub();
    other.extend();
    other.extendSub();
    if (!isSame(other)) {
        return false;
    }
    for (auto it1 = _fieldGroups.begin(), it2 = other._fieldGroups.begin();
         it1 != _fieldGroups.end() && it2 != other._fieldGroups.end();
         it1++, it2++) {
        it1->second->swapDocStorage(*it2->second);
    }
    swap(_size, other._size);
    swap(_capacity, other._capacity);
    swap(_poolPtr, other._poolPtr);
    _deletedIds.swap(other._deletedIds);
    if (_subAllocator) {
        _subAllocator->swapDocStorage(*other._subAllocator);
    }
    return true;
}

bool MatchDocAllocator::addGroup(std::unique_ptr<FieldGroup> group, bool genRefIndex, bool construct) {
    autil::ScopedSpinLock lock(_lock);
    return addGroupLocked(std::move(group), genRefIndex, construct);
}

bool MatchDocAllocator::addGroupLocked(std::unique_ptr<FieldGroup> group, bool genRefIndex, bool construct) {
    const auto &groupName = group->getGroupName();
    if (_fieldGroups.count(groupName) > 0) {
        AUTIL_LOG(ERROR, "group %s already exists", groupName.c_str());
        return false;
    }
    if (genRefIndex) {
        const auto &refSet = group->getReferenceSet();
        bool hasDuplicatedRef = false;
        refSet.for_each([&](auto ref) {
            auto ret = _referenceMap.emplace(ref->getName(), ref);
            if (!ret.second) {
                AUTIL_LOG(WARN, "duplicate reference %s", ref->getName().c_str());
                hasDuplicatedRef = true;
            }
        });
        if (hasDuplicatedRef) {
            return false;
        }
    }
    extendGroup(group.get(), construct);
    _fieldGroups.emplace(groupName, group.release());
    return true;
}

ReferenceMap MatchDocAllocator::getAllNeedSerializeReferenceMap(uint8_t serializeLevel) const {
    autil::ScopedSpinLock lock(_lock);
    ReferenceMap refmap;
    std::for_each(_fieldGroups.begin(), _fieldGroups.end(), [&](const auto &it) {
        const auto &refSet = it.second->getReferenceSet();
        refSet.for_each([&](auto ref) {
            if (ref->getSerializeLevel() >= serializeLevel) {
                refmap.emplace(ref->getName(), ref);
            }
        });
    });
    return refmap;
}

template <typename T>
bool MatchDocAllocator::deserializeAndAppendImpl(autil::DataBuffer &dataBuffer, T &matchDocs) {
    int64_t thisSignature = 0;
    dataBuffer.read(thisSignature);

    // skip meta
    uint32_t metaLen = 0;
    dataBuffer.read(metaLen);
    auto metaData = dataBuffer.readNoCopy(metaLen);

    if (thisSignature != _metaSignature) {
        AUTIL_LOG(WARN,
                  "signature not same, can not append, this signature: %ld"
                  ", target signature: %ld",
                  thisSignature,
                  _metaSignature);

        // deserialize meta and print debug infos
        MatchDocAllocator debugAllocator(_poolPtr, _subAllocator);
        autil::DataBuffer metaBuffer((void *)metaData, metaLen, dataBuffer.getPool());
        debugAllocator.deserializeMeta(metaBuffer, nullptr);

        AUTIL_LOG(WARN, "target allocator : [%s]", toDebugString().c_str());
        AUTIL_LOG(WARN, "deserialize allocator : [%s]", debugAllocator.toDebugString().c_str());
        return false;
    }

    uint32_t count = 0;
    dataBuffer.read(count);
    extend();

    if (_subAllocator) {
        uint32_t subCount = 0;
        dataBuffer.read(subCount);
        _subAllocator->extend();
    }

    matchDocs.reserve(count + matchDocs.size());
    for (uint32_t i = 0; i < count; i++) {
        MatchDoc doc = deserializeMatchDoc(dataBuffer);
        matchDocs.push_back(doc);
    }
    return true;
}

template <typename T>
void MatchDocAllocator::serializeImpl(autil::DataBuffer &dataBuffer,
                                      const T &matchdocs,
                                      uint8_t serializeLevel,
                                      bool ignoreUselessGroupMeta) {
    autil::DataBuffer metaBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, dataBuffer.getPool());
    // 1. serialize meta
    serializeMeta(metaBuffer, serializeLevel, ignoreUselessGroupMeta);

    int64_t metaSignature = autil::MurmurHash::MurmurHash64A(metaBuffer.getData(), metaBuffer.getDataLen(), 0);
    dataBuffer.write(metaSignature);
    uint32_t dataLen = metaBuffer.getDataLen();
    dataBuffer.write(dataLen);
    dataBuffer.writeBytes(metaBuffer.getData(), metaBuffer.getDataLen());

    // 2. serialize matchdocs
    dataBuffer.write(uint32_t(matchdocs.size()));
    // for reserve
    if (_subAllocator) {
        dataBuffer.write(uint32_t(_subAllocator->_size));
    }
    for (size_t i = 0; i < matchdocs.size(); i++) {
        serializeMatchDoc(dataBuffer, matchdocs[i], matchdocs[i].getDocId(), serializeLevel);
    }
}

template <typename T>
void MatchDocAllocator::deserializeImpl(autil::DataBuffer &dataBuffer, T &matchdocs) {
    dataBuffer.read(_metaSignature);
    uint32_t metaLen = 0;
    dataBuffer.read(metaLen);
    const void *metaData = dataBuffer.readNoCopy(metaLen);

    autil::DataBuffer metaBuffer((void *)metaData, metaLen, dataBuffer.getPool());

    // 1. deserialize meta
    deserializeMeta(metaBuffer, nullptr);

    // 2. deserialize matchdocs
    uint32_t count = 0;
    dataBuffer.read(count);
    extend();

    if (_subAllocator) {
        uint32_t subcount = 0;
        dataBuffer.read(subcount);
        _subAllocator->extend();
    }

    for (uint32_t i = 0; i < count; i++) {
        MatchDoc doc = deserializeMatchDoc(dataBuffer);
        matchdocs.push_back(doc);
    }
}

template <typename DocIdGenerator>
void MatchDocAllocator::innerBatchAllocate(DocIdGenerator &docIdGenerator,
                                           std::vector<matchdoc::MatchDoc> &retVec,
                                           bool adviseNotConstruct) {
    if (!_fieldGroupBuilders.empty()) {
        extend();
    }
    size_t count = docIdGenerator.size();
    size_t delCount = _deletedIds.size();
    retVec.reserve(count);
    if (delCount < count) {
        for (size_t i = 0; i < delCount; i++) {
            retVec.emplace_back(_deletedIds[i], docIdGenerator.element(i));
        }
        _deletedIds.clear();
        size_t leftCount = count - delCount;
        while (_capacity <= _size + leftCount) {
            grow();
        }
        for (size_t i = delCount; i < count; i++) {
            retVec.emplace_back(_size++, docIdGenerator.element(i));
        }
    } else {
        size_t offset = delCount - count;
        for (size_t i = 0; i < count; i++) {
            retVec.emplace_back(_deletedIds[i + offset], docIdGenerator.element(i));
        }
        _deletedIds.resize(offset);
    }

    FieldGroups::iterator it = _fieldGroups.begin();
    FieldGroups::iterator itEnd = _fieldGroups.end();
    for (; it != itEnd; ++it) {
        if (it->second->needDestruct() || !adviseNotConstruct) {
            it->second->constructDocs(retVec);
        }
    }

    return;
}

std::unique_ptr<MatchDocAllocator>
MatchDocAllocator::fromFieldGroups(std::shared_ptr<autil::mem_pool::Pool> pool,
                                   std::vector<std::unique_ptr<FieldGroup>> fieldGroups,
                                   uint32_t size,
                                   uint32_t capacity) {
    auto allocator = std::make_unique<MatchDocAllocator>(pool);
    for (size_t i = 0; i < fieldGroups.size(); ++i) {
        if (!allocator->addGroup(std::move(fieldGroups[i]), true, false)) {
            return nullptr;
        }
    }
    allocator->_size = size;
    allocator->_capacity = capacity;
    return allocator;
}

} // namespace matchdoc
