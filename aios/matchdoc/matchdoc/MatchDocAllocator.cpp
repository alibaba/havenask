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

#include "autil/DataBuffer.h"
#include "autil/Hyperloglog.h"
#include "autil/Lock.h"
#include "autil/LongHashValue.h"
#include "autil/MultiValueType.h"
#include "autil/MurmurHash.h"
#include "autil/StringUtil.h"
#include <algorithm>
#include <cstdint>
#include <limits>
#include <ostream>
#include <type_traits>
#include "matchdoc/SubDocAccessor.h"
#include "matchdoc/VectorDocStorage.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

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

static std::unordered_set<std::string> createStaticMountableTypes() {
    unordered_set<string> mountTypes;
    mountTypes.insert(typeid(uint8_t).name());
    mountTypes.insert(typeid(uint16_t).name());
    mountTypes.insert(typeid(uint32_t).name());
    mountTypes.insert(typeid(uint64_t).name());
    mountTypes.insert(typeid(int8_t).name());
    mountTypes.insert(typeid(int16_t).name());
    mountTypes.insert(typeid(int32_t).name());
    mountTypes.insert(typeid(int64_t).name());
    mountTypes.insert(typeid(double).name());
    mountTypes.insert(typeid(float).name());

    mountTypes.insert(typeid(MultiInt8).name());
    mountTypes.insert(typeid(MultiUInt8).name());
    mountTypes.insert(typeid(MultiInt16).name());
    mountTypes.insert(typeid(MultiUInt16).name());
    mountTypes.insert(typeid(MultiInt32).name());
    mountTypes.insert(typeid(MultiUInt32).name());
    mountTypes.insert(typeid(MultiInt64).name());
    mountTypes.insert(typeid(MultiUInt64).name());
    mountTypes.insert(typeid(MultiFloat).name());
    mountTypes.insert(typeid(MultiDouble).name());
    mountTypes.insert(typeid(MultiChar).name());
    mountTypes.insert(typeid(MultiString).name());
    return mountTypes;
}

static ReferenceTypes createStaticReferenceTypes() {
    ReferenceTypes refTypes;
    Reference<uint8_t>::registerInnerType(&refTypes);

    Reference<uint8_t>::registerInnerType(&refTypes);
    Reference<uint16_t>::registerInnerType(&refTypes);
    Reference<uint32_t>::registerInnerType(&refTypes);
    Reference<uint64_t>::registerInnerType(&refTypes);
    Reference<autil::uint128_t>::registerInnerType(&refTypes);
    Reference<int8_t>::registerInnerType(&refTypes);
    Reference<int16_t>::registerInnerType(&refTypes);
    Reference<int32_t>::registerInnerType(&refTypes);
    Reference<int64_t>::registerInnerType(&refTypes);
    Reference<double>::registerInnerType(&refTypes);
    Reference<float>::registerInnerType(&refTypes);
    Reference<bool>::registerInnerType(&refTypes);
    Reference<std::string>::registerInnerType(&refTypes);
    Reference<MatchDoc>::registerInnerType(&refTypes);
    Reference<autil::HllCtx>::registerInnerType(&refTypes);

    Reference<MultiInt8>::registerInnerType(&refTypes);
    Reference<MultiUInt8>::registerInnerType(&refTypes);
    Reference<MultiInt16>::registerInnerType(&refTypes);
    Reference<MultiUInt16>::registerInnerType(&refTypes);
    Reference<MultiInt32>::registerInnerType(&refTypes);
    Reference<MultiUInt32>::registerInnerType(&refTypes);
    Reference<MultiInt64>::registerInnerType(&refTypes);
    Reference<MultiUInt64>::registerInnerType(&refTypes);
    Reference<MultiFloat>::registerInnerType(&refTypes);
    Reference<MultiDouble>::registerInnerType(&refTypes);
    Reference<MultiChar>::registerInnerType(&refTypes);
    Reference<MultiString>::registerInnerType(&refTypes);

    Reference<std::vector<int8_t>>::registerInnerType(&refTypes);
    Reference<std::vector<int16_t>>::registerInnerType(&refTypes);
    Reference<std::vector<int32_t>>::registerInnerType(&refTypes);
    Reference<std::vector<int64_t>>::registerInnerType(&refTypes);
    Reference<std::vector<uint8_t>>::registerInnerType(&refTypes);
    Reference<std::vector<uint16_t>>::registerInnerType(&refTypes);
    Reference<std::vector<uint32_t>>::registerInnerType(&refTypes);
    Reference<std::vector<uint64_t>>::registerInnerType(&refTypes);
    Reference<std::vector<autil::uint128_t>>::registerInnerType(&refTypes);
    Reference<std::vector<float>>::registerInnerType(&refTypes);
    Reference<std::vector<double>>::registerInnerType(&refTypes);
    Reference<std::vector<bool>>::registerInnerType(&refTypes);
    Reference<std::vector<std::string>>::registerInnerType(&refTypes);

    return refTypes;
}
std::unordered_set<std::string> MatchDocAllocator::staticMountableTypes =
    createStaticMountableTypes();
ReferenceTypes MatchDocAllocator::staticReferenceTypes = createStaticReferenceTypes();


MatchDocAllocator::MatchDocAllocator(
        Pool *pool, bool useSub,
        const MountInfoPtr& mountInfo)
    : _size(0)
    , _capacity(0)
    , _metaSignature(0)
    , _sessionPool(pool)
    , _mountInfo(mountInfo)
    , _subAllocator(nullptr)
    , _firstSub(nullptr)
    , _currentSub(nullptr)
    , _nextSub(nullptr)
    , _subAccessor(nullptr)
    , _defaultGroupNameCounter(0)
    , _merged(false)
    , _mountInfoIsPrivate(false)
    , _sortRefFlag(true)
{
    _mountableTypesWrapper = new MountableTypesWrapper(&staticMountableTypes);
    _referenceTypesWrapper = new ReferenceTypesWrapper(&staticReferenceTypes);
    if (useSub) {
        initSub();
    }
}

MatchDocAllocator::MatchDocAllocator(
        const std::shared_ptr<autil::mem_pool::Pool> &poolPtr,
        bool useSub,
        const MountInfoPtr& mountInfo)
    : MatchDocAllocator(poolPtr.get(), false, mountInfo)
{
    _poolPtr = poolPtr;
    if (useSub) {
        initSub();
    }
}

MatchDocAllocator::MatchDocAllocator(
        const std::shared_ptr<autil::mem_pool::Pool> &poolPtr,
        Pool *pool,
        bool useSub,
        const MountInfoPtr& mountInfo)
    : MatchDocAllocator(pool, false, mountInfo)
{
    _poolPtr = poolPtr;
    if (useSub) {
        initSub();
    }
}

MatchDocAllocator::~MatchDocAllocator() {
    if (!_merged) {
        deallocateAllMatchDocs();
    }
    clearFieldGroups();

    DELETE_AND_SET_NULL(_subAllocator);
    DELETE_AND_SET_NULL(_subAccessor);
    DELETE_AND_SET_NULL(_mountableTypesWrapper);
    DELETE_AND_SET_NULL(_referenceTypesWrapper);
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
                AUTIL_LOG(WARN, "insert mount field [%s] failed. mount offset [%lu] vs [%lu]",
                        fieldName.c_str(), meta.mountOffset, existedMeta->mountOffset);
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

bool MatchDocAllocator::mergeMountInfo(const MountInfoPtr &mountInfo,
                                       const std::set<std::string> &mergeFields)
{
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

bool MatchDocAllocator::mergeMountInfo(const MatchDocAllocator *allocator,
                                       const std::set<std::string> &mergeFields)
{
    if (unlikely(allocator == nullptr)) {
        return true;
    }
    return mergeMountInfo(allocator->getMountInfo(), mergeFields);
}

void MatchDocAllocator::clearFieldGroups() {
    FieldGroups::iterator it = _fieldGroups.begin();
    FieldGroups::iterator itEnd = _fieldGroups.end();
    for (; it != itEnd; ++it) {
        delete it->second;
    }
    _fieldGroups.clear();
    it = _toExtendfieldGroups.begin();
    itEnd = _toExtendfieldGroups.end();
    for (; it != itEnd; ++it) {
        delete it->second;
    }
    _toExtendfieldGroups.clear();
    _fastReferenceMap.clear();
}

uint32_t MatchDocAllocator::getAllocatedCount() const {
    autil::ScopedSpinLock lock(_lock);
    return _size;
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

void MatchDocAllocator::clean(const unordered_set<string>& keepFieldGroups) {
    deallocateAllMatchDocs();
    extend();
    FieldGroups::iterator it = _fieldGroups.begin();
    for (; it != _fieldGroups.end();) {
        if (keepFieldGroups.find(it->first) == keepFieldGroups.end())
        {
            const auto &refMap = it->second->getReferenceMap();
            for (const auto &ref : refMap) {
                auto it2 = _fastReferenceMap.find(ref.first);
                if (_fastReferenceMap.end() != it2) {
                    _fastReferenceMap.erase(it2);
                }
            }
            delete it->second;
            _fieldGroups.erase(it++);
        }
        else
        {
            it->second->clearContent();
            it->second->resetSerializeLevelAndAlias();
            it++;
        }
    }
    if (_subAllocator)
    {
        _subAllocator->clean(keepFieldGroups);
        _firstSub = declare<MatchDoc>(FIRST_SUB_REF, DEFAULT_GROUP);
        _currentSub = declare<MatchDoc>(CURRENT_SUB_REF, DEFAULT_GROUP);
        for (const auto& entry : _subAllocator->getFastReferenceMap()) {
            entry.second->setCurrentRef(_currentSub);
        }
        _nextSub = _subAllocator->declare<MatchDoc>(NEXT_SUB_REF, DEFAULT_GROUP);
        _nextSub->setCurrentRef(nullptr);
        DELETE_AND_SET_NULL(_subAccessor);
        _subAccessor = new SubDocAccessor(_subAllocator,
                _firstSub, _currentSub, _nextSub);
    }
    _deletedIds.clear();
    _size = 0;
    _capacity = 0;
    _merged = false;
    _aliasMap.reset();
    _defaultGroupNameCounter = 0;
}

void MatchDocAllocator::truncateSubDoc(MatchDoc parent,
                                       MatchDoc subdoc) const
{
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

void MatchDocAllocator::cowMountedGroup() {
    FieldGroups::iterator it = _mountedGroups.begin();
    FieldGroups::iterator itEnd = _mountedGroups.end();
    for (; it != itEnd; ++it) {
        it->second->doCoW();
    }
    _mountedGroups.clear();
}

MatchDoc MatchDocAllocator::allocate(int32_t docid) {
    if (!_toExtendfieldGroups.empty()) {
        extend();
    }
    cowMountedGroup();
    MatchDoc doc = getOne();
    FieldGroups::iterator it = _fieldGroups.begin();
    FieldGroups::iterator itEnd = _fieldGroups.end();
    for (; it != itEnd; ++it) {
        it->second->constructDoc(doc);
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

void MatchDocAllocator::batchAllocate(std::vector<matchdoc::MatchDoc>& matchDocs,
                                      size_t count, int32_t docid, bool adviseNotConstruct) {
    ConstDocIdGenerator generator(count, docid);
    return innerBatchAllocate<ConstDocIdGenerator>(generator, matchDocs, adviseNotConstruct);
}

void MatchDocAllocator::batchAllocate(size_t count, int32_t docid,
                                      std::vector<matchdoc::MatchDoc> &retVec, bool adviseNotConstruct) {
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

std::vector<matchdoc::MatchDoc> MatchDocAllocator::batchAllocate(const std::vector<int32_t> &docIds, bool adviseNotConstruct) {
    std::vector<matchdoc::MatchDoc> retVec;
    batchAllocate(docIds, retVec, adviseNotConstruct);
    return retVec;
}

void MatchDocAllocator::batchAllocate(const std::vector<int32_t>& docIds,
                                      std::vector<matchdoc::MatchDoc> &retVec, bool adviseNotConstruct)
{
    VectorDocIdGenerator generator(docIds);
    return innerBatchAllocate<VectorDocIdGenerator>(generator, retVec, adviseNotConstruct);
}


matchdoc::MatchDoc MatchDocAllocator::batchAllocateSubdoc(size_t count,
        std::vector<matchdoc::MatchDoc> &subdocVec, int32_t docid)
{
    MatchDoc doc = allocate(docid);
    vector<int32_t> docIds(count, -1);
    if (_subAllocator) {
        _subAllocator->batchAllocate(docIds, subdocVec);
    }
    return doc;
}

MatchDoc MatchDocAllocator::allocateAndClone(const MatchDoc &cloneDoc) {
    if (!_toExtendfieldGroups.empty()) {
        extend();
    }
    cowMountedGroup();
    MatchDoc newDoc = getOne();
    FieldGroups::iterator it = _fieldGroups.begin();
    FieldGroups::iterator itEnd = _fieldGroups.end();
    for (; it != itEnd; ++it) {
        it->second->cloneDoc(newDoc, cloneDoc);
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
	_subAccessor->foreach(cloneDoc, subCloner);
        auto firstNewSubDoc = _firstSub->get(newDoc);
        _currentSub->set(newDoc, firstNewSubDoc);
    }
    return newDoc;
}

MatchDoc MatchDocAllocator::allocateAndClone(
        MatchDocAllocator *other,
        const MatchDoc &matchDoc)
{
    if (!_toExtendfieldGroups.empty()) {
        extend();
    }
    cowMountedGroup();
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
    MatchDoc *ret = NULL;
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
    const std::string& refName = getReferenceName(name);
    autil::ScopedSpinLock lock(_lock);
    auto ref = findMainReference(refName);
    if (ref) {
        _fastReferenceMap.erase(refName);
        deallocateAllMatchDocs(ref);
        const auto &groupName = ref->getGroupName();
        auto extendIter = _toExtendfieldGroups.find(groupName);
        if (extendIter != _toExtendfieldGroups.end()) {
            auto fieldGroup = extendIter->second;
            if (fieldGroup->dropField(refName)) {
                delete fieldGroup;
                _toExtendfieldGroups.erase(extendIter);
            }
        }
        auto iter = _fieldGroups.find(groupName);
        if (iter != _fieldGroups.end()) {
            auto fieldGroup = iter->second;
            if (fieldGroup->dropField(refName)) {
                delete fieldGroup;
                _fieldGroups.erase(iter);
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

void MatchDocAllocator::reserveFields(uint8_t serializeLevel) {
    ReferenceNameSet needReserveFieldSet;
    for (auto *ref : getAllNeedSerializeReferences(serializeLevel)) {
        needReserveFieldSet.insert(ref->getName());
    }
    for (auto *ref : getAllSubNeedSerializeReferences(serializeLevel)) {
        needReserveFieldSet.insert(ref->getName());
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
        AUTIL_LOG(WARN, "skip renaming reference [%s] to [%s].",
                            name.c_str(), dstName.c_str());
        return true;
    }
    if (findReferenceByName(getReferenceName(dstName)) != nullptr) {
        AUTIL_LOG(WARN, "skip renaming reference [%s] to [%s], since [%s] already exists.",
                            name.c_str(), dstName.c_str(), dstName.c_str());
        return false;
    }
    renameFieldImpl(name, dstName);
    return true;
}

bool MatchDocAllocator::renameFieldImpl(const std::string &name, const std::string &dstName) {
    const std::string& refName = getReferenceName(name);
    auto ref = findMainReference(refName);
    if (ref) {
        _fastReferenceMap.erase(refName);
        _fastReferenceMap.emplace(dstName, ref);
        const auto &groupName = ref->getGroupName();
        auto extendIter = _toExtendfieldGroups.find(groupName);
        if (extendIter != _toExtendfieldGroups.end()) {
            auto fieldGroup = extendIter->second;
            fieldGroup->renameField(refName, dstName);
        }
        auto iter = _fieldGroups.find(groupName);
        if (iter != _fieldGroups.end()) {
            auto fieldGroup = iter->second;
            fieldGroup->renameField(refName, dstName);
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
            const MountMeta* mountMeta = _mountInfo->get(refName);
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
    if(_deletedIds.size() == _size) {
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
        _subAccessor->foreach(doc, deleter);
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
            _subAccessor->foreach(docs[i], deleter);
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
    _capacity += VectorDocStorage::CHUNKSIZE;
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
    if (_toExtendfieldGroups.empty()) {
        return;
    }
    FieldGroups::iterator it = _toExtendfieldGroups.begin();
    FieldGroups::iterator itEnd = _toExtendfieldGroups.end();
    for (; it != itEnd; ++it) {
        extendGroup(it->second);
    }
    _fieldGroups.insert(_toExtendfieldGroups.begin(),
                        _toExtendfieldGroups.end());
    _toExtendfieldGroups.clear();
}

void MatchDocAllocator::extendSub() {
    if (_subAllocator) {
        _subAllocator->extend();
    }
}

void MatchDocAllocator::extendGroup(const std::string &group) {
    autil::ScopedSpinLock lock(_lock);
    auto it = _toExtendfieldGroups.find(group);
    if (it == _toExtendfieldGroups.end()) {
        return;
    }
    extendGroup(it->second);
    _fieldGroups[it->first] = it->second;
    _toExtendfieldGroups.erase(it->first);
}

void MatchDocAllocator::extendSubGroup(const std::string &group) {
    _subAllocator->extendGroup(group);
}

ReferenceBase* MatchDocAllocator::findMainReference(const std::string &name) const {
    auto it = _fastReferenceMap.find(name);
    if (_fastReferenceMap.end() == it) {
        return nullptr;
    }
    return it->second;
}

ReferenceBase* MatchDocAllocator::findSubReference(const std::string &name) const {
    if (_subAllocator) {
        return _subAllocator->findReferenceWithoutType(name);
    }
    return NULL;
}

ReferenceBase* MatchDocAllocator::findReferenceByName(const std::string &name) const {
    ReferenceBase* mainRef = findMainReference(name);
    if (mainRef) {
        return mainRef;
    }
    if (unlikely(_subAllocator != NULL)) {
        mainRef = findSubReference(name);
    }
    return mainRef;
}

void MatchDocAllocator::serializeMeta(
        DataBuffer &dataBuffer, uint8_t serializeLevel,
        bool ignoreUselessGroupMeta)
{
    extend(); // extend group for zero matchdoc
    // make sure FieldGroup is ordered, serialize use this to cal signature
    // 1. serialize main
    FieldGroups *tmpFieldGroups = &_fieldGroups;
    if (ignoreUselessGroupMeta) {
        tmpFieldGroups = new FieldGroups;
        for (auto item : _fieldGroups) {
            if (item.second->needSerialize(serializeLevel)) {
                (*tmpFieldGroups).emplace(item.first, item.second);
            }
        }
    }
    dataBuffer.write((*tmpFieldGroups).size());
    for (const auto &fieldGroup : (*tmpFieldGroups)) {
        bool grpNeedSerialize = false;
        if (ignoreUselessGroupMeta) {
            grpNeedSerialize = true;
        } else {
            grpNeedSerialize = fieldGroup.second->needSerialize(serializeLevel);
        }
        dataBuffer.write(grpNeedSerialize);
        if (grpNeedSerialize) {
            dataBuffer.write(fieldGroup.first);
            if (_sortRefFlag) {
                fieldGroup.second->sortReference();
            }
            fieldGroup.second->serializeMeta(dataBuffer, serializeLevel);
        }
    }

    // 2. serialize sub
    if (_subAllocator) {
        dataBuffer.write(true);
        _subAllocator->serializeMeta(dataBuffer, serializeLevel, ignoreUselessGroupMeta);
    } else {
        dataBuffer.write(false);
    }

    if (ignoreUselessGroupMeta) {
        DELETE_AND_SET_NULL(tmpFieldGroups);
    }
}

void MatchDocAllocator::serializeMatchDoc(DataBuffer &dataBuffer,
        const MatchDoc &matchdoc, int32_t docid, uint8_t serializeLevel)
{
    // 1. serialize main
    dataBuffer.write(docid); // store docid
    FieldGroups::iterator itEnd = _fieldGroups.end();
    FieldGroups::iterator it = _fieldGroups.begin();
    for (; it != itEnd; ++it) {
        it->second->serialize(dataBuffer, matchdoc, serializeLevel);
    }

    // 2. deserialize sub
    if (_subAllocator) {
        SubCounter counter;
        _subAccessor->foreach(matchdoc, counter);
        dataBuffer.write(uint32_t(counter.get()));

        assert(_currentSub);
        MatchDoc &current = _currentSub->getReference(matchdoc);
        Reference<MatchDoc> *nextSubRef =
            _subAllocator->findReference<MatchDoc>(NEXT_SUB_REF);
        assert(nextSubRef);

        MatchDoc originCurrent = current;
        MatchDoc next = _firstSub->get(matchdoc);
        while (next != INVALID_MATCHDOC) {
            current = next;
            next = nextSubRef->get(current);
            _subAllocator->serializeMatchDoc(dataBuffer, matchdoc,
                    current.getDocId(), serializeLevel);
        }
        current = originCurrent;
    }
}

void MatchDocAllocator::serialize(
        DataBuffer &dataBuffer, const vector<MatchDoc> &matchdocs,
        uint8_t serializeLevel, bool ignoreUselessGroupMeta)
{
    serializeImpl(dataBuffer, matchdocs, serializeLevel, ignoreUselessGroupMeta);
}

void MatchDocAllocator::serialize(
        DataBuffer &dataBuffer, const autil::mem_pool::PoolVector<MatchDoc> &matchdocs,
        uint8_t serializeLevel, bool ignoreUselessGroupMeta)
{
    serializeImpl(dataBuffer, matchdocs, serializeLevel, ignoreUselessGroupMeta);
}

void MatchDocAllocator::deserializeMeta(DataBuffer &dataBuffer,
                                        Reference<MatchDoc> *currentSub)
{
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

        string name;
        dataBuffer.read(name);
        FieldGroup *fieldGroup = new FieldGroup(_sessionPool, _referenceTypesWrapper, name);
        fieldGroup->setPoolPtr(_poolPtr);
        fieldGroup->deserializeMeta(dataBuffer);
        fieldGroup->setCurrentRef(currentSub);
        _fieldGroups[name] = fieldGroup;
        for (const auto &ref : fieldGroup->getReferenceMap()) {
            _fastReferenceMap[ref.first] = ref.second;
        }
    }

    // 2. deserialize sub
    bool useSub = false;
    dataBuffer.read(useSub);
    if (useSub) {
        DELETE_AND_SET_NULL(_subAccessor);
        DELETE_AND_SET_NULL(_subAllocator);
        _subAllocator = new MatchDocAllocator(_poolPtr, _sessionPool);
        _subAllocator->registerTypes(getExtraType());
        _firstSub = declare<MatchDoc>(FIRST_SUB_REF, BUILD_IN_REF_GROUP);
        _currentSub = declare<MatchDoc>(CURRENT_SUB_REF, BUILD_IN_REF_GROUP);

        _subAllocator->deserializeMeta(dataBuffer, this->_currentSub);
        _nextSub = _subAllocator->declare<MatchDoc>(NEXT_SUB_REF, BUILD_IN_REF_GROUP);
        _nextSub->setCurrentRef(NULL);

        _subAccessor = new SubDocAccessor(_subAllocator,
                _firstSub, _currentSub, _nextSub);
    }
}

void MatchDocAllocator::deserializeOneMatchDoc(
        DataBuffer &dataBuffer, const MatchDoc &doc)
{
    FieldGroups::iterator itEnd = _fieldGroups.end();
    FieldGroups::iterator it = _fieldGroups.begin();
    it = _fieldGroups.begin();
    for (; it != itEnd; ++it) {
        it->second->deserialize(dataBuffer, doc);
    }
}

MatchDoc MatchDocAllocator::deserializeMatchDoc(DataBuffer &dataBuffer)
{
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

void MatchDocAllocator::deserialize(DataBuffer &dataBuffer,
                                    vector<MatchDoc> &matchdocs)
{
    deserializeImpl(dataBuffer, matchdocs);
}

void MatchDocAllocator::deserialize(DataBuffer &dataBuffer,
                                    autil::mem_pool::PoolVector<MatchDoc> &matchdocs)
{
    deserializeImpl(dataBuffer, matchdocs);
}

bool MatchDocAllocator::deserializeAndAppend(
        DataBuffer &dataBuffer,
        vector<MatchDoc> &matchdocs)
{
    return deserializeAndAppendImpl(dataBuffer, matchdocs);
}

bool MatchDocAllocator::deserializeAndAppend(DataBuffer &dataBuffer,
        autil::mem_pool::PoolVector<MatchDoc> &matchdocs)
{
    return deserializeAndAppendImpl(dataBuffer, matchdocs);
}

// allocator other will be destroyed, any valid doc
// belonging to it has to be in matchDocs
bool MatchDocAllocator::mergeAllocator(MatchDocAllocator *other,
                                       const std::vector<MatchDoc> &matchDocs,
                                       std::vector<MatchDoc> &newMatchDocs)
{
    if (other->_merged) {
        return false;
    }
    extend();
    other->extend();
    if (other == this || matchDocs.empty()) {
        newMatchDocs.insert(newMatchDocs.end(), matchDocs.begin(),
                            matchDocs.end());
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
        _subAllocator->mergeAllocator(other->_subAllocator,
                subMatchDocs, subNewMatchDocs);
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
                                       std::vector<MatchDoc> &newMatchDocs)
{
    if (other->_merged) {
        return false;
    }
    extend();
    other->extend();
    if (other == this || matchDocs.empty()) {
        newMatchDocs.insert(newMatchDocs.end(), matchDocs.begin(),
                            matchDocs.end());
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
        _subAllocator->mergeAllocatorOnlySame(other->_subAllocator,
                subMatchDocs, subNewMatchDocs);
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
    for (const auto &refPair : _fastReferenceMap) {
        const auto &refName = refPair.first;
        auto ref = refPair.second;
        if (!ref->supportClone()) {
            return false;
        }
        auto otherRef = other->findMainReference(refName);
        if (!otherRef || ref->getReferenceMeta() != otherRef->getReferenceMeta()) {
            if (otherRef) {
                AUTIL_LOG(DEBUG, "ref not equal: [%s] -> [%s]", ref->toDebugString().c_str(),
                        otherRef->toDebugString().c_str());
            } else {
                AUTIL_LOG(DEBUG, "ref not equal: name[%s] -> [%s]", refName.c_str(),
                        ref->toDebugString().c_str());
            }
            return false;
        }
    }
    return true;
}

void MatchDocAllocator::mergeDocs(MatchDocAllocator *other,
                                  const std::vector<MatchDoc> &matchDocs,
                                  std::vector<MatchDoc> &newMatchDocs)
{
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
                                   std::vector<MatchDoc> &newMatchDocs)
{
    std::vector<std::pair<FieldGroup *, FieldGroup *>> appendGroups;
    auto &otherFieldGroups = other->_fieldGroups;
    for (const auto &groupPair : _fieldGroups) {
        const string& groupName = groupPair.first;
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

bool MatchDocAllocator::copyGroupDocFields(
        const vector<MatchDoc> &matchDocs,
        vector<MatchDoc> &newMatchDocs,
        const vector<FieldGroup *> &copyGroups,
        MatchDocAllocator *other, bool genNewDoc)
{
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

FieldGroup *MatchDocAllocator::getReferenceGroup(MatchDocAllocator *allocator,
        const std::string &refName)
{
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

uint32_t MatchDocAllocator::getReferenceCount() const {
    return _fastReferenceMap.size();
}

uint32_t MatchDocAllocator::getSubReferenceCount() const {
    if (_subAllocator) {
        return _subAllocator->getReferenceCount();
    }
    return 0;
}

void MatchDocAllocator::initSub() {
    _subAllocator = new MatchDocAllocator(_poolPtr, _sessionPool, false, _mountInfo);
    _firstSub = declare<MatchDoc>(FIRST_SUB_REF, DEFAULT_GROUP);
    _currentSub = declare<MatchDoc>(CURRENT_SUB_REF, DEFAULT_GROUP);
    _nextSub = _subAllocator->declare<MatchDoc>(NEXT_SUB_REF, DEFAULT_GROUP);
    _subAccessor = new SubDocAccessor(_subAllocator,
                                  _firstSub, _currentSub, _nextSub);
}

FieldGroup* MatchDocAllocator::createFieldGroup(const string& groupName) {
    FieldGroup *fieldGroup = new FieldGroup(_sessionPool, _referenceTypesWrapper, groupName);
    fieldGroup->setPoolPtr(_poolPtr);
    _toExtendfieldGroups[groupName] = fieldGroup;
    return fieldGroup;
}

FieldGroup *MatchDocAllocator::createFieldGroupUnattached() {
    auto groupName = getUniqueGroupName();
    FieldGroup *fieldGroup = new FieldGroup(_sessionPool, _referenceTypesWrapper, groupName);
    fieldGroup->setPoolPtr(_poolPtr);
    return fieldGroup;
}

bool MatchDocAllocator::attachAndExtendFieldGroup(FieldGroup *fieldGroup, uint8_t serializeLevel, bool needConstruct) {
    auto groupName = fieldGroup->getGroupName();
    autil::ScopedSpinLock lock{_lock};
    if (_fieldGroups.find(groupName) != _fieldGroups.end() ||
        _toExtendfieldGroups.find(groupName) != _toExtendfieldGroups.end()) {
        return false;
    }
    for (auto &ref : fieldGroup->getReferenceVec()) {
        if (findReferenceByName(ref->getName())) {
            return false;
        }
    }
    for (auto &ref : fieldGroup->getReferenceVec()) {
        ref->setSerializeLevel(serializeLevel);
        _fastReferenceMap[ref->getName()] = ref;
    }
    extendGroup(fieldGroup, needConstruct);
    _fieldGroups[groupName] = fieldGroup;
    return true;
}

FieldGroup* MatchDocAllocator::getOrCreateToExtendFieldGroup(const std::string &fieldGroupName) {
    const auto it = _toExtendfieldGroups.find(fieldGroupName);
    if (_toExtendfieldGroups.end() == it) {
        return createFieldGroup(fieldGroupName);
    } else {
        return it->second;
    }
}

const MatchDocAllocator::FieldGroups& MatchDocAllocator::getFieldGroups() const
{
    return _fieldGroups;
}

ReferenceBase* MatchDocAllocator::cloneReference(const ReferenceBase *reference,
        const std::string &name, const std::string &groupName, const bool cloneWithMount)
{
    // already declared
    ReferenceBase *ref = findReferenceWithoutType(name);
    if (ref) {
        AUTIL_LOG(ERROR, "duplicate reference name: %s", name.c_str());
        return NULL;
    }
    if (reference->isSubDocReference() && _subAllocator != nullptr) {
        ReferenceBase *cloneRef = _subAllocator->cloneReference(reference, name, groupName, cloneWithMount);
        cloneRef->setCurrentRef(_currentSub);
        _fastReferenceMap[name] = cloneRef;
        return cloneRef;
    }
    // declare in allocated FieldGroup
    FieldGroups::const_iterator it = _fieldGroups.find(groupName);
    if (_fieldGroups.end() != it) {
        AUTIL_LOG(ERROR, "clone in allocated FieldGroup: %s", groupName.c_str());
        return NULL;
    }

    FieldGroup *fieldGroup = getOrCreateToExtendFieldGroup(groupName);
    if (cloneWithMount && reference->isMount()) {
        const auto mountMeta = _mountInfo->get(reference->getName());
        if (mountMeta) {
            ref = fieldGroup->cloneMountedReference(reference, name, mountMeta->mountId);
            _fastReferenceMap[name] = ref;
            return ref;
        } else {
            AUTIL_LOG(WARN, "clone mounted reference [%s] but found no mount meta!", name.c_str());
        }
    }

    ref = fieldGroup->cloneReference(reference, name);
    _fastReferenceMap[name] = ref;
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
    if (_toExtendfieldGroups.empty()) {
        for (;; ++_defaultGroupNameCounter) {
            string groupName = "_default_" + autil::StringUtil::toString(_defaultGroupNameCounter) + "_";
            if (_fieldGroups.find(groupName) == _fieldGroups.end()) {
                return groupName;
            }
        }
    } else {
        return _toExtendfieldGroups.begin()->first;
    }
}

std::string MatchDocAllocator::getUniqueGroupName() {
    autil::ScopedSpinLock lock(_lock);
    while (1) {
        string groupName = "_uniq_" + autil::StringUtil::toString(_defaultGroupNameCounter++) + "_";
        if (_fieldGroups.find(groupName) == _fieldGroups.end() &&
            _toExtendfieldGroups.find(groupName) == _toExtendfieldGroups.end()) {
            return groupName;
        }
    }
}

void MatchDocAllocator::registerType(const ReferenceBase *refType) {
    _referenceTypesWrapper->registerType(refType);
}

void MatchDocAllocator::registerTypes(MatchDocAllocator *other) {
    registerTypes(other->getExtraType());
    if (other->_subAllocator) {
        registerTypes(other->_subAllocator->getExtraType());
    }
}

void MatchDocAllocator::registerTypes(const ReferenceTypes *refTypes) {
    if (likely(refTypes == nullptr)) {
        return;
    }
    for (ReferenceTypes::const_iterator it = refTypes->begin();
         it != refTypes->end(); ++it)
    {
        registerType(it->second.first);
    }
}

void MatchDocAllocator::check() const {
    FieldGroups::const_iterator it = _fieldGroups.begin();
    FieldGroups::const_iterator itEnd = _fieldGroups.end();
    for (; it != itEnd; ++it) {
        it->second->check();
    }

    if (_subAllocator) {
        _subAllocator->check();
    }
}

bool MatchDocAllocator::isSame(const MatchDocAllocator &other) const {
    if (this == &other) {
        return true;
    }
    if (!isSame(_fieldGroups, other._fieldGroups)) {
        AUTIL_LOG(WARN, "fieldGroups not same");
        return false;
    }
    if (!isSame(_toExtendfieldGroups, other._toExtendfieldGroups)) {
        AUTIL_LOG(WARN, "toExtendFieldGroups not same");
        return false;
    }
    if ((_subAllocator == NULL && other._subAllocator != NULL)
        || (_subAllocator != NULL && other._subAllocator == NULL))
    {
        AUTIL_LOG(WARN, "sub allocate not same");
        return false;
    }
    return _subAllocator ? _subAllocator->isSame(*(other._subAllocator)) : true;
}

bool MatchDocAllocator::isSame(const FieldGroups &src, const FieldGroups &dst) const {
    if (src.size() != dst.size()) {
        AUTIL_LOG(WARN, "field group count not same, src[%lu], dst[%lu]",
                  src.size(), dst.size());
        return false;
    }
    for (auto it1 = src.begin(), it2 = dst.begin();
         it1 != src.end() && it2 != dst.end();
         it1++, it2++)
    {
        if (it1->first != it2->first) {
            AUTIL_LOG(WARN, "group name not equal, src[%s], dst[%s]",
                      it1->first.c_str(), it2->first.c_str());
            return false;
        }
        FieldGroup *g1 = it1->second;
        FieldGroup *g2 = it2->second;
        if ((g1 == NULL && g2 != NULL) || (g1 != NULL && g2 == NULL)) {
            return false;
        }
        if (g1 != NULL && !g1->isSame(*g2)) {
            AUTIL_LOG(WARN, "group[%s] not equal", it1->first.c_str());
            return false;
        }
    }
    return true;
}

ReferenceNameSet MatchDocAllocator::getAllReferenceNames() const {
    autil::ScopedSpinLock lock(_lock);
    unordered_set<string> names;
    for (auto it = _fieldGroups.begin(); it != _fieldGroups.end(); it++) {
        it->second->getAllReferenceNames(names);
    }
    for (auto it = _toExtendfieldGroups.begin(); it != _toExtendfieldGroups.end(); it++) {
        it->second->getAllReferenceNames(names);
    }
    if (_subAllocator) {
        const auto &subNames = _subAllocator->getAllReferenceNames();
        names.insert(subNames.begin(), subNames.end());
    }
    return names;
}

ReferenceVector MatchDocAllocator::getAllNeedSerializeReferences(uint8_t serializeLevel) const {
    ReferenceVector refVec;
    for (auto it = _fieldGroups.begin(); it != _fieldGroups.end(); it++) {
        ReferenceVector groupRefVec;
        it->second->getAllSerializeElements(groupRefVec, serializeLevel);
        refVec.insert(refVec.end(), groupRefVec.begin(), groupRefVec.end());
    }
    for (auto it = _toExtendfieldGroups.begin(); it != _toExtendfieldGroups.end(); it++) {
        ReferenceVector groupRefVec;
        it->second->getAllSerializeElements(groupRefVec, serializeLevel);
        refVec.insert(refVec.end(), groupRefVec.begin(), groupRefVec.end());
    }
    return refVec;
}

pair<FieldGroup*, bool> MatchDocAllocator::findGroup(const string &groupName) {
    auto it = _fieldGroups.find(groupName);
    if (it != _fieldGroups.end()) {
        return make_pair(it->second, true);
    }
    it = _toExtendfieldGroups.find(groupName);
    if (it != _toExtendfieldGroups.end()) {
        return make_pair(it->second, false);
    }
    return make_pair(nullptr, false);
}

ReferenceVector MatchDocAllocator::getAllSubNeedSerializeReferences(uint8_t serializeLevel) const {
    if(!_subAllocator) {
        return ReferenceVector();
    }
    return _subAllocator->getAllNeedSerializeReferences(serializeLevel);
}

ReferenceVector MatchDocAllocator::getRefBySerializeLevel(uint8_t serializeLevel) const {
    ReferenceVector refVec;
    for (auto it = _fieldGroups.begin(); it != _fieldGroups.end(); it++) {
        ReferenceVector groupRefVec;
        it->second->getRefBySerializeLevel(groupRefVec, serializeLevel);
        refVec.insert(refVec.end(), groupRefVec.begin(), groupRefVec.end());
    }
    for (auto it = _toExtendfieldGroups.begin(); it != _toExtendfieldGroups.end(); it++) {
        ReferenceVector groupRefVec;
        it->second->getRefBySerializeLevel(groupRefVec, serializeLevel);
        refVec.insert(refVec.end(), groupRefVec.begin(), groupRefVec.end());
    }
    return refVec;
}

FieldGroup *MatchDocAllocator::getFieldGroup(
    const std::string &groupName, const MountMeta *mountMeta) {
    // declare in allocated FieldGroup
    FieldGroups::const_iterator it = _fieldGroups.find(groupName);
    if (_fieldGroups.end() != it) {
        if (mountMeta && it->second->isMounted(mountMeta)) {
            return it->second;
        }
        AUTIL_LOG(WARN, "can't declare in allocated group [%s]", groupName.c_str());
        return nullptr;
    }
    it = _toExtendfieldGroups.find(groupName);
    FieldGroup *fieldGroup = nullptr;
    if (_toExtendfieldGroups.end() == it) {
        fieldGroup = createFieldGroup(groupName);
    } else {
        fieldGroup = it->second;
    }
    return fieldGroup;
}

FieldGroup *MatchDocAllocator::createIfNotExist(
        const std::string &groupName,
        const MountMeta* mountMeta)
{
    auto findResult = findGroup(groupName);
    if (findResult.first != nullptr) {
        if (findResult.second) {
            // in an allocated group
            if (mountMeta && findResult.first->isMounted(mountMeta)) {
                return findResult.first;
            } else {
                AUTIL_LOG(WARN, "can't declare in allocated group [%s]", groupName.c_str());
                return nullptr;
            }
        } else {
            return findResult.first;
        }
    } else {
        return createFieldGroup(groupName);
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

std::string MatchDocAllocator::toDebugString(uint8_t serializeLevel,
        const std::vector<MatchDoc> &matchDocs, bool onlySize) const
{
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

MatchDocAllocator *MatchDocAllocator::cloneAllocatorWithoutData() {
    DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, _sessionPool);
    setSortRefFlag(false);
    this->serializeMeta(dataBuffer, 0);
    auto allocator = new MatchDocAllocator(_poolPtr, _sessionPool);
    allocator->registerTypes(this);
    allocator->deserializeMeta(dataBuffer, NULL);
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

bool MatchDocAllocator::swapDocStorage(MatchDocAllocator &other)
{
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
         it1++, it2++)
    {
        it1->second->swapDocStorage(*it2->second);
    }
    swap(_size, other._size);
    swap(_capacity, other._capacity);
    swap(_sessionPool, other._sessionPool);
    swap(_poolPtr, other._poolPtr);
    _deletedIds.swap(other._deletedIds);
    if (_subAllocator) {
        _subAllocator->swapDocStorage(*other._subAllocator);
    }
    return true;
}

class MatchDocSeeker {
public:
    MatchDocSeeker(std::vector<MatchDoc> &docs)
        : _docs(docs)
        , _current(0)
    {
        _newSize = VectorDocStorage::getAlignSize(_docs.size());
        _docIndex.reserve(_docs.size());
        for (uint32_t i = 0; i < _docs.size(); i++) {
            _docIndex.push_back(i);
        }
        sort(_docIndex.begin(), _docIndex.end(),
             [this](uint32_t a, uint32_t b) {
                 return _docs[a].getIndex() < _docs[b].getIndex();
             });
        _docIter = _docIndex.begin();
    }
    inline uint32_t seek(uint32_t &last) {
        if (last < _current) {
            return _current;
        }
        while (_docIter != _docIndex.end() && last > getDocIndex(_docs, *_docIter)) {
            _docIter++;
        }
        if (_docIter != _docIndex.end()) {
            _current = getDocIndex(_docs, *_docIter);
        } else {
            _current = std::numeric_limits<uint32_t>::max();
        }
        return _current;
    }
    uint32_t getNewSize() const {
        return _newSize;
    }
    void getMoveBegin(uint32_t &begin, uint32_t &lastDelete) {
        begin = _docs.size();
        lastDelete = 0;
        for (uint32_t i = 0; i < _docs.size(); i++) {
            auto current = getDocIndex(_docs, _docIndex[i]);
            if (current >= _newSize) {
                begin = i;
                break;
            } else {
                lastDelete = current;
            }
        }
    }
    MatchDoc &getDoc(uint32_t moveIndex) {
        return _docs[_docIndex[moveIndex]];
    }
private:
    uint32_t getDocIndex(const std::vector<MatchDoc> &docs, size_t offset) {
        return docs[offset].getIndex();
    }
private:
    uint32_t _newSize;
    std::vector<uint32_t> _docIndex;
    std::vector<uint32_t>::const_iterator _docIter;
    std::vector<MatchDoc> &_docs;
    uint32_t _current;
};

class DeleteMapSeeker {
public:
    DeleteMapSeeker(std::vector<uint32_t> &deletedIds)
        : _deletedIds(deletedIds)
        , _current(0)
    {
        std::sort(_deletedIds.begin(), _deletedIds.end());
        _deleteIter = _deletedIds.begin();
    }
    inline uint32_t seek(uint32_t last) {
        if (last < _current) {
            return _current;
        }
        while (_deleteIter != _deletedIds.end() && last > *_deleteIter) {
            _deleteIter++;
        }
        if (_deleteIter != _deletedIds.end()) {
            _current = *_deleteIter;
        } else {
            _current = std::numeric_limits<uint32_t>::max();
        }
        return _current;
    }
private:
    std::vector<uint32_t>::const_iterator _deleteIter;
    std::vector<uint32_t> &_deletedIds;
    uint32_t _current;
};

void MatchDocAllocator::cloneDoc(uint32_t from, uint32_t to) {
    MatchDoc src(from);
    MatchDoc dest(to);
    for (auto it = _fieldGroups.begin(); it != _fieldGroups.end(); it++) {
        it->second->cloneDoc(dest, src);
    }
    if (_firstSub) {
        _firstSub->set(src, INVALID_MATCHDOC);
    }
}

void MatchDocAllocator::compact(std::vector<MatchDoc> &docs) {
    extend();
    MatchDocSeeker docSeeker(docs);
    DeleteMapSeeker deleteSeeker(_deletedIds);
    uint32_t newSize = docSeeker.getNewSize();
    uint32_t moveIndex = 0;
    uint32_t lastDelete = 0;
    docSeeker.getMoveBegin(moveIndex, lastDelete);
    std::vector<uint32_t> newDeletedIds;
    newDeletedIds.reserve(newSize - docs.size());
    uint32_t last = 0;
    for (; last < _size; last++) {
        while (last == docSeeker.seek(last)) {
            last++;
        }
        if (last >= _size) {
            break;
        }
        if (last != deleteSeeker.seek(last)) {
            _deallocate(MatchDoc(last));
        }
        if (moveIndex < docs.size()) {
            auto &moveDoc = docSeeker.getDoc(moveIndex);
            cloneDoc(moveDoc.getIndex(), last);
            moveDoc = MatchDoc(last, moveDoc.getDocId());
            moveIndex++;
        } else {
            if (last < lastDelete) {
                newDeletedIds.push_back(last);
            } else {
                break;
            }
        }
    }
    vector<MatchDoc> todelDocs;
    todelDocs.reserve(_size - last);
    for (; last < _size; last++) {
        if (last != deleteSeeker.seek(last)) {
            todelDocs.emplace_back(last);
        }
    }
    if (todelDocs.size() > 0) {
        _deallocate(todelDocs.data(), todelDocs.size());
    }
    std::swap(newDeletedIds, _deletedIds);
    for (const auto &pair : _fieldGroups) {
        pair.second->truncateGroup(newSize);
    }
    _capacity = newSize;
    _size = docs.size() + _deletedIds.size();

    if (_subAllocator) {
        std::vector<MatchDoc> subMatchDocs;
        subMatchDocs.reserve(_subAllocator->_size -
                             _subAllocator->_deletedIds.size());
        for (auto doc : docs) {
            _subAccessor->getSubDocs(doc, subMatchDocs);
        }
        _subAllocator->compact(subMatchDocs);
        size_t beginPos = 0;
        for (auto doc : docs) {
            _subAccessor->setSubDocs(doc, subMatchDocs, beginPos);
        }
    }
}

bool MatchDocAllocator::validate() {
    if (_size > _capacity) {
        return false;
    }
    for (const auto &pair : _fieldGroups) {
        if (_capacity != pair.second->getColomnSize()) {
            return false;
        }
    }
    return true;
}

ReferenceMap MatchDocAllocator::getAllNeedSerializeReferenceMap(uint8_t serializeLevel) const {
    ReferenceMap refmap;
    for (auto it = _fieldGroups.begin(); it != _fieldGroups.end(); it++) {
        it->second->getAllSerializeElements(refmap, serializeLevel);
    }
    for (auto it = _toExtendfieldGroups.begin(); it != _toExtendfieldGroups.end(); it++) {
        it->second->getAllSerializeElements(refmap, serializeLevel);
    }
    return refmap;
}

}
