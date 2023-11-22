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
#pragma once

#include <assert.h>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/MurmurHash.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/PoolVector.h"
#include "matchdoc/FieldGroup.h"
#include "matchdoc/FieldGroupBuilder.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MountInfo.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ReferenceTypesWrapper.h"
#include "matchdoc/Trait.h"

namespace autil {
class DataBuffer;
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace matchdoc {

class FieldGroup;
class SubDocAccessor;

extern const std::string BUILD_IN_REF_GROUP;
extern const std::string FIRST_SUB_REF;
extern const std::string CURRENT_SUB_REF;
extern const std::string NEXT_SUB_REF;
extern const std::string DEFAULT_GROUP;

constexpr uint8_t DEFAULT_SERIALIZE_LEVEL = 1;

class MatchDocAllocator {
public:
    // alias->refName
    typedef std::unordered_map<std::string, std::string> ReferenceAliasMap;
    typedef std::shared_ptr<ReferenceAliasMap> ReferenceAliasMapPtr;

public:
    MatchDocAllocator(autil::mem_pool::Pool *pool, bool useSub = false, const MountInfoPtr &mountInfo = MountInfoPtr());
    MatchDocAllocator(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr,
                      bool useSub = false,
                      const MountInfoPtr &mountInfo = MountInfoPtr());
    virtual ~MatchDocAllocator();

private:
    MatchDocAllocator(const MatchDocAllocator &) = delete;
    MatchDocAllocator &operator=(const MatchDocAllocator &) = delete;

public:
    bool mergeMountInfo(const MountInfoPtr &mountInfo);
    bool mergeMountInfo(const MountInfoPtr &mountInfo, const std::set<std::string> &mergeFields);
    bool mergeMountInfo(const MatchDocAllocator *allocator);
    bool mergeMountInfo(const MatchDocAllocator *allocator, const std::set<std::string> &mergeFields);

    // TODO: auto extend
    uint32_t getAllocatedCount() const;
    uint32_t getCapacity() const;
    const std::vector<uint32_t> &getDeletedIds() const;

    // auto assign group
    template <typename T>
    Reference<T> *declare(const std::string &name, uint8_t serializeLevel = 0, uint32_t allocateSize = sizeof(T));

    // declare reference need no construct
    template <typename T>
    Reference<T> *declareWithConstructFlagDefaultGroup(const std::string &name,
                                                       bool needConstruct = true,
                                                       uint8_t serializeLevel = 0,
                                                       uint32_t allocateSize = sizeof(T));

    template <typename T>
    Reference<T> *declareSub(const std::string &name, uint8_t serializeLevel = 0, uint32_t allocateSize = sizeof(T));

    template <typename T>
    Reference<T> *declareSubWithConstructFlagDefaultGroup(const std::string &name,
                                                          bool needConstruct = true,
                                                          uint8_t serializeLevel = 0,
                                                          uint32_t allocateSize = sizeof(T));

    // by group name
    template <typename T>
    Reference<T> *declare(const std::string &name,
                          const std::string &groupName,
                          uint8_t serializeLevel = 0,
                          uint32_t allocateSize = sizeof(T));

    template <typename T>
    Reference<T> *declareWithConstructFlag(const std::string &name,
                                           const std::string &groupName,
                                           bool needConstruct = true,
                                           uint8_t serializeLevel = 0,
                                           uint32_t allocateSize = sizeof(T));

    template <typename T>
    Reference<T> *declareSub(const std::string &name,
                             const std::string &groupName,
                             uint8_t serializeLevel = 0,
                             uint32_t allocateSize = sizeof(T));

    template <typename T>
    Reference<T> *declareSubWithConstructFlag(const std::string &name,
                                              const std::string &groupName,
                                              bool needConstruct = true,
                                              uint8_t serializeLevel = 0,
                                              uint32_t allocateSize = sizeof(T));

    bool addGroup(std::unique_ptr<FieldGroup> group, bool genRefIndex = true, bool construct = true);

    void dropField(const std::string &name, bool dropMountMetaIfExist = false);
    void reserveFields(uint8_t serializeLevel, const std::unordered_set<std::string> &extraFields = {});
    bool renameField(const std::string &srcName, const std::string &dstName);

    template <typename T>
    Reference<T> *findReference(const std::string &name) const;
    ReferenceBase *findReferenceWithoutType(const std::string &name) const;

    MatchDoc allocate(int32_t docid = -1);
    // if set adviseNotConstruct = true, not construct docs when reference need no destruct, other references still
    // construct to avoid core
    std::vector<matchdoc::MatchDoc> batchAllocate(const std::vector<int32_t> &docIds, bool adviseNotConstruct = false);
    std::vector<matchdoc::MatchDoc> batchAllocate(size_t count, int32_t docid = -1, bool adviseNotConstruct = false);
    void batchAllocate(std::vector<matchdoc::MatchDoc> &matchDocs,
                       size_t count,
                       int32_t docid = -1,
                       bool adviseNotConstruct = false);
    void batchAllocate(size_t count,
                       int32_t docid,
                       std::vector<matchdoc::MatchDoc> &retVec,
                       bool adviseNotConstruct = false);
    void batchAllocate(const std::vector<int32_t> &docIds,
                       std::vector<matchdoc::MatchDoc> &retVec,
                       bool adviseNotConstruct = false);

    matchdoc::MatchDoc
    batchAllocateSubdoc(size_t count, std::vector<matchdoc::MatchDoc> &subdocVec, int32_t docid = -1);
    MatchDoc allocateAndClone(const MatchDoc &cloneDoc);
    MatchDoc allocateAndCloneWithSub(const MatchDoc &cloneDoc);

    MatchDoc allocateSub(const MatchDoc &parent, int32_t docid = -1);
    MatchDoc &allocateSubReturnRef(const MatchDoc &parent, int32_t docid = -1);

    template <typename DocIdIterator>
    MatchDoc allocateWithSubDoc(DocIdIterator *iterator);

    // use batchAllocateSubdoc first with un-initialized docid vector
    template <typename DocIdIterator>
    void fillDocidWithSubDoc(DocIdIterator *iterator, MatchDoc &matchDoc, std::vector<matchdoc::MatchDoc> &subdocVec);

    // clone match doc from other allocator
    // make sure this->isSame(other)
    MatchDoc allocateAndClone(MatchDocAllocator *other, const MatchDoc &matchDoc);

    void setReferenceAliasMap(const ReferenceAliasMapPtr &aliasMap) { _aliasMap = aliasMap; }

    void deallocate(const MatchDoc &doc);
    void deallocate(const MatchDoc *docs, uint32_t count);
    void extend();
    void extendSub();
    void extendGroup(const std::string &group);
    void extendSubGroup(const std::string &group);

    Reference<MatchDoc> *getCurrentSubDocRef() const { return _currentSub; }

    bool hasSubDocAllocator() const { return _subAccessor != nullptr; }

    SubDocAccessor *getSubDocAccessor() const { return _subAccessor; }

    void serialize(autil::DataBuffer &dataBuffer,
                   const std::vector<MatchDoc> &matchdocs,
                   uint8_t serializeLevel = DEFAULT_SERIALIZE_LEVEL,
                   bool ignoreUselessGroupMeta = false);
    void serialize(autil::DataBuffer &dataBuffer,
                   const autil::mem_pool::PoolVector<MatchDoc> &matchdocs,
                   uint8_t serializeLevel = DEFAULT_SERIALIZE_LEVEL,
                   bool ignoreUselessGroupMeta = false);
    template <typename T>
    void serializeImpl(autil::DataBuffer &dataBuffer,
                       const T &matchdocs,
                       uint8_t serializeLevel = DEFAULT_SERIALIZE_LEVEL,
                       bool ignoreUselessGroupMeta = false);
    void deserialize(autil::DataBuffer &dataBuffer, std::vector<MatchDoc> &matchdocs);
    void deserialize(autil::DataBuffer &dataBuffer, autil::mem_pool::PoolVector<MatchDoc> &matchdocs);
    template <typename T>
    void deserializeImpl(autil::DataBuffer &dataBuffer, T &matchdocs);

    bool deserializeAndAppend(autil::DataBuffer &dataBuffer, std::vector<MatchDoc> &matchdocs);
    bool deserializeAndAppend(autil::DataBuffer &dataBuffer, autil::mem_pool::PoolVector<MatchDoc> &matchdocs);
    template <typename T>
    bool deserializeAndAppendImpl(autil::DataBuffer &dataBuffer, T &matchDocs);

    void serializeMeta(autil::DataBuffer &dataBuffer, uint8_t serializeLevel, bool ignoreUselessGroupMeta = false);
    void deserializeMeta(autil::DataBuffer &dataBuffer, Reference<MatchDoc> *currentSub);
    bool canMerge(MatchDocAllocator *other) const;
    bool mergeAllocatorOnlySame(MatchDocAllocator *other,
                                const std::vector<MatchDoc> &matchDocs,
                                std::vector<MatchDoc> &newMatchDocs);
    bool mergeAllocator(MatchDocAllocator *other,
                        const std::vector<MatchDoc> &matchDocs,
                        std::vector<MatchDoc> &newMatchDocs);
    uint32_t getDocSize() const;
    uint32_t getSubDocSize() const;
    uint32_t getReferenceCount() const;
    uint32_t getSubReferenceCount() const;

    void clean(const std::unordered_set<std::string> &keepFieldGroups);

    bool isSame(const MatchDocAllocator &other) const;

    ReferenceNameSet getAllReferenceNames() const;
    ReferenceVector getAllNeedSerializeReferences(uint8_t serializeLevel) const;
    ReferenceMap getAllNeedSerializeReferenceMap(uint8_t serializeLevel) const;
    ReferenceVector getAllSubNeedSerializeReferences(uint8_t serializeLevel) const;
    ReferenceVector getRefBySerializeLevel(uint8_t serializeLevel) const;
    const std::unordered_map<std::string, ReferenceBase *> &getFastReferenceMap() const { return _referenceMap; }
    std::string toDebugString() const;
    std::string toDebugString(uint8_t serializeLevel, const std::vector<MatchDoc> &matchDocs, bool onlySize) const;
    std::string getDefaultGroupName();
    void setDefaultGroupName(const std::string &name) { _defaultGroupName = name; }
    void setSortRefFlag(bool sortRefFlag) {
        _sortRefFlag = sortRefFlag;
        if (_subAllocator) {
            _subAllocator->setSortRefFlag(sortRefFlag);
        }
    }
    void truncateSubDoc(MatchDoc parent, MatchDoc truncPointSubDoc) const;
    void deallocateSubDoc(MatchDoc subdoc) const;
    matchdoc::Reference<matchdoc::MatchDoc> *getFirstSubDocRef() const { return _firstSub; }
    matchdoc::Reference<matchdoc::MatchDoc> *getSubDocRef() const { return _currentSub; }
    MatchDocAllocator *cloneAllocatorWithoutData(const std::shared_ptr<autil::mem_pool::Pool> &pool = {});
    bool
    swapDocStorage(MatchDocAllocator &other, std::vector<MatchDoc> &otherMatchDocs, std::vector<MatchDoc> &myMatchDocs);
    bool swapDocStorage(MatchDocAllocator &other);

public:
    // make sure FieldGroup is ordered, serialize use this to cal signature
    typedef std::map<std::string, FieldGroup *> FieldGroups;

public: // for MatchDocJoiner
    const FieldGroups &getFieldGroups() const;
    FieldGroup *findGroup(const std::string &name) const {
        auto it = _fieldGroups.find(name);
        return it == _fieldGroups.end() ? nullptr : it->second;
    }
    ReferenceBase *cloneReference(const ReferenceBase *reference,
                                  const std::string &name,
                                  const std::string &groupName = DEFAULT_GROUP,
                                  const bool cloneWithMount = false);
    const std::string &getReferenceName(const std::string &name) const {
        if (!_aliasMap) {
            return name;
        }
        ReferenceAliasMap::const_iterator iter = _aliasMap->find(name);
        if (iter != _aliasMap->end()) {
            return iter->second;
        }
        return name;
    }
    const ReferenceAliasMapPtr &getReferenceAliasMap() const { return _aliasMap; }
    autil::mem_pool::Pool *getSessionPool() const { return _poolPtr.get(); }

    autil::mem_pool::Pool *getPool() const { return _poolPtr.get(); }
    std::shared_ptr<autil::mem_pool::Pool> getPoolPtr() const { return _poolPtr; }

public:
    static std::unique_ptr<MatchDocAllocator> fromFieldGroups(std::shared_ptr<autil::mem_pool::Pool> pool,
                                                              std::vector<std::unique_ptr<FieldGroup>> fieldGroups,
                                                              uint32_t size,
                                                              uint32_t capacity);

private:
    template <typename T>
    Reference<T> *declareInner(const std::string &name,
                               const std::string &groupName,
                               const MountMeta *mountMeta,
                               bool needConstruct,
                               uint8_t serializeLevel,
                               uint32_t allocateSize);

    bool addGroupLocked(std::unique_ptr<FieldGroup> group, bool checkRef = true, bool construct = true);

    std::string getDefaultGroupNameInner();

    ReferenceBase *findReferenceByName(const std::string &name) const;
    ReferenceBase *findMainReference(const std::string &name) const;
    ReferenceBase *findSubReference(const std::string &name) const;

    const MountInfoPtr &getMountInfo() const { return _mountInfo; }

private:
    MatchDoc getOne();
    void grow();
    void extendGroup(FieldGroup *fieldGroup, bool needConstruct = true);
    void initSub();
    MatchDoc &addSub(const MatchDoc &parent, const MatchDoc &doc);

    template <typename DocIdGenerator>
    void innerBatchAllocate(DocIdGenerator &docIdGenerator,
                            std::vector<matchdoc::MatchDoc> &retVec,
                            bool adviseNotConstruct);

    void deallocateAllMatchDocs(ReferenceBase *ref = nullptr);
    void clearFieldGroups();
    void _deallocate(const MatchDoc &doc);
    void _deallocate(const MatchDoc *docs, uint32_t count);

    bool isSame(const FieldGroups &src, const FieldGroups &dst) const;
    void
    mergeDocs(MatchDocAllocator *other, const std::vector<MatchDoc> &matchDocs, std::vector<MatchDoc> &newMatchDocs);
    void
    appendDocs(MatchDocAllocator *other, const std::vector<MatchDoc> &matchDocs, std::vector<MatchDoc> &newMatchDocs);
    FieldGroupBuilder *findOrCreate(const std::string &name);

private:
    // subdoc serialize: docid use sub docid
    void
    serializeMatchDoc(autil::DataBuffer &dataBuffer, const MatchDoc &matchdoc, int32_t docid, uint8_t serializeLevel);

    MatchDoc deserializeMatchDoc(autil::DataBuffer &dataBuffer);
    void deserializeOneMatchDoc(autil::DataBuffer &dataBuffer, const MatchDoc &doc);

private:
    static FieldGroup *getReferenceGroup(MatchDocAllocator *allocator, const std::string &refName);
    bool copyGroupDocFields(const std::vector<MatchDoc> &matchdocs,
                            std::vector<MatchDoc> &newMatchDocs,
                            const std::vector<FieldGroup *> &copyGroups,
                            MatchDocAllocator *other,
                            bool genNewDoc);
    bool renameFieldImpl(const std::string &name, const std::string &dstName);

private:
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    uint32_t _size;
    uint32_t _capacity;
    // for deserialize
    int64_t _metaSignature;

    std::vector<uint32_t> _deletedIds;
    FieldGroups _fieldGroups;
    std::map<std::string, std::unique_ptr<FieldGroupBuilder>> _fieldGroupBuilders;
    MountInfoPtr _mountInfo;
    ReferenceAliasMapPtr _aliasMap;
    std::unordered_map<std::string, ReferenceBase *> _referenceMap;

public:
    mutable autil::SpinLock _lock;

protected:
    // sub
    MatchDocAllocator *_subAllocator;
    Reference<MatchDoc> *_firstSub;
    Reference<MatchDoc> *_currentSub;
    Reference<MatchDoc> *_nextSub;
    SubDocAccessor *_subAccessor;

private:
    std::string _defaultGroupName;
    size_t _defaultGroupNameCounter;

private:
    bool _merged;
    bool _mountInfoIsPrivate;
    bool _sortRefFlag;

private:
    friend class MatchDocAllocatorTest;
    friend class SubCloner;
    friend class MatchDocDefaultValueSetter;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MatchDocAllocator> MatchDocAllocatorPtr;

template <typename T>
Reference<T> *MatchDocAllocator::declare(const std::string &name, uint8_t serializeLevel, uint32_t allocateSize) {
    return declareWithConstructFlag<T>(name, getDefaultGroupName(), true, serializeLevel, allocateSize);
}

template <typename T>
Reference<T> *MatchDocAllocator::declareWithConstructFlagDefaultGroup(const std::string &name,
                                                                      bool needConstruct,
                                                                      uint8_t serializeLevel,
                                                                      uint32_t allocateSize) {
    return declareWithConstructFlag<T>(name, getDefaultGroupName(), needConstruct, serializeLevel, allocateSize);
}

template <typename T>
Reference<T> *MatchDocAllocator::declare(const std::string &name,
                                         const std::string &groupName,
                                         uint8_t serializeLevel,
                                         uint32_t allocateSize) {
    return declareWithConstructFlag<T>(name, groupName, true, serializeLevel, allocateSize);
}

template <typename T>
Reference<T> *MatchDocAllocator::declareWithConstructFlag(const std::string &name,
                                                          const std::string &groupName,
                                                          bool needConstruct,
                                                          uint8_t serializeLevel,
                                                          uint32_t allocateSize) {
    autil::ScopedSpinLock lock(_lock);
    const std::string &refName = getReferenceName(name);
    const MountMeta *mountMeta = nullptr;
    if (IsMountable<T>::value && _mountInfo) {
        mountMeta = _mountInfo->get(refName);
    }
    return declareInner<T>(refName, groupName, mountMeta, needConstruct, serializeLevel, allocateSize);
}

template <typename T>
Reference<T> *MatchDocAllocator::declareInner(const std::string &refName,
                                              const std::string &groupName,
                                              const MountMeta *mountMeta,
                                              bool needConstruct,
                                              uint8_t serializeLevel,
                                              uint32_t allocateSize) {
    // already declared
    ReferenceBase *ref = findReferenceByName(refName);
    if (ref) {
        Reference<T> *typedRef = dynamic_cast<Reference<T> *>(ref);
        if (!typedRef) {
            return nullptr;
        }
        if (serializeLevel > typedRef->getSerializeLevel()) {
            typedRef->setSerializeLevel(serializeLevel);
        }
        return typedRef;
    }

    if (_fieldGroups.count(groupName) > 0) {
        // already exists
        return nullptr;
    }
    auto builder = findOrCreate(groupName);
    Reference<T> *typedRef = nullptr;
    if (likely(mountMeta == nullptr)) {
        typedRef = builder->declare<T>(refName, needConstruct, allocateSize);
    } else {
        typedRef =
            builder->declareMount<T>(refName, mountMeta->mountId, mountMeta->mountOffset, needConstruct, allocateSize);
    }
    if (typedRef != nullptr) {
        typedRef->setSerializeLevel(serializeLevel);
        _referenceMap[refName] = typedRef;
    }
    return typedRef;
}

template <typename T>
Reference<T> *MatchDocAllocator::declareSub(const std::string &name, uint8_t serializeLevel, uint32_t allocateSize) {
    if (!_subAllocator) {
        initSub();
    }
    return declareSubWithConstructFlag<T>(
        name, _subAllocator->getDefaultGroupName(), true, serializeLevel, allocateSize);
}

template <typename T>
Reference<T> *MatchDocAllocator::declareSub(const std::string &name,
                                            const std::string &groupName,
                                            uint8_t serializeLevel,
                                            uint32_t allocateSize) {
    return declareSubWithConstructFlag<T>(name, groupName, true, serializeLevel, allocateSize);
}

template <typename T>
Reference<T> *MatchDocAllocator::declareSubWithConstructFlagDefaultGroup(const std::string &name,
                                                                         bool needConstruct,
                                                                         uint8_t serializeLevel,
                                                                         uint32_t allocateSize) {
    if (!_subAllocator) {
        initSub();
    }
    return declareSubWithConstructFlag<T>(
        name, _subAllocator->getDefaultGroupName(), needConstruct, serializeLevel, allocateSize);
}

template <typename T>
Reference<T> *MatchDocAllocator::declareSubWithConstructFlag(const std::string &name,
                                                             const std::string &groupName,
                                                             bool needConstruct,
                                                             uint8_t serializeLevel,
                                                             uint32_t allocateSize) {
    if (!_subAllocator) {
        initSub();
    }

    autil::ScopedSpinLock lock(_lock);
    const std::string &refName = getReferenceName(name);
    ReferenceBase *baseRef = findSubReference(refName);
    if (baseRef) {
        return dynamic_cast<Reference<T> *>(baseRef);
    }

    baseRef = findMainReference(refName);
    if (baseRef) {
        AUTIL_LOG(ERROR, "%s already declared in main allocator", refName.c_str());
        return nullptr;
    }

    Reference<T> *ref =
        _subAllocator->declareWithConstructFlag<T>(refName, groupName, needConstruct, serializeLevel, allocateSize);
    if (ref) {
        ref->setCurrentRef(_currentSub);
    }
    return ref;
}

template <typename T>
Reference<T> *MatchDocAllocator::findReference(const std::string &name) const {
    ReferenceBase *ref = findReferenceWithoutType(name);
    if (!ref) {
        return nullptr;
    }
    return dynamic_cast<Reference<T> *>(ref);
}

inline ReferenceBase *MatchDocAllocator::findReferenceWithoutType(const std::string &name) const {
    const std::string &refName = getReferenceName(name);
    autil::ScopedSpinLock lock(_lock);
    return findReferenceByName(refName);
}

template <class DocIdIterator>
inline MatchDoc MatchDocAllocator::allocateWithSubDoc(DocIdIterator *iterator) {
    int32_t docId = iterator->getCurDocId();
    MatchDoc matchDoc = allocate(docId);
    if (!hasSubDocAllocator()) {
        return matchDoc;
    }
    if (!iterator->beginSubDoc()) {
        return matchDoc;
    }
    while (true) {
        int32_t subDocId = iterator->nextSubDocId();
        if (unlikely(subDocId == -1)) {
            break;
        }
        allocateSub(matchDoc, subDocId);
    }
    return matchDoc;
}

template <typename DocIdIterator>
void MatchDocAllocator::fillDocidWithSubDoc(DocIdIterator *iterator,
                                            MatchDoc &matchDoc,
                                            std::vector<matchdoc::MatchDoc> &subdocVec) {
    int32_t docId = iterator->getCurDocId();
    matchDoc.setDocId(docId);
    if (!hasSubDocAllocator() || !iterator->beginSubDoc()) {
        return;
    }

    MatchDoc &first = _firstSub->getReference(matchDoc);
    MatchDoc &current = _currentSub->getReference(matchDoc);
    uint32_t index = 0;
    uint32_t subDocCount = subdocVec.size();
    while (index < subDocCount) {
        int32_t subDocId = iterator->nextSubDocId();
        if (unlikely(subDocId == -1)) {
            auto &nextSub = _nextSub->getReference(current);
            nextSub = INVALID_MATCHDOC;
            break;
        }
        MatchDoc &doc = subdocVec[index];
        doc.setDocId(subDocId);
        if (index == 0) {
            first = doc;
        } else {
            auto &nextSub = _nextSub->getReference(current);
            nextSub = doc;
        }
        current = doc;
        ++index;
    }
}

} // namespace matchdoc
