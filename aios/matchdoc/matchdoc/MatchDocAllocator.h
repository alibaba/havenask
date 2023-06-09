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
#ifndef ISEARCH_MATCHDOCALLOCATOR_H
#define ISEARCH_MATCHDOCALLOCATOR_H

#include "alog/Logger.h"
#include <assert.h>
#include "autil/CommonMacros.h"
#include "autil/MurmurHash.h"
#include "autil/mem_pool/PoolVector.h"
#include "autil/Log.h"
#include <stddef.h>
#include <stdint.h>
#include <map>
#include <unordered_set>
#include <string>
#include <memory>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "matchdoc/FieldGroup.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MountInfo.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"

namespace autil {
class DataBuffer;
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace matchdoc {

class FieldGroup;
class SubDocAccessor;

extern const std::string BUILD_IN_REF_GROUP;
extern const std::string FIRST_SUB_REF;
extern const std::string CURRENT_SUB_REF;
extern const std::string NEXT_SUB_REF;
extern const std::string DEFAULT_GROUP;

constexpr uint8_t DEFAULT_SERIALIZE_LEVEL = 1;
typedef std::unordered_set<std::string> MountableTypes;
class MountableTypesWrapper;


class MatchDocAllocator {
 public:
    // alias->refName
    typedef std::unordered_map<std::string, std::string> ReferenceAliasMap;
    typedef std::shared_ptr<ReferenceAliasMap> ReferenceAliasMapPtr;

public:
    MatchDocAllocator(autil::mem_pool::Pool *pool,
                      bool useSub = false,
                      const MountInfoPtr& mountInfo = MountInfoPtr());
    MatchDocAllocator(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr,
                      bool useSub = false,
                      const MountInfoPtr& mountInfo = MountInfoPtr());
    virtual ~MatchDocAllocator();
private:
    MatchDocAllocator(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr,
                      autil::mem_pool::Pool *pool,
                      bool useSub = false,
                      const MountInfoPtr& mountInfo = MountInfoPtr());
private:
    MatchDocAllocator(const MatchDocAllocator &) = delete;
    MatchDocAllocator& operator=(const MatchDocAllocator &) = delete;
public:
    bool mergeMountInfo(const MountInfoPtr &mountInfo);
    bool mergeMountInfo(const MountInfoPtr &mountInfo, const std::set<std::string> &mergeFields);
    bool mergeMountInfo(const MatchDocAllocator *allocator);
    bool mergeMountInfo(const MatchDocAllocator *allocator,
                        const std::set<std::string> &mergeFields);

    // TODO: auto extend
    uint32_t getAllocatedCount() const;

    // auto assign group
    template<typename T>
    Reference<T>* declare(const std::string &name,
                          uint8_t serializeLevel = 0,
                          uint32_t allocateSize = sizeof(T));

    // declare reference need no construct
    template<typename T>
    Reference<T>* declareWithConstructFlagDefaultGroup(const std::string &name,
                                           bool needConstruct = true,
                                           uint8_t serializeLevel = 0,
                                           uint32_t allocateSize = sizeof(T));

    template<typename T>
    Reference<T>* declareSub(const std::string &name,
                             uint8_t serializeLevel = 0,
                             uint32_t allocateSize = sizeof(T));

    template<typename T>
    Reference<T>* declareSubWithConstructFlagDefaultGroup(const std::string &name,
                                              bool needConstruct = true,
                                              uint8_t serializeLevel = 0,
                                              uint32_t allocateSize = sizeof(T));

    // by group name
    template<typename T>
    Reference<T>* declare(const std::string &name,
                          const std::string &groupName,
                          uint8_t serializeLevel = 0,
                          uint32_t allocateSize = sizeof(T));

    template<typename T>
    Reference<T>* declareWithConstructFlag(const std::string &name,
                            const std::string &groupName,
                            bool needConstruct = true,
                            uint8_t serializeLevel = 0,
                            uint32_t allocateSize = sizeof(T));

    template<typename T>
    Reference<T>* declareSub(const std::string &name,
                             const std::string &groupName,
                             uint8_t serializeLevel = 0,
                             uint32_t allocateSize = sizeof(T));

    template<typename T>
    Reference<T>*
    declareSubWithConstructFlag(const std::string &name,
                               const std::string &groupName,
                               bool needConstruct = true,
                               uint8_t serializeLevel = 0,
                               uint32_t allocateSize = sizeof(T));

    // mount group from external storage, such as vector/tensor...
    template <typename T>
    Reference<T>* createSingleFieldGroup(const std::string &name,
            T *data, uint32_t count, uint8_t serializeLevel = 0);

    void dropField(const std::string &name, bool dropMountMetaIfExist = false);
    void reserveFields(uint8_t serializeLevel);
    bool renameField(const std::string &srcName, const std::string &dstName);

    template<typename T>
    Reference<T>* findReference(const std::string &name) const;
    ReferenceBase* findReferenceWithoutType(const std::string &name) const;

    void cowMountedGroup();
    MatchDoc allocate(int32_t docid = -1);
    // if set adviseNotConstruct = true, not construct docs when reference need no destruct, other references still construct to avoid core
    std::vector<matchdoc::MatchDoc> batchAllocate(const std::vector<int32_t>& docIds, bool adviseNotConstruct = false);
    std::vector<matchdoc::MatchDoc> batchAllocate(size_t count, int32_t docid = -1, bool adviseNotConstruct = false);
    void batchAllocate(std::vector<matchdoc::MatchDoc>& matchDocs, size_t count, int32_t docid = -1, bool adviseNotConstruct = false);
    void batchAllocate(size_t count, int32_t docid, std::vector<matchdoc::MatchDoc> &retVec, bool adviseNotConstruct = false);
    void batchAllocate(const std::vector<int32_t>& docIds,
                       std::vector<matchdoc::MatchDoc> &retVec, bool adviseNotConstruct = false);

    matchdoc::MatchDoc batchAllocateSubdoc(size_t count,
            std::vector<matchdoc::MatchDoc> &subdocVec, int32_t docid = -1);
    MatchDoc allocateAndClone(const MatchDoc &cloneDoc);
    MatchDoc allocateAndCloneWithSub(const MatchDoc &cloneDoc);

    MatchDoc allocateSub(const MatchDoc &parent, int32_t docid = -1);
    MatchDoc &allocateSubReturnRef(const MatchDoc &parent, int32_t docid = -1);

    template <typename DocIdIterator>
    MatchDoc allocateWithSubDoc(DocIdIterator *iterator);

    // use batchAllocateSubdoc first with un-initialized docid vector
    template <typename DocIdIterator>
    void fillDocidWithSubDoc(DocIdIterator *iterator, MatchDoc &matchDoc,
                             std::vector<matchdoc::MatchDoc> &subdocVec);

    // clone match doc from other allocator
    // make sure this->isSame(other)
    MatchDoc allocateAndClone(MatchDocAllocator *other, const MatchDoc &matchDoc);

    void setReferenceAliasMap(const ReferenceAliasMapPtr& aliasMap) {
        _aliasMap = aliasMap;
    }

    void deallocate(const MatchDoc &doc);
    void deallocate(const MatchDoc *docs, uint32_t count);
    void extend();
    void extendSub();
    void extendGroup(const std::string &group);
    void extendSubGroup(const std::string &group);

    Reference<MatchDoc>* getCurrentSubDocRef() const
    { return _currentSub; }

    bool hasSubDocAllocator() const {
        return _subAccessor != NULL;
    }

    SubDocAccessor *getSubDocAccessor() const {
        return _subAccessor;
    }

    void serialize(autil::DataBuffer &dataBuffer,
                   const std::vector<MatchDoc> &matchdocs,
                   uint8_t serializeLevel = DEFAULT_SERIALIZE_LEVEL,
                   bool ignoreUselessGroupMeta = false);
    void serialize(autil::DataBuffer &dataBuffer,
                   const autil::mem_pool::PoolVector<MatchDoc> &matchdocs,
                   uint8_t serializeLevel = DEFAULT_SERIALIZE_LEVEL,
                   bool ignoreUselessGroupMeta = false);
    template<typename T>
    void serializeImpl(autil::DataBuffer &dataBuffer,
                       const T &matchdocs,
                       uint8_t serializeLevel = DEFAULT_SERIALIZE_LEVEL,
                       bool ignoreUselessGroupMeta = false);
    void deserialize(autil::DataBuffer &dataBuffer,
                     std::vector<MatchDoc> &matchdocs);
    void deserialize(autil::DataBuffer &dataBuffer,
                     autil::mem_pool::PoolVector<MatchDoc> &matchdocs);
    template<typename T>
    void deserializeImpl(autil::DataBuffer &dataBuffer, T &matchdocs);

    bool deserializeAndAppend(autil::DataBuffer &dataBuffer,
                              std::vector<MatchDoc> &matchdocs);
    bool deserializeAndAppend(autil::DataBuffer &dataBuffer,
                              autil::mem_pool::PoolVector<MatchDoc> &matchdocs);
    template<typename T>
    bool deserializeAndAppendImpl(autil::DataBuffer &dataBuffer,
                                  T &matchDocs);

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
    template<typename T>
    void registerType();
    void registerType(const ReferenceBase *refType);
    void registerTypes(MatchDocAllocator *other);
    void registerTypes(const ReferenceTypes *refTypes);
    inline const ReferenceTypes *getExtraType() {
        return _referenceTypesWrapper->getExtraType();
    }
    template<typename T>
    void registerMountableType();

    void clean(const std::unordered_set<std::string>& keepFieldGroups);

    bool isSame(const MatchDocAllocator &other) const;

    ReferenceNameSet getAllReferenceNames() const;
    ReferenceVector getAllNeedSerializeReferences(uint8_t serializeLevel) const;
    ReferenceMap getAllNeedSerializeReferenceMap(uint8_t serializeLevel) const;
    ReferenceVector getAllSubNeedSerializeReferences(uint8_t serializeLevel) const;
    ReferenceVector getRefBySerializeLevel(uint8_t serializeLevel) const;
    const std::unordered_map<std::string, ReferenceBase *> &getFastReferenceMap() const { return _fastReferenceMap;}
    std::string toDebugString() const;
    std::string toDebugString(uint8_t serializeLevel,
                              const std::vector<MatchDoc> &matchDocs,
                              bool onlySize) const;
    std::string getDefaultGroupName();
    std::string getUniqueGroupName();
    void setDefaultGroupName(const std::string &name) {
        _defaultGroupName = name;
    }
    void setSortRefFlag(bool sortRefFlag) {
        _sortRefFlag = sortRefFlag;
        if (_subAllocator) {
            _subAllocator->setSortRefFlag(sortRefFlag);
        }
    }
    void truncateSubDoc(MatchDoc parent, MatchDoc truncPointSubDoc) const;
    void deallocateSubDoc(MatchDoc subdoc) const;
    matchdoc::Reference<matchdoc::MatchDoc> *getFirstSubDocRef() const {
        return _firstSub;
    }
    matchdoc::Reference<matchdoc::MatchDoc> *getSubDocRef() const {
        return _currentSub;
    }
    MatchDocAllocator *cloneAllocatorWithoutData();
    bool swapDocStorage(MatchDocAllocator &other, std::vector<MatchDoc> &otherMatchDocs,
                        std::vector<MatchDoc> &myMatchDocs);
    bool swapDocStorage(MatchDocAllocator &other);
    void compact(std::vector<MatchDoc> &docs);

public:
    // make sure FieldGroup is ordered, serialize use this to cal signature
    typedef std::map<std::string, FieldGroup *> FieldGroups;

public: // for MatchDocJoiner
    const FieldGroups &getFieldGroups() const;
    ReferenceBase *cloneReference(const ReferenceBase *reference,
                                  const std::string &name,
                                  const std::string &groupName = DEFAULT_GROUP,
                                  const bool cloneWithMount = false);
    // for test
    void check() const;
    const std::string& getReferenceName(const std::string &name) const {
        if (!_aliasMap) {
            return name;
        }
        ReferenceAliasMap::const_iterator iter = _aliasMap->find(name);
        if (iter != _aliasMap->end()) {
            return iter->second;
        }
        return name;
    }
    const ReferenceAliasMapPtr& getReferenceAliasMap() const {
        return _aliasMap;
    }
    autil::mem_pool::Pool* getSessionPool() const {
        return _sessionPool;
    }

    FieldGroup *getFieldGroup(const std::string &fieldGroup, const MountMeta* mountMeta);

    autil::mem_pool::Pool *getPool() const { return _sessionPool; }
    std::shared_ptr<autil::mem_pool::Pool> getPoolPtr() const { return _poolPtr; }

    template<typename T>
    bool isMountableType() const;

private:
    template<typename T>
    Reference<T>* declareInner(const std::string &name,
        const std::string &groupName, const MountMeta *mountMeta,
        bool needConstruct, uint8_t serializeLevel, uint32_t allocateSize);

    std::string getDefaultGroupNameInner();

    ReferenceBase *findReferenceByName(const std::string &name) const;
    ReferenceBase *findMainReference(const std::string &name) const;
    ReferenceBase *findSubReference(const std::string &name) const;

    const MountInfoPtr& getMountInfo() const { return _mountInfo; }

public:
    FieldGroup *createFieldGroup(const std::string &fieldGroupName);
    FieldGroup *createFieldGroupUnattached();
    bool attachAndExtendFieldGroup(FieldGroup *fieldGroup, uint8_t serializeLevel, bool needConstruct = true);

private:
    MatchDoc getOne();
    void grow();
    void extendGroup(FieldGroup *fieldGroup, bool needConstruct = true);
    void initSub();
    MatchDoc &addSub(const MatchDoc &parent, const MatchDoc &doc);

    template<typename DocIdGenerator>
    void innerBatchAllocate(DocIdGenerator &docIdGenerator, std::vector<matchdoc::MatchDoc> &retVec, bool adviseNotConstruct);

    void deallocateAllMatchDocs(ReferenceBase *ref = NULL);
    void clearFieldGroups();
    void _deallocate(const MatchDoc &doc);
    void _deallocate(const MatchDoc *docs, uint32_t count);

    bool isSame(const FieldGroups &src, const FieldGroups &dst) const;
    void mergeDocs(MatchDocAllocator *other,
                   const std::vector<MatchDoc> &matchDocs,
                   std::vector<MatchDoc> &newMatchDocs);
    void appendDocs(MatchDocAllocator *other,
                   const std::vector<MatchDoc> &matchDocs,
                   std::vector<MatchDoc> &newMatchDocs);
    FieldGroup *createIfNotExist(const std::string &fieldGroup, const MountMeta* mountMeta);
    std::pair<FieldGroup*, bool> findGroup(const std::string &groupName);
private:
    // subdoc serialize: docid use sub docid
    void serializeMatchDoc(autil::DataBuffer &dataBuffer, const MatchDoc &matchdoc,
                           int32_t docid, uint8_t serializeLevel);

    MatchDoc deserializeMatchDoc(autil::DataBuffer &dataBuffer);
    void deserializeOneMatchDoc(autil::DataBuffer &dataBuffer, const MatchDoc &doc);
    void cloneDoc(uint32_t from, uint32_t to);
private:
    static FieldGroup *getReferenceGroup(MatchDocAllocator *allocator,
            const std::string &refName);
    bool copyGroupDocFields(const std::vector<MatchDoc> &matchdocs,
                            std::vector<MatchDoc> &newMatchDocs,
                            const std::vector<FieldGroup *> &copyGroups,
                            MatchDocAllocator *other, bool genNewDoc);
    bool renameFieldImpl(const std::string &name, const std::string &dstName);
    bool validate();
    FieldGroup *getOrCreateToExtendFieldGroup(const std::string &fieldGroupName);

private:
    uint32_t _size;
    uint32_t _capacity;
    // for deserialize
    int64_t _metaSignature;

    std::vector<uint32_t> _deletedIds;
    FieldGroups _fieldGroups;
    FieldGroups _toExtendfieldGroups;
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    autil::mem_pool::Pool *_sessionPool;
    ReferenceTypesWrapper *_referenceTypesWrapper;
    MountableTypesWrapper *_mountableTypesWrapper;
    MountInfoPtr _mountInfo;
    ReferenceAliasMapPtr _aliasMap;
    std::unordered_map<std::string, ReferenceBase *> _fastReferenceMap;
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
    // for mount tensor to matchdocs.
    // record field groups mounted and not modified. should do CoW on modifying.
    FieldGroups _mountedGroups;
private:
    bool _merged;
    bool _mountInfoIsPrivate;
    bool _sortRefFlag;
private:
    static MountableTypes staticMountableTypes;
    static ReferenceTypes staticReferenceTypes;
private:
    friend class MatchDocAllocatorTest;
    friend class SubCloner;
    friend class MatchDocDefaultValueSetter;

    AUTIL_LOG_DECLARE();
};


typedef std::shared_ptr<MatchDocAllocator> MatchDocAllocatorPtr;

class MountableTypesWrapper {
 public:
    MountableTypesWrapper(MountableTypes *mountableTypes)
        : _ownedReferenceTypes(false), _mountableTypes(mountableTypes){};
    ~MountableTypesWrapper() {
        if (_ownedReferenceTypes) {
            DELETE_AND_SET_NULL(_mountableTypes);
        }
    }

    template <typename T>
    void registerMountableType() {
        static std::string name = typeid(T).name();
        if (_mountableTypes->end() != _mountableTypes->find(name)) {
            return;
        }
        if (!_ownedReferenceTypes) {
            _mountableTypes = new MountableTypes(*_mountableTypes);
            _ownedReferenceTypes = true;
        }
        _mountableTypes->insert(name);
    }
    inline MountableTypes::const_iterator find(const std::string &name) {
        return _mountableTypes->find(name);
    }

    inline MountableTypes::const_iterator end() { return _mountableTypes->end(); }

 private:
    MountableTypesWrapper() = delete;
    MountableTypesWrapper(const MountableTypesWrapper &) = delete;
    MountableTypesWrapper &operator=(const MountableTypesWrapper &) = delete;

 private:
    bool _ownedReferenceTypes;
    MountableTypes *_mountableTypes;
};

template<typename T>
Reference<T>* MatchDocAllocator::declare(
        const std::string &name, uint8_t serializeLevel, uint32_t allocateSize)
{
    return declareWithConstructFlag<T>(name, getDefaultGroupName(), true,
            serializeLevel, allocateSize);
}

template<typename T>
void MatchDocAllocator::serializeImpl(autil::DataBuffer &dataBuffer, const T &matchdocs,
                                      uint8_t serializeLevel, bool ignoreUselessGroupMeta)
{
    autil::DataBuffer metaBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, dataBuffer.getPool());
    // 1. serialize meta
    serializeMeta(metaBuffer, serializeLevel, ignoreUselessGroupMeta);

    int64_t metaSignature = autil::MurmurHash::MurmurHash64A(
            metaBuffer.getData(), metaBuffer.getDataLen(), 0);
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
        serializeMatchDoc(dataBuffer, matchdocs[i],
                          matchdocs[i].getDocId(), serializeLevel);
    }
}

template<typename T>
void MatchDocAllocator::deserializeImpl(autil::DataBuffer &dataBuffer, T &matchdocs)
{
    dataBuffer.read(_metaSignature);
    uint32_t metaLen = 0;
    dataBuffer.read(metaLen);
    const void *metaData = dataBuffer.readNoCopy(metaLen);

    autil::DataBuffer metaBuffer((void *)metaData, metaLen, dataBuffer.getPool());

    // 1. deserialize meta
    deserializeMeta(metaBuffer, NULL);

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

template<typename T>
bool MatchDocAllocator::deserializeAndAppendImpl(autil::DataBuffer &dataBuffer, T &matchDocs)
{
    int64_t thisSignature = 0;
    dataBuffer.read(thisSignature);

    // skip meta
    uint32_t metaLen = 0;
    dataBuffer.read(metaLen);
    auto metaData = dataBuffer.readNoCopy(metaLen);

    if (thisSignature != _metaSignature) {
        AUTIL_LOG(WARN, "signature not same, can not append, this signature: %ld"
                  ", target signature: %ld", thisSignature, _metaSignature);

        // deserialize meta and print debug infos
        MatchDocAllocator debugAllocator(_sessionPool, _subAllocator);
        autil::DataBuffer metaBuffer((void*)metaData, metaLen, dataBuffer.getPool());
        debugAllocator.deserializeMeta(metaBuffer, NULL);

        AUTIL_LOG(WARN, "target allocator : [%s]", toDebugString().c_str());
        AUTIL_LOG(WARN, "deserialize allocator : [%s]",
                  debugAllocator.toDebugString().c_str());
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

template<typename T>
Reference<T>* MatchDocAllocator::declareWithConstructFlagDefaultGroup(
        const std::string &name, bool needConstruct,
        uint8_t serializeLevel, uint32_t allocateSize)
{
    return declareWithConstructFlag<T>(name, getDefaultGroupName(), needConstruct,
                        serializeLevel, allocateSize);
}

template<typename T>
Reference<T>* MatchDocAllocator::declare(const std::string &name,
        const std::string &groupName, uint8_t serializeLevel, uint32_t allocateSize) {
    return declareWithConstructFlag<T>(name, groupName, true, serializeLevel, allocateSize);
}

template<typename T>
Reference<T>* MatchDocAllocator::declareWithConstructFlag(const std::string &name,
        const std::string &groupName, bool needConstruct,
        uint8_t serializeLevel, uint32_t allocateSize)
{
    autil::ScopedSpinLock lock(_lock);
    const std::string& refName = getReferenceName(name);
    const MountMeta* mountMeta = NULL;
    if (_mountInfo && isMountableType<T>())
    {
        mountMeta = _mountInfo->get(refName);
    }
    return declareInner<T>(refName, groupName, mountMeta, needConstruct, serializeLevel, allocateSize);
}

template<typename T>
Reference<T>* MatchDocAllocator::declareInner(const std::string &refName,
        const std::string &groupName, const MountMeta *mountMeta,
        bool needConstruct, uint8_t serializeLevel, uint32_t allocateSize)
{
    // already declared
    ReferenceBase *ref = findReferenceByName(refName);
    if (ref) {
        Reference<T>* typedRef = dynamic_cast<Reference<T>*>(ref);
        if (!typedRef) {
            return nullptr;
        }
        if (serializeLevel > typedRef->getSerializeLevel()) {
            typedRef->setSerializeLevel(serializeLevel);
        }
        return typedRef;
    }

    auto fieldGroup = createIfNotExist(groupName, mountMeta);
    if (!fieldGroup) {
        return nullptr;
    }

    Reference<T> *typedRef = fieldGroup->declare<T>(refName, mountMeta,
            needConstruct, allocateSize);
    assert(typedRef);
    typedRef->setSerializeLevel(serializeLevel);

    _fastReferenceMap[refName] = typedRef;
    return typedRef;
}


template<typename T>
Reference<T>* MatchDocAllocator::declareSub(
        const std::string &name, uint8_t serializeLevel, uint32_t allocateSize)
{
    if (!_subAllocator) {
        initSub();
    }
    return declareSubWithConstructFlag<T>(name,
            _subAllocator->getDefaultGroupName(), true,
            serializeLevel, allocateSize);
}

template<typename T>
Reference<T>* MatchDocAllocator::declareSub(
        const std::string &name,
        const std::string &groupName,
        uint8_t serializeLevel, uint32_t allocateSize)
{
    return declareSubWithConstructFlag<T>(name, groupName, true,
            serializeLevel, allocateSize);
}

template<typename T>
Reference<T>* MatchDocAllocator::declareSubWithConstructFlagDefaultGroup(
        const std::string &name,
        bool needConstruct,
        uint8_t serializeLevel,
        uint32_t allocateSize) {
    if (!_subAllocator) {
        initSub();
    }
    return declareSubWithConstructFlag<T>(name, _subAllocator->getDefaultGroupName(),
            needConstruct, serializeLevel, allocateSize);
}

template<typename T>
Reference<T>* MatchDocAllocator::declareSubWithConstructFlag(
        const std::string &name,
        const std::string &groupName,
        bool needConstruct,
        uint8_t serializeLevel,
        uint32_t allocateSize)
{
    if (!_subAllocator) {
        initSub();
    }

    autil::ScopedSpinLock lock(_lock);
    const std::string& refName = getReferenceName(name);
    ReferenceBase *baseRef = findSubReference(refName);
    if (baseRef) {
        return dynamic_cast<Reference<T>*>(baseRef);
    }

    baseRef = findMainReference(refName);
    if (baseRef) {
        AUTIL_LOG(ERROR, "%s already declared in main allocator", refName.c_str());
        return nullptr;
    }

    Reference<T> *ref = _subAllocator->declareWithConstructFlag<T>(
            refName, groupName, needConstruct, serializeLevel, allocateSize);
    if (ref) {
        ref->setCurrentRef(_currentSub);
    }
    return ref;
}

template<typename T>
Reference<T>* MatchDocAllocator::findReference(const std::string &name) const {
    ReferenceBase *ref = findReferenceWithoutType(name);
    if (!ref) {
        return nullptr;
    }
    return dynamic_cast<Reference<T> *>(ref);
}

template<typename T>
void MatchDocAllocator::registerType() {
    _referenceTypesWrapper->registerType<T>();
    if (_subAllocator) {
        _subAllocator->registerType<T>();
    }
}

template<typename T>
void MatchDocAllocator::registerMountableType() {
    _mountableTypesWrapper->registerMountableType<T>();
    if (_subAllocator) {
        _subAllocator->registerMountableType<T>();
    }
}

template<typename T>
bool MatchDocAllocator::isMountableType() const {
    static std::string name = typeid(T).name();
    return _mountableTypesWrapper->find(name) != _mountableTypesWrapper->end();
}

inline ReferenceBase* MatchDocAllocator::findReferenceWithoutType(
        const std::string &name) const
{
    const std::string& refName = getReferenceName(name);
    autil::ScopedSpinLock lock(_lock);
    return findReferenceByName(refName);
}

template <typename T>
inline Reference<T>* MatchDocAllocator::createSingleFieldGroup(
        const std::string &name,
        T *data, uint32_t count,
        uint8_t serializeLevel)
{
    autil::ScopedSpinLock lock(_lock);
    if (!CanDataMountTypeTraits<T>::canDataMount()) {
        AUTIL_LOG(WARN, "type %s can not data mount into matchdoc.",
                  autil::StringUtil::getTypeString(*data).c_str());
        return nullptr;
    }
    bool needDestruct = DestructTypeTraits<T>::NeedDestruct();
    if (needDestruct) {
        AUTIL_LOG(WARN, "need destruct type %s can not mount into matchdoc.",
                  autil::StringUtil::getTypeString(*data).c_str());
        return nullptr;
    }
    if (_size != count) {
        AUTIL_LOG(WARN, "count mismatch, expected: %d, actual: %d",
                  (int)count, (int)_size);
        return nullptr;
    }
    FieldGroup *group = findGroup(name).first;
    if (group != nullptr) {
        AUTIL_LOG(WARN, "group %s already exists", name.c_str());
        return nullptr;
    }
    group = new FieldGroup(_sessionPool, name, _referenceTypesWrapper, (char *)data, sizeof(T), count, _capacity);
    group->setPoolPtr(_poolPtr);
    Reference<T> *ref = group->createSingleFieldRef<T>();
    if (ref == nullptr) {
        delete group;
        return nullptr;
    }
    ref->setSerializeLevel(serializeLevel);
    ref->setValueType(ValueTypeHelper<T>::getValueType());
    _fastReferenceMap[ref->getName()] = ref;
    _fieldGroups[name] = group;
    _mountedGroups[name] = group;
    return ref;
}

template<typename DocIdGenerator>
void MatchDocAllocator::innerBatchAllocate(DocIdGenerator &docIdGenerator,
                                        std::vector<matchdoc::MatchDoc> &retVec, bool adviseNotConstruct) {
    if (!_toExtendfieldGroups.empty()) {
        extend();
    }
    cowMountedGroup();
    size_t count = docIdGenerator.size();
    size_t delCount = _deletedIds.size();
    retVec.reserve(count);
    if (delCount < count) {
        for (size_t i = 0; i < delCount; i++ ) {
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
        MatchDoc &matchDoc, std::vector<matchdoc::MatchDoc> &subdocVec)
{
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

}
#endif //ISEARCH_MATCHDOCALLOCATOR_H
