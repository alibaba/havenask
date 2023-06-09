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
#ifndef ISEARCH_FIELDGROUP_H
#define ISEARCH_FIELDGROUP_H

#include <assert.h>
#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include <stddef.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <unordered_set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "matchdoc/CommonDefine.h"
#include "matchdoc/DocStorage.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MountInfo.h"
#include "matchdoc/Reference.h"

namespace autil {
class DataBuffer;
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace matchdoc {
typedef std::unordered_set<std::string> ReferenceNameSet;

class FieldGroup {
public:
    FieldGroup(autil::mem_pool::Pool *pool,
               ReferenceTypesWrapper *referenceTypes,
               const std::string &groupName);
    //for mount storage
    FieldGroup(autil::mem_pool::Pool *pool,
               const std::string &name, ReferenceTypesWrapper *referenceTypes,
               char *data, uint32_t docSize, uint32_t docCount,
               uint32_t capacity);
    ~FieldGroup();
private:
    FieldGroup(const FieldGroup &);
    FieldGroup& operator=(const FieldGroup &);
public:
    template<typename T>
    void registerType(int id);

    template <typename T>
    Reference<T>* createSingleFieldRef();

    template<typename T>
    Reference<T>* declare(const std::string &name, const MountMeta* mountMeta,
                          bool needConstruct,
                          uint32_t allocateSize = sizeof(T));
    bool dropField(const std::string &name);
    bool renameField(const std::string &name, const std::string &dstName);
    ReferenceBase* findReferenceWithoutType(const std::string &name);

    bool isMounted(const MountMeta* mountMeta) const;

    void growToSize(uint32_t size);
    void doCoW() {
        _storage->cowFromMount();
    }

    void setConstructEssential(bool flag);
    void constructDoc(const MatchDoc &doc);
    void constructDocs(const std::vector<matchdoc::MatchDoc> &docs);
    void constructDocs(const std::vector<MatchDoc>::const_iterator &begin,
                       const std::vector<MatchDoc>::const_iterator &end);
    void destructDoc(const MatchDoc &doc);
    void destructDoc(const MatchDoc *docs, uint32_t count);
    void cloneDoc(const MatchDoc &newDoc, const MatchDoc &cloneDoc);
    // clone match doc from other allocator
    void cloneDoc(FieldGroup *other, const MatchDoc &newDoc, const MatchDoc &cloneDoc);

    // must be called before serialize
    void sortReference();
    void serializeMeta(autil::DataBuffer &dataBuffer,
                       uint8_t serializeLevel) const;
    void deserializeMeta(autil::DataBuffer &dataBuffer);

    void serialize(autil::DataBuffer &dataBuffer, const MatchDoc &doc,
                   uint8_t serializeLevel);
    void deserialize(autil::DataBuffer &dataBuffer, const MatchDoc &doc);

    uint32_t getDocSize() const {
        return _storage->getDocSize();
    }

    uint32_t getColomnSize() const {
        return _storage->getSize();
    }
    // for deserialize
    void setCurrentRef(Reference<MatchDoc> *current);
    void clearContent() {
        _storage->reset();
    }

    void resetSerializeLevelAndAlias(uint8_t level = 0);
    void resetSerializeLevel(uint8_t level);

    bool needSerialize(uint8_t serializeLevel) const;

    bool isSame(const FieldGroup &other) const;

    void getAllReferenceNames(ReferenceNameSet &names) const;

    void getAllSerializeElements(ReferenceVector &vec,
                                 uint8_t serializeLevel) const;
    void getAllSerializeElements(ReferenceMap& map, uint8_t serializeLevel) const;
    void getRefBySerializeLevel(ReferenceVector &vec,
                                uint8_t serializeLevel) const;
    const std::string &getGroupName() const {
        return _groupName;
    }
    void setPoolPtr(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr) {
        _storage->setPoolPtr(poolPtr);
    }
public: // for MatchDocJoiner
    const ReferenceMap& getReferenceMap() const;
    ReferenceBase* cloneReference(const ReferenceBase *reference,
                                  const std::string &name);
    ReferenceBase* cloneMountedReference(const ReferenceBase *reference,
                                         const std::string &name,
                                         uint32_t mountId);
    void check() const;
    void appendGroup(FieldGroup *other);
    void truncateGroup(uint32_t size);
    const ReferenceVector &getReferenceVec() const {
        return _referenceVec;
    }
    void swapDocStorage(FieldGroup &other);
private:
    typedef std::map<uint32_t, uint32_t> MountId2OffsetMap;

private:
    template<typename T>
    Reference<T>* doDeclare(const std::string &name,
                            uint32_t offset,
                            uint64_t mountOffset,
                            bool needConstruct,
                            uint32_t allocateSize);

    void setSharedOffset(uint32_t mountId, uint64_t offset);
    uint64_t getSharedOffset(uint32_t mountId) const;
public:
    bool needConstruct() const { return _needConstruct; }
    bool needDestruct() const { return _needDestruct; }

private:
    autil::mem_pool::Pool *_pool;
    DocStorage *_storage;
    ReferenceMap _referenceMap; // for find, check, isSame
    ReferenceVector _referenceVec;
    ReferenceTypesWrapper *_referenceTypesWrapper;
    MountId2OffsetMap _id2offsetMap;
    std::string _groupName;
    bool _needConstruct;
    bool _needDestruct;
private:
    AUTIL_LOG_DECLARE();
};

inline void FieldGroup::constructDoc(const MatchDoc &doc) {
    if (!_needConstruct) {
        return;
    }

    bool mountInit = false;
    for (auto ref : _referenceVec) {
        if (ref->isMount()) {
            if (!mountInit) {
                ref->construct(doc);
                mountInit = (_id2offsetMap.size() == 1) ? true : false;
            }
        } else if (ref->needConstructMatchDoc()) {
            ref->construct(doc);
        }
    }
}

inline void FieldGroup::constructDocs(const std::vector<matchdoc::MatchDoc> &docs) {
    constructDocs(docs.begin(), docs.end());
}

inline void FieldGroup::constructDocs(const std::vector<MatchDoc>::const_iterator &begin,
                                      const std::vector<MatchDoc>::const_iterator &end) {
    if (!_needConstruct) {
        return;
    }

    bool mountInit = false;
    for (auto ref : _referenceVec) {
        if (ref->isMount()) {
            if (!mountInit) {
                ref->constructDocs(begin, end);
                mountInit = (_id2offsetMap.size() == 1) ? true : false;
            }
        } else if (ref->needConstructMatchDoc()) {
            ref->constructDocs(begin, end);
        }
    }
}

inline void FieldGroup::destructDoc(const MatchDoc &doc) {
    if (!_needDestruct) {
        return;
    }
    for (auto ref : _referenceVec) {
        if (ref->needDestructMatchDoc()) {
            ref->destruct(doc);
        }
    }
}

inline void FieldGroup::destructDoc(const MatchDoc *docs, uint32_t count) {
    if (!_needDestruct) {
        return;
    }
    for (auto ref : _referenceVec) {
        if (ref->needDestructMatchDoc()) {
            for (uint32_t i = 0; i < count; i++) {
                ref->destruct(docs[i]);
            }
        }
    }
}

inline void FieldGroup::cloneDoc(const MatchDoc &newDoc, const MatchDoc &cloneDoc) {
    for (auto ref : _referenceVec) {
        ref->cloneConstruct(newDoc, cloneDoc);
    }
}

template<typename T>
Reference<T> *FieldGroup::declare(const std::string &name,
                                  const MountMeta *mountMeta,
                                  bool needConstruct,
                                  uint32_t allocateSize)
{
    uint32_t offset = _storage->getDocSize();
    uint64_t mountOffset = INVALID_OFFSET;

    // NOTE: the allocateSize here is actually the size of data type.
    // for mounted reference, can not allocate memory of this size.
    allocateSize = allocateSize > sizeof(T) ? allocateSize : sizeof(T);
    if (mountMeta == NULL) {
        _storage->incDocSize(allocateSize);
    } else {
        assert(mountMeta->mountId != INVALID_MOUNT_ID);
        assert(mountMeta->mountOffset != INVALID_OFFSET);
        uint64_t sharedOffset = getSharedOffset(mountMeta->mountId);
        if (sharedOffset == INVALID_OFFSET) {
            setSharedOffset(mountMeta->mountId, offset);
            _storage->incDocSize(sizeof(char*));
        } else {
            offset = sharedOffset;
        }
        mountOffset = mountMeta->mountOffset;
    }
    Reference<T>* ref = doDeclare<T>(name, offset, mountOffset,
            needConstruct, allocateSize);
    return ref;
}

template<typename T>
Reference<T>* FieldGroup::doDeclare(
    const std::string &name,
    uint32_t offset,
    uint64_t mountOffset,
    bool needConstruct,
    uint32_t allocateSize)
{
    _referenceTypesWrapper->registerType<T>();
    Reference<T>* ref = new Reference<T>(_storage,
            offset, mountOffset, allocateSize, _groupName, needConstruct);
    ref->setName(name);
    _referenceVec.push_back(ref);
    _referenceMap[name] = ref;
    if (!_needConstruct && ref->needConstructMatchDoc()) {
        _needConstruct = true;
    }
    if (!_needDestruct && ref->needDestructMatchDoc())
    {
        _needDestruct = true;
    }
    return ref;
}

template <typename T>
inline Reference<T>* FieldGroup::createSingleFieldRef() {
    if (_storage->getDocSize() != sizeof(T)) {
        return nullptr;
    }
    Reference<T> *ref = doDeclare<T>(_groupName, 0, INVALID_OFFSET, false, sizeof(T));
    _needConstruct = false;
    _needDestruct = false;
    ref->setNeedDestructMatchDoc(false);
    return ref;
}

inline void FieldGroup::growToSize(uint32_t size) {
    _storage->growToSize(size);
}

inline bool FieldGroup::dropField(const std::string &name) {
    auto mpIt = _referenceMap.find(name);
    for (auto it = _referenceVec.begin(); it != _referenceVec.end(); it++) {
        if ((*it)->getName() == name) {
            delete *it;
            _referenceVec.erase(it);
            break;
        }
    }
    if (mpIt != _referenceMap.end()) {
        _referenceMap.erase(mpIt);
    }
    return _referenceMap.empty();
}

inline bool FieldGroup::renameField(const std::string &name, const std::string &dstName) {
    auto mpIt = _referenceMap.find(name);
    if (mpIt != _referenceMap.end()) {
        if (name != dstName) {
            if (_referenceMap.find(dstName) != _referenceMap.end()) {
                return false;
            }
            auto ref = mpIt->second;
            ref->setName(dstName);
            _referenceMap.erase(mpIt);
            _referenceMap.emplace(dstName, ref);
        }
        return true;
    }
    return false;
}

inline ReferenceBase* FieldGroup::findReferenceWithoutType(const std::string &name) {
    auto it = _referenceMap.find(name);
    if (_referenceMap.end() != it) {
        return it->second;
    }
    return NULL;
}

inline bool FieldGroup::isMounted(const MountMeta* mountMeta) const {
    return INVALID_OFFSET != getSharedOffset(mountMeta->mountId);
}

inline void FieldGroup::setSharedOffset(uint32_t mountId, uint64_t offset) {
    _id2offsetMap[mountId] = offset;
}

inline uint64_t FieldGroup::getSharedOffset(uint32_t mountId) const {
    auto it = _id2offsetMap.find(mountId);
    if (it != _id2offsetMap.end()) {
        return it->second;
    }
    return INVALID_OFFSET;
}

}

#endif //ISEARCH_FIELDGROUP_H
