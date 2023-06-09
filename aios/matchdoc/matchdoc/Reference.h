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
#ifndef ISEARCH_REFERENCE_H
#define ISEARCH_REFERENCE_H

#include <unordered_map>
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/DocStorage.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "autil/DataBuffer.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "autil/MultiValueType.h"
#include "autil/PackDataReader.h"

namespace matchdoc {

class FieldGroup;
class MatchDocAllocator;
class ReferenceBase;
class ReferenceTypesWrapper;
typedef std::unordered_map<VariableType, std::pair<ReferenceBase *, bool>> ReferenceTypes;
typedef std::unordered_map<std::string, ReferenceBase*> ReferenceMap;
typedef std::vector<ReferenceBase*> ReferenceVector;

template <typename T>
class Reference;

class ReferenceMeta {
public:
    ReferenceMeta();
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer,
                     autil::mem_pool::Pool* pool);

    bool operator==(const ReferenceMeta &other) const;
    bool operator!=(const ReferenceMeta &other) const;

    bool isAutilMultiValueType() const;

public:
    VariableType vt;
    uint32_t allocateSize;
    bool needDestruct;
    bool useAlias;
    uint8_t serializeLevel;
    uint8_t optFlag;
    std::string name;
    std::string alias;
    ValueType valueType;
};

class ReferenceBase {
public:
    ReferenceBase(const VariableType &type,
                  const std::string &groupName);
    ReferenceBase(const VariableType &type, DocStorage *docStorage,
                  uint32_t allocateSize, const std::string &groupName);
    ReferenceBase(const VariableType &type, DocStorage *docStorage,
                  uint32_t offset, uint64_t mountOffset, uint32_t allocateSize,
                  const std::string &groupName);
    virtual ~ReferenceBase();

public:
    virtual ReferenceBase *getTypedRef(autil::mem_pool::Pool *pool,
            DocStorage *docStorage, uint32_t allocateSize, std::string &groupName,
            bool needConstruct) const = 0;
    virtual ReferenceBase *getTypedMountRef(autil::mem_pool::Pool *pool,
            DocStorage *docStorage, uint32_t offset, uint64_t mountOffset,
            uint32_t allocateSize, std::string &groupName) const = 0;
    virtual ReferenceBase *clone() const = 0;
    virtual void construct(MatchDoc doc) const = 0;
    virtual void constructDocs(const std::vector<matchdoc::MatchDoc> &docs) const = 0;
    virtual void constructDocs(const std::vector<MatchDoc>::const_iterator &begin,
                               const std::vector<MatchDoc>::const_iterator &end) const = 0;
    virtual void cloneConstruct(MatchDoc newDoc, MatchDoc cloneDoc) const = 0;
    virtual void destruct(MatchDoc doc) const = 0;
    virtual void serialize(MatchDoc doc, autil::DataBuffer &dataBuffer) const = 0;
    virtual void deserialize(MatchDoc doc, autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool) = 0;
    virtual std::string toString(MatchDoc doc) const = 0;
    virtual bool supportClone() const = 0;
    inline bool isMount() const { return _mountOffset != INVALID_OFFSET; }
    inline bool mount(MatchDoc doc, const char* baseAddress) const {
        if (likely(isMount())) {
            (const char*&)getMountAddress(getCurrentSubDocMaybe(doc)) = baseAddress;
            return true;
        }
        return false;
    }
    inline char*& getMountAddress(MatchDoc doc) const __attribute__((always_inline)) {
        char* addr = _docStorage->get(doc.getIndex()) + _offset;
        return *(char**)addr;
    }

    inline uint64_t getMountOffset() const {
        return _mountOffset;
    }

    inline uint64_t getOffset() const {
        return _offset;
    }

    inline const std::string& getGroupName() const
    { return _groupName; }

    void setSerializeLevel(uint8_t sl) {
        meta.serializeLevel = sl;
    }

    uint8_t getSerializeLevel() const {
        return meta.serializeLevel;
    }

    void setSerialAliasName(const std::string& name, bool useAlias = true) {
        meta.alias = name;
        meta.useAlias = useAlias;
    }
    void resetSerialAliasName() {
        meta.alias.clear();
        meta.useAlias = false;
    }
    const std::string& getSerialAliasName() {
        if (meta.useAlias) {
            return meta.alias;
        } else {
            return meta.name;
        }
    }

    void setName(const std::string &name) {
        meta.name = name;
    }

    const std::string& getName() const {
        return meta.name;
    }

    ValueType getValueType() const {
        return meta.valueType;
    }

    void setValueType(ValueType vt) {
        meta.valueType = vt;
    }

    const VariableType &getVariableType() const {
        return meta.vt;
    }

    uint32_t getAllocateSize() const {
        return meta.allocateSize;
    }

    const ReferenceMeta &getReferenceMeta() const {
        return meta;
    }

    bool isSubDocReference() const {
        return _current != NULL;
    }

    bool needConstructMatchDoc() const { return meta.valueType.needConstruct(); }
    bool needDestructMatchDoc() const { return meta.needDestruct; }
    void setNeedDestructMatchDoc(bool needDestruct)  {
        meta.needDestruct = needDestruct;
    }
    void replaceStorage(DocStorage *docStorage) {
        _docStorage = docStorage;
    }
    std::string toDebugString() const;
    void setIsSubReference() {
        meta.optFlag |= 1;
    }
    void setIsMountReference() {
        meta.optFlag |= (1<<1);
    }

public: // for MatchDocAllocator, not for user
    void setCurrentRef(Reference<MatchDoc> *current) {
        _current = current;
        if (_current) {
            setIsSubReference();
        }

    }
    Reference<MatchDoc> *getCurrentRef() const {
        return _current;
    }

public: // for MatchDocJoiner, not for user
    virtual void cloneConstruct(const MatchDoc& dstDoc, const MatchDoc& srcDoc,
                                const ReferenceBase* srcRef) const = 0;

protected:
    inline MatchDoc getCurrentSubDocMaybe(MatchDoc doc) const __attribute__((always_inline));

protected:
    const uint64_t _offset;
    const uint64_t _mountOffset;
    DocStorage *_docStorage;
    Reference<MatchDoc> *_current;
    const std::string _groupName;
    ReferenceMeta meta;

protected:
    template<typename T> friend class Reference; // need access _docStorage
};

template<typename T>
class Reference : public ReferenceBase {
public:
    Reference(bool needConstruct = true);
    Reference(DocStorage *docStorage, uint32_t allocateSize,
              const std::string &groupName, bool needConstruct);
    Reference(DocStorage *docStorage, uint32_t offset, uint64_t mountOffset,
              uint32_t allocateSize, const std::string &groupName,
              bool needConstruct);
    ~Reference();
private:
    Reference(const Reference &);
    Reference& operator=(const Reference &);

public: // for user
    inline T* getPointer(MatchDoc doc) const __attribute__((always_inline));
    inline T get(MatchDoc doc) const __attribute__((always_inline));
    inline T& getReference(MatchDoc doc) __attribute__((always_inline));
    inline const T& getReference(MatchDoc doc) const __attribute__((always_inline));
    inline void set(MatchDoc doc, const T& value) const __attribute__((always_inline));

public: // for DocStorage, not for user
    ReferenceBase *getTypedRef(autil::mem_pool::Pool *pool,
                               DocStorage *docStorage,
                               uint32_t allocateSize,
                               std::string &groupName,
                               bool needConstruct) const override;
    ReferenceBase *getTypedMountRef(autil::mem_pool::Pool *pool,
                                    DocStorage *docStorage, uint32_t offset, uint64_t mountOffset,
                                    uint32_t allocateSize, std::string &groupName) const override;
    ReferenceBase *clone() const override;
    void construct(MatchDoc doc) const override;
    void constructDocs(const std::vector<matchdoc::MatchDoc> &docs) const override;
    void constructDocs(const std::vector<MatchDoc>::const_iterator &begin,
                       const std::vector<MatchDoc>::const_iterator &end) const override;
    void cloneConstruct(MatchDoc newDoc, MatchDoc cloneDoc) const override;
    void destruct(MatchDoc doc) const override;
    void serialize(MatchDoc doc, autil::DataBuffer &dataBuffer) const override;
    void deserialize(MatchDoc doc, autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool) override;
    std::string toString(MatchDoc doc) const override;
    bool supportClone() const override;
public:
    static void registerType(ReferenceTypes *referenceTypes) {
        static  std::string name = typeid(T).name();
        if (referenceTypes->end() != referenceTypes->find(name)) {
            return;
        }
        (*referenceTypes)[name] = std::make_pair(new Reference<T>(), true);
    }

    static void registerInnerType(ReferenceTypes *referenceTypes) {
        static Reference<T> value;
        std::string name = typeid(T).name();
        (*referenceTypes)[name] = std::make_pair(&value, false);
    }

private: // for MatchDocJoiner
    void cloneConstruct(const MatchDoc& dstDoc, const MatchDoc& srcDoc,
                        const ReferenceBase* srcRef) const override;
private:
    void _serialize(MatchDoc doc, autil::DataBuffer &dataBuffer, SupportSerializeType) const;
    void _serialize(MatchDoc doc, autil::DataBuffer &dataBuffer, UnSupportSerializeType) const;
    void _deserialize(MatchDoc doc, autil::DataBuffer &dataBuffer, SupportSerializeType, autil::mem_pool::Pool *pool);
    void _deserialize(MatchDoc doc, autil::DataBuffer &dataBuffer, UnSupportSerializeType, autil::mem_pool::Pool *pool);
    std::string _toString(MatchDoc& doc, SimpleType) const;
    std::string _toString(MatchDoc& doc, SimpleVectorType) const;
    std::string _toString(MatchDoc& doc, ToStringType) const;
    std::string _toString(MatchDoc& doc, TwoLevelVectorType) const;
    void _cloneConstruct(MatchDoc newDoc, MatchDoc cloneDoc, SupportCloneType) const;
    void _cloneConstruct(MatchDoc newDoc, MatchDoc cloneDoc, UnSupportCloneType) const;
    void _cloneConstruct(const MatchDoc& dstDoc, const MatchDoc& srcDoc,
                         const ReferenceBase* srcRef, SupportCloneType) const;
    void _cloneConstruct(const MatchDoc& dstDoc, const MatchDoc& srcDoc,
                         const ReferenceBase* srcRef, UnSupportCloneType) const;

    T getMountValue(MatchDoc doc) const;
    T* getRealPointer(MatchDoc doc) const;
    T* getMountRealPointer(MatchDoc doc) const;
    T* getNonMountRealPointer(MatchDoc doc) const;
    inline T toValue(const void *addr, uint64_t offset) const __attribute__((always_inline));
private:
    friend class SubDocAccessor;
};

template<typename T>
const std::string& referenceTypeName() {
    static std::string name = typeid(T).name();
    return name;
}
// for registerType
template<typename T>
Reference<T>::Reference(bool needConstruct)
    : ReferenceBase(referenceTypeName<T>(), "")
{
    meta.needDestruct = DestructTypeTraits<T>::NeedDestruct();
    meta.valueType = ValueTypeHelper<T>::getValueType();
    meta.valueType.setNeedConstruct(needConstruct);
}

// for getTypedRef
template<typename T>
Reference<T>::Reference(DocStorage *docStorage,
                        uint32_t allocateSize,
                        const std::string &groupName,
                        bool needConstruct)
    : ReferenceBase(referenceTypeName<T>(), docStorage, allocateSize,
                    groupName)
{
    docStorage->incDocSize(allocateSize);
    meta.needDestruct = DestructTypeTraits<T>::NeedDestruct();
    meta.valueType = ValueTypeHelper<T>::getValueType();
    meta.valueType.setNeedConstruct(needConstruct);
}

// for declare mount reference
template<typename T>
inline Reference<T>::Reference(DocStorage *docStorage, uint32_t offset,
                               uint64_t mountOffset, uint32_t allocateSize,
                               const std::string &groupName,
                               bool needConstruct)
    : ReferenceBase(referenceTypeName<T>(), docStorage,
                    offset, mountOffset, allocateSize, groupName)
{
    meta.needDestruct = DestructTypeTraits<T>::NeedDestruct();
    meta.valueType = ValueTypeHelper<T>::getValueType();
    meta.valueType.setNeedConstruct(needConstruct || mountOffset != INVALID_OFFSET);
}

template<typename T>
Reference<T>::~Reference() {}

template<typename T>
std::string Reference<T>::toString(MatchDoc doc) const {
    return _toString(doc, typename ToStringTypeTraits<T>::Type());
}

template<typename T>
std::string Reference<T>::_toString(MatchDoc& doc, SimpleType) const {
    const T &value = get(doc);
    return autil::StringUtil::toString(value);
}

template<typename T>
std::string Reference<T>::_toString(MatchDoc& doc, SimpleVectorType) const {
    const T &value = get(doc);
    return autil::StringUtil::toString(value,
            std::string(1, autil::MULTI_VALUE_DELIMITER));
}

// set first level delimiter to '\x1D', second level default is '\x1E'
template<typename T>
std::string Reference<T>::_toString(MatchDoc& doc, TwoLevelVectorType) const {
    const T &value = get(doc);
    return autil::StringUtil::toString(value, "\x1D", "\x1E");
}

template<typename T>
std::string Reference<T>::_toString(MatchDoc& doc, ToStringType) const {
    return "";
}

template<typename T>
inline ReferenceBase *Reference<T>::getTypedRef(autil::mem_pool::Pool *pool,
        DocStorage *docStorage, uint32_t allocateSize, std::string &groupName,
        bool needConstruct) const
{
    return POOL_COMPATIBLE_NEW_CLASS(pool, Reference<T>, docStorage, allocateSize,
                                     groupName, needConstruct);
}

template<typename T>
inline ReferenceBase *Reference<T>::getTypedMountRef(autil::mem_pool::Pool *pool,
        DocStorage *docStorage, uint32_t offset, uint64_t mountOffset,
        uint32_t allocateSize, std::string &groupName) const
{
    return POOL_COMPATIBLE_NEW_CLASS(pool, Reference<T>, docStorage, offset, mountOffset,
                                     allocateSize, groupName, true);
}


template<typename T>
inline bool Reference<T>::supportClone() const {
    return std::is_same<typename SupportCloneTrait<T>::type, SupportCloneType>::value;
}

template<typename T>
inline ReferenceBase *Reference<T>::clone() const {
    return new Reference<T>();
}

template<typename T>
inline void Reference<T>::construct(MatchDoc doc) const {
    if (!isMount()) {
        new ((void *)getNonMountRealPointer(doc)) T();
    } else {
        getMountAddress(doc) = NULL;
    }
}

template <typename T>
inline void Reference<T>::constructDocs(const std::vector<matchdoc::MatchDoc> &docs) const {
    constructDocs(docs.begin(), docs.end());
}

template <typename T>
inline void Reference<T>::constructDocs(const std::vector<MatchDoc>::const_iterator &begin,
                                        const std::vector<MatchDoc>::const_iterator &end) const {
    if (!isMount()) {
        for (auto itr = begin; itr != end; ++itr) {
            new ((void *)getNonMountRealPointer(*itr)) T();
        }
    } else {
        for (auto itr = begin; itr != end; ++itr) {
            getMountAddress(*itr) = NULL;
        }
    }
}

template<typename T>
inline void Reference<T>::cloneConstruct(MatchDoc newDoc, MatchDoc cloneDoc) const {
    typedef typename SupportCloneTrait<T>::type Type;
    _cloneConstruct(newDoc, cloneDoc, Type());
}

template <typename T>
inline void Reference<T>::_cloneConstruct(
        MatchDoc newDoc, MatchDoc cloneDoc, SupportCloneType) const {
    if (!isMount()) {
        new ((void *)getNonMountRealPointer(newDoc)) T(*getNonMountRealPointer(cloneDoc));
    } else {
        getMountAddress(newDoc) = getMountAddress(cloneDoc);
    }
}

template <typename T>
inline void Reference<T>::_cloneConstruct(
        MatchDoc newDoc, MatchDoc cloneDoc, UnSupportCloneType) const {
    // default construct, avoid destruct core
    construct(newDoc);
}

template<typename T>
inline void Reference<T>::cloneConstruct(
        const MatchDoc& dstDoc, const MatchDoc& srcDoc,
        const ReferenceBase* srcRef) const
{
    typedef typename SupportCloneTrait<T>::type Type;
    _cloneConstruct(dstDoc, srcDoc, srcRef, Type());
}

template<typename T>
inline void Reference<T>::_cloneConstruct(
        const MatchDoc& dstDoc, const MatchDoc& srcDoc,
        const ReferenceBase* srcRef, SupportCloneType) const
{
    assert(dynamic_cast<const Reference<T>*>(srcRef));
    if (isMount()) {
        assert(srcRef->isMount());
        mount(dstDoc, srcRef->getMountAddress(srcDoc));
    } else {
        const Reference<T>* srcTypedRef = static_cast<const Reference<T>*>(srcRef);
        T val = srcTypedRef->get(srcDoc);
        new ((void *)getPointer(dstDoc)) T(val);
    }
}

template<typename T>
inline void Reference<T>::_cloneConstruct(const MatchDoc& dstDoc, const MatchDoc& srcDoc,
                                   const ReferenceBase* srcRef, UnSupportCloneType) const
{
    new ((void *)getPointer(dstDoc)) T();
}

template<typename T>
inline void Reference<T>::destruct(MatchDoc doc) const {
    if (needDestructMatchDoc() && !isMount()) {
        getNonMountRealPointer(doc)->~T();
    }
}

template<typename T>
inline void Reference<T>::serialize(MatchDoc doc, autil::DataBuffer &dataBuffer) const  {
    _serialize(doc, dataBuffer, typename SupportSerializeTrait<T>::type());
}

template<typename T>
inline void Reference<T>::_serialize(MatchDoc doc, autil::DataBuffer &dataBuffer,
                                     SupportSerializeType) const
{
    dataBuffer.write(get(doc));
}

template<typename T>
inline void Reference<T>::_serialize(MatchDoc doc, autil::DataBuffer &dataBuffer,
                                     UnSupportSerializeType) const
{
}

template<typename T>
inline void Reference<T>::deserialize(MatchDoc doc, autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool) {
    _deserialize(doc, dataBuffer, typename SupportSerializeTrait<T>::type(), pool);
}

template<typename T>
inline void Reference<T>::_deserialize(MatchDoc doc, autil::DataBuffer &dataBuffer, SupportSerializeType, autil::mem_pool::Pool *pool)
{
    T &x = getReference(doc);
    dataBuffer.read(x);
}

template<typename T>
inline void Reference<T>::_deserialize(MatchDoc doc, autil::DataBuffer &dataBuffer,
                                       UnSupportSerializeType, autil::mem_pool::Pool *pool)
{
}

template<typename T>
inline T* Reference<T>::getPointer(MatchDoc doc) const {
    if (meta.optFlag == 0) {
        return getNonMountRealPointer(doc);
    } else {
        if (!_current) {
            return getMountRealPointer(doc);
        } else {
            MatchDoc *realDoc = (MatchDoc *)(_current->_docStorage->get(doc.getIndex()) + _current->_offset);
            return realDoc ? getRealPointer(*realDoc) : NULL;
        }
    }
}

template <typename T>
inline T& Reference<T>::getReference(MatchDoc doc) {
    const Reference<T> *cthis = this;
    const T &val = cthis->getReference(doc);
    return const_cast<T &>(val);
};

template<typename T>
inline const T& Reference<T>::getReference(MatchDoc doc) const {
    T *t = getPointer(doc);
    if (t == NULL) {
        static T defaultT;
        return defaultT;
    }
    return *t;
}

template<typename T>
inline T Reference<T>::get(MatchDoc doc) const {
    if (isMount()) {
        return getMountValue(doc);
    }
    T *t = getPointer(doc);
    if (t == nullptr) {
        return T();
    }
    return *t;
}

template<typename T>
inline T Reference<T>::getMountValue(MatchDoc doc) const {
    assert(isMount());
    char * mountAddr = nullptr;
    if (_current) {
        MatchDoc *realDoc = (MatchDoc *)(_current->_docStorage->get(doc.getIndex()) + _current->_offset);
        if (realDoc) {
            mountAddr = getMountAddress(*realDoc);
        }
    } else {
        mountAddr = getMountAddress(doc);
    }
    return toValue(mountAddr,  _mountOffset);
}

template <typename T>
T Reference<T>::toValue(const void *addr, uint64_t offset) const {
    if (unlikely(addr == nullptr)) {
        return T();
    }
    autil::PackOffset pOffset;
    pOffset.fromUInt64(offset);
    assert(!pOffset.isImpactFormat());
    return *reinterpret_cast<const T *>((const char*)addr + pOffset.getOffset());
}

#define SPECIALIZE_MULTI(T)                                             \
    template <>                                                         \
    inline T Reference<T>::toValue(const void *addr, uint64_t offset) const { \
        if (unlikely(addr == nullptr)) {                                \
            return T();                                                 \
        } else if (unlikely(isMount())) {                               \
            using single_type = typename T::value_type;                 \
            return autil::PackDataReader::readMultiValue<single_type>((const char*)addr, offset); \
        } else {                                                        \
            autil::PackOffset pOffset;                                  \
            pOffset.fromUInt64(offset);                                 \
            assert(!pOffset.isImpactFormat());                          \
            return *reinterpret_cast<const T*>((const char*)addr + pOffset.getOffset()); \
        }                                                               \
    }                                                                   \
    template <>                                                         \
    inline void Reference<T>::_deserialize(MatchDoc doc, autil::DataBuffer &dataBuffer, SupportSerializeType, autil::mem_pool::Pool *pool)\
    {                                                                   \
        T &x = getReference(doc);                                       \
        dataBuffer.read(x, pool);                                       \
    }

SPECIALIZE_MULTI(autil::MultiValueType<bool>)
SPECIALIZE_MULTI(autil::MultiValueType<char>)
SPECIALIZE_MULTI(autil::MultiValueType<int8_t>)
SPECIALIZE_MULTI(autil::MultiValueType<uint8_t>)
SPECIALIZE_MULTI(autil::MultiValueType<int16_t>)
SPECIALIZE_MULTI(autil::MultiValueType<uint16_t>)
SPECIALIZE_MULTI(autil::MultiValueType<int32_t>)
SPECIALIZE_MULTI(autil::MultiValueType<uint32_t>)
SPECIALIZE_MULTI(autil::MultiValueType<int64_t>)
SPECIALIZE_MULTI(autil::MultiValueType<uint64_t>)
SPECIALIZE_MULTI(autil::MultiValueType<float>)
SPECIALIZE_MULTI(autil::MultiValueType<double>)
SPECIALIZE_MULTI(autil::MultiValueType<autil::MultiChar>)

#undef SPECIALIZE_MULTI

template<typename T>
inline void Reference<T>::set(MatchDoc doc, const T& value) const {
    *getPointer(doc) = value;
}

template<typename T>
inline T* Reference<T>::getRealPointer(MatchDoc doc) const {
    if (!isMount()) {
        return (T*)(_docStorage->get(doc.getIndex()) + _offset);
    }
    char *mountAddr = getMountAddress(doc);
    if (mountAddr == NULL) {
        return NULL;
    }
    autil::PackOffset pOffset;
    pOffset.fromUInt64(_mountOffset);
    assert(!pOffset.isImpactFormat());
    return (T*)(mountAddr + pOffset.getOffset());
}

template<typename T>
inline T* Reference<T>::getMountRealPointer(MatchDoc doc) const {
    assert(isMount());
    char *mountAddr = getMountAddress(doc);
    if (likely(mountAddr != NULL)) {
        autil::PackOffset pOffset;
        pOffset.fromUInt64(_mountOffset);
        assert(!pOffset.isImpactFormat());
        return (T*)(mountAddr + pOffset.getOffset());
    }
    return NULL;
}

template<>
inline MatchDoc* Reference<MatchDoc>::getMountRealPointer(MatchDoc doc) const {
    assert(false);
    return NULL;
}

template<typename T>
inline T* Reference<T>::getNonMountRealPointer(MatchDoc doc) const {
    assert(!isMount());
    return (T*)(_docStorage->get(doc.getIndex()) + _offset);
}

template<>
inline MatchDoc* Reference<MatchDoc>::getPointer(MatchDoc doc) const {
    assert(!isMount());
    MatchDoc targetDoc =
        (!_current) ? doc : *(MatchDoc*)(_current->_docStorage->get(doc.getIndex()) + _current->_offset);
    return (MatchDoc*)(_docStorage->get(targetDoc.getIndex()) + _offset);
}

template<>
inline MatchDoc* Reference<MatchDoc>::getRealPointer(MatchDoc doc) const {
    assert(!isMount());
    return (MatchDoc*)(_docStorage->get(doc.getIndex()) + _offset);
}

inline MatchDoc ReferenceBase::getCurrentSubDocMaybe(MatchDoc doc) const {
    return (!_current) ? doc : _current->get(doc);
}

class ReferenceTypesWrapper {
 public:
    ReferenceTypesWrapper(ReferenceTypes *referenceTypes)
        : _referenceTypes(referenceTypes)
        , _extraReferenceTypes(nullptr)
    {}

    ~ReferenceTypesWrapper() {
        if (_extraReferenceTypes != nullptr) {
            for (ReferenceTypes::const_iterator it = _extraReferenceTypes->begin();
                 it != _extraReferenceTypes->end(); it++) {
                if (it->second.second) {
                    delete it->second.first;
                }
            }
            DELETE_AND_SET_NULL(_extraReferenceTypes);
        }
    }

    template <typename T>
    void registerType() {
        static std::string name = typeid(T).name();
        if (_referenceTypes->end() != _referenceTypes->find(name)) {
            return;
        }
        if (unlikely(_extraReferenceTypes == nullptr)) {
            _extraReferenceTypes = new ReferenceTypes();
        }
        if (_extraReferenceTypes->end() != _extraReferenceTypes->find(name)) {
            return;
        }
        (*_extraReferenceTypes)[name] = std::make_pair(new Reference<T>(), true);
    }

    bool registerType(const ReferenceBase *refType) {
        if (_referenceTypes->end() != _referenceTypes->find(refType->getVariableType())) {
            return false;
        }
        if (unlikely(_extraReferenceTypes == nullptr)) {
            _extraReferenceTypes = new ReferenceTypes();
        }
        (*_extraReferenceTypes)[refType->getVariableType()] = std::make_pair(refType->clone(), true);
        return true;
    }
    inline bool find(VariableType vt, ReferenceTypes::const_iterator &iter) const {
        iter = _referenceTypes->find(vt);
        if (likely(iter != _referenceTypes->end())) {
            return true;
        }
        if (_extraReferenceTypes != nullptr) {
            iter = _extraReferenceTypes->find(vt);
            return iter != _extraReferenceTypes->end();
        }
        return false;
    }
    inline const ReferenceTypes *getBaseType() { return _referenceTypes; }
    inline const ReferenceTypes *getExtraType() { return _extraReferenceTypes; }
    ReferenceTypesWrapper() = delete;
    ReferenceTypesWrapper(const ReferenceTypesWrapper &) = delete;
    ReferenceTypesWrapper &operator=(const ReferenceTypesWrapper &) = delete;

 private:
    ReferenceTypes *_referenceTypes;
    ReferenceTypes *_extraReferenceTypes;
};

}
#endif //ISEARCH_REFERENCE_H
