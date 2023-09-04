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

#include <unordered_map>

#include "autil/DataBuffer.h"
#include "autil/MultiValueType.h"
#include "autil/PackDataReader.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/ToString.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorStorage.h"

namespace matchdoc {

class ReferenceBase;
class MatchDocAllocator;
typedef std::unordered_map<std::string, ReferenceBase *> ReferenceMap;
typedef std::vector<ReferenceBase *> ReferenceVector;

template <typename T>
class Reference;

namespace {
template <typename T>
static const std::string &referenceTypeName() {
    static std::string name = typeid(T).name();
    return name;
}
} // namespace

class ReferenceMeta {
public:
    ReferenceMeta();
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool);

    bool operator==(const ReferenceMeta &other) const;
    bool operator!=(const ReferenceMeta &other) const;

    bool isAutilMultiValueType() const;

    template <typename T>
    static ReferenceMeta make(bool needConstruct = ConstructTypeTraits<T>::NeedConstruct(),
                              uint32_t allocateSize = sizeof(T),
                              bool needDestruct = DestructTypeTraits<T>::NeedDestruct()) {
        ReferenceMeta meta;
        meta.vt = referenceTypeName<T>();
        meta.allocateSize = allocateSize;
        meta.needDestruct = needDestruct;
        meta.valueType = ValueTypeHelper<T>::getValueType();
        meta.valueType.setNeedConstruct(needConstruct);
        return meta;
    }

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
    ReferenceBase(const VariableType &type, const std::string &groupName);
    ReferenceBase(const VariableType &type,
                  VectorStorage *docStorage,
                  uint32_t allocateSize,
                  const std::string &groupName);
    ReferenceBase(const VariableType &type,
                  VectorStorage *docStorage,
                  uint32_t offset,
                  uint32_t allocateSize,
                  const std::string &groupName);
    ReferenceBase(const VariableType &type,
                  VectorStorage *docStorage,
                  uint32_t offset,
                  uint64_t mountOffset,
                  uint32_t allocateSize,
                  const std::string &groupName);
    virtual ~ReferenceBase();

public:
    ReferenceBase *copy() const { return copyWith(getReferenceMeta()); }
    ReferenceBase *copyWith(const ReferenceMeta &meta,
                            VectorStorage *docStorage = nullptr,
                            uint64_t offset = 0,
                            uint64_t mountOffset = INVALID_OFFSET,
                            const std::string &groupName = "") const;

    virtual ReferenceBase *createInstance() const = 0;
    virtual void construct(MatchDoc doc) const = 0;
    virtual void constructDocs(const std::vector<matchdoc::MatchDoc> &docs) const = 0;
    virtual void cloneConstruct(MatchDoc newDoc, MatchDoc cloneDoc) const = 0;
    virtual void destruct(MatchDoc doc) const = 0;
    virtual void serialize(MatchDoc doc, autil::DataBuffer &dataBuffer) const = 0;
    virtual void deserialize(MatchDoc doc, autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool) = 0;
    virtual std::string toString(MatchDoc doc) const = 0;
    virtual bool supportClone() const = 0;
    inline bool isMount() const { return _mountOffset != INVALID_OFFSET; }
    inline bool mount(MatchDoc doc, const char *baseAddress) const {
        if (likely(isMount())) {
            (const char *&)getMountAddress(getCurrentSubDocMaybe(doc)) = baseAddress;
            return true;
        }
        return false;
    }
    inline char *&getMountAddress(MatchDoc doc) const __attribute__((always_inline)) {
        char *addr = _docStorage->get(doc.getIndex()) + _offset;
        return *(char **)addr;
    }

    void attachStorage(VectorStorage *storage) { _docStorage = storage; }
    const VectorStorage *getAttachedStorage() const { return _docStorage; }
    VectorStorage *mutableStorage() { return _docStorage; }

    void setMountOffset(uint64_t offset) { _mountOffset = offset; }
    inline uint64_t getMountOffset() const { return _mountOffset; }

    void setOffset(uint64_t offset) { _offset = offset; }
    inline uint64_t getOffset() const { return _offset; }

    void setGroupName(const std::string &name) { _groupName = name; }
    inline const std::string &getGroupName() const { return _groupName; }

    void setSerializeLevel(uint8_t sl) { meta.serializeLevel = sl; }
    uint8_t getSerializeLevel() const { return meta.serializeLevel; }

    void setSerialAliasName(const std::string &name, bool useAlias = true) {
        meta.alias = name;
        meta.useAlias = useAlias;
    }
    void resetSerialAliasName() {
        meta.alias.clear();
        meta.useAlias = false;
    }
    const std::string &getSerialAliasName() const {
        if (meta.useAlias) {
            return meta.alias;
        } else {
            return meta.name;
        }
    }

    void setName(const std::string &name) { meta.name = name; }
    const std::string &getName() const { return meta.name; }

    ValueType getValueType() const { return meta.valueType; }
    void setValueType(ValueType vt) { meta.valueType = vt; }

    const VariableType &getVariableType() const { return meta.vt; }

    uint32_t getAllocateSize() const { return meta.allocateSize; }

    const ReferenceMeta &getReferenceMeta() const { return meta; }

    bool isSubDocReference() const { return _current != NULL; }

    bool needConstructMatchDoc() const { return meta.valueType.needConstruct(); }
    bool needDestructMatchDoc() const { return meta.needDestruct; }
    void setNeedDestructMatchDoc(bool needDestruct) { meta.needDestruct = needDestruct; }

    std::string toDebugString() const;
    void setIsSubReference() { meta.optFlag |= 1; }
    void setIsMountReference() { meta.optFlag |= (1 << 1); }

    bool equals(const ReferenceBase &other) const;

public: // for MatchDocAllocator, not for user
    void setCurrentRef(Reference<MatchDoc> *current) {
        _current = current;
        if (_current) {
            setIsSubReference();
        }
    }
    Reference<MatchDoc> *getCurrentRef() const { return _current; }

public: // for MatchDocJoiner, not for user
    virtual void cloneConstruct(const MatchDoc &dstDoc, const MatchDoc &srcDoc, const ReferenceBase *srcRef) const = 0;

protected:
    inline MatchDoc getCurrentSubDocMaybe(MatchDoc doc) const __attribute__((always_inline));

protected:
    uint64_t _offset;
    uint64_t _mountOffset;
    VectorStorage *_docStorage;
    Reference<MatchDoc> *_current;
    std::string _groupName;
    ReferenceMeta meta;

protected:
    template <typename T>
    friend class Reference; // need access _docStorage
};

template <typename T>
class Reference : public ReferenceBase {
public:
    Reference(bool needConstruct = true);
    Reference(uint32_t offset, uint32_t allocateSize, const std::string &groupName, bool needConstruct);
    Reference(VectorStorage *docStorage, uint32_t allocateSize, const std::string &groupName, bool needConstruct);
    Reference(VectorStorage *docStorage,
              uint32_t offset,
              uint64_t mountOffset,
              uint32_t allocateSize,
              const std::string &groupName,
              bool needConstruct);
    ~Reference();

private:
    Reference(const Reference &);
    Reference &operator=(const Reference &);

public: // for user
    inline T *getPointer(MatchDoc doc) const __attribute__((always_inline));
    inline T get(MatchDoc doc) const __attribute__((always_inline));
    inline T &getReference(MatchDoc doc) __attribute__((always_inline));
    inline const T &getReference(MatchDoc doc) const __attribute__((always_inline));
    inline void set(MatchDoc doc, const T &value) const __attribute__((always_inline));

public: // for VectorStorage, not for user
    void construct(MatchDoc doc) const override;
    void constructDocs(const std::vector<matchdoc::MatchDoc> &docs) const override;
    void cloneConstruct(MatchDoc newDoc, MatchDoc cloneDoc) const override;
    void destruct(MatchDoc doc) const override;
    void serialize(MatchDoc doc, autil::DataBuffer &dataBuffer) const override;
    void deserialize(MatchDoc doc, autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool) override;
    std::string toString(MatchDoc doc) const override;
    bool supportClone() const override;

private: // for MatchDocJoiner
    void cloneConstruct(const MatchDoc &dstDoc, const MatchDoc &srcDoc, const ReferenceBase *srcRef) const override;

private:
    ReferenceBase *createInstance() const override;

    T getMountValue(MatchDoc doc) const;
    T *getRealPointer(MatchDoc doc) const;
    T *getMountRealPointer(MatchDoc doc) const;
    T *getNonMountRealPointer(MatchDoc doc) const;
    inline T toValue(const void *addr, uint64_t offset) const __attribute__((always_inline));

private:
    friend class SubDocAccessor;
};

template <typename T>
Reference<T>::Reference(bool needConstruct) : ReferenceBase(referenceTypeName<T>(), "") {
    meta.needDestruct = DestructTypeTraits<T>::NeedDestruct();
    meta.valueType = ValueTypeHelper<T>::getValueType();
    meta.valueType.setNeedConstruct(needConstruct);
}

template <typename T>
Reference<T>::Reference(uint32_t offset, uint32_t allocateSize, const std::string &groupName, bool needConstruct)
    : Reference<T>(nullptr, offset, INVALID_OFFSET, allocateSize, groupName, needConstruct) {}

template <typename T>
Reference<T>::Reference(VectorStorage *docStorage,
                        uint32_t allocateSize,
                        const std::string &groupName,
                        bool needConstruct)
    : Reference<T>(docStorage, docStorage->getDocSize(), INVALID_OFFSET, allocateSize, groupName, needConstruct) {}

template <typename T>
inline Reference<T>::Reference(VectorStorage *docStorage,
                               uint32_t offset,
                               uint64_t mountOffset,
                               uint32_t allocateSize,
                               const std::string &groupName,
                               bool needConstruct)
    : ReferenceBase(referenceTypeName<T>(), docStorage, offset, mountOffset, allocateSize, groupName) {
    meta.needDestruct = DestructTypeTraits<T>::NeedDestruct();
    meta.valueType = ValueTypeHelper<T>::getValueType();
    meta.valueType.setNeedConstruct(needConstruct || mountOffset != INVALID_OFFSET);
}

template <typename T>
Reference<T>::~Reference() {}

template <typename T>
std::string Reference<T>::toString(MatchDoc doc) const {
    if constexpr (__detail::ToStringImpl<T>::value) {
        T value = get(doc);
        return __detail::ToStringImpl<T>::toString(value);
    } else {
        return "";
    }
}

template <typename T>
inline bool Reference<T>::supportClone() const {
    return std::is_copy_constructible<T>::value;
}

template <typename T>
inline ReferenceBase *Reference<T>::createInstance() const {
    return new Reference<T>();
}

template <typename T>
inline void Reference<T>::construct(MatchDoc doc) const {
    if (!isMount()) {
        new ((void *)getNonMountRealPointer(doc)) T();
    } else {
        getMountAddress(doc) = NULL;
    }
}

template <typename T>
inline void Reference<T>::constructDocs(const std::vector<matchdoc::MatchDoc> &docs) const {
    if (!isMount()) {
        for (auto it = docs.begin(); it != docs.end(); ++it) {
            new ((void *)getNonMountRealPointer(*it)) T();
        }
    } else {
        for (auto it = docs.begin(); it != docs.end(); ++it) {
            getMountAddress(*it) = NULL;
        }
    }
}

template <typename T>
inline void Reference<T>::cloneConstruct(MatchDoc newDoc, MatchDoc cloneDoc) const {
    if constexpr (std::is_copy_constructible<T>::value) {
        if (!isMount()) {
            new ((void *)getNonMountRealPointer(newDoc)) T(*getNonMountRealPointer(cloneDoc));
        } else {
            getMountAddress(newDoc) = getMountAddress(cloneDoc);
        }
    } else {
        construct(newDoc);
    }
}

template <typename T>
inline void
Reference<T>::cloneConstruct(const MatchDoc &dstDoc, const MatchDoc &srcDoc, const ReferenceBase *srcRef) const {
    if constexpr (std::is_copy_constructible<T>::value) {
        if (isMount()) {
            assert(srcRef->isMount());
            mount(dstDoc, srcRef->getMountAddress(srcDoc));
        } else {
            const Reference<T> *srcTypedRef = static_cast<const Reference<T> *>(srcRef);
            T val = srcTypedRef->get(srcDoc);
            new ((void *)getPointer(dstDoc)) T(val);
        }
    } else {
        new ((void *)getPointer(dstDoc)) T();
    }
}

template <typename T>
inline void Reference<T>::destruct(MatchDoc doc) const {
    if (needDestructMatchDoc() && !isMount()) {
        getNonMountRealPointer(doc)->~T();
    }
}

template <typename T>
inline void Reference<T>::serialize(MatchDoc doc, autil::DataBuffer &dataBuffer) const {
    if constexpr (autil::SerializeDeserializeTraits<T>::CAN_SERIALIZE) {
        dataBuffer.write(get(doc));
    }
}

template <typename T>
inline void Reference<T>::deserialize(MatchDoc doc, autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool) {
    if constexpr (autil::SerializeDeserializeTraits<T>::CAN_DESERIALIZE) {
        T &x = getReference(doc);
        if constexpr (autil::IsMultiType<T>::value) {
            dataBuffer.read(x, pool);
        } else {
            dataBuffer.read(x);
        }
    }
}

template <typename T>
inline T *Reference<T>::getPointer(MatchDoc doc) const {
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
inline T &Reference<T>::getReference(MatchDoc doc) {
    const Reference<T> *cthis = this;
    const T &val = cthis->getReference(doc);
    return const_cast<T &>(val);
};

template <typename T>
inline const T &Reference<T>::getReference(MatchDoc doc) const {
    T *t = getPointer(doc);
    if (t == NULL) {
        static T defaultT;
        return defaultT;
    }
    return *t;
}

template <typename T>
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

template <typename T>
inline T Reference<T>::getMountValue(MatchDoc doc) const {
    assert(isMount());
    char *mountAddr = nullptr;
    if (_current) {
        MatchDoc *realDoc = (MatchDoc *)(_current->_docStorage->get(doc.getIndex()) + _current->_offset);
        if (realDoc) {
            mountAddr = getMountAddress(*realDoc);
        }
    } else {
        mountAddr = getMountAddress(doc);
    }
    return toValue(mountAddr, _mountOffset);
}

template <typename T>
T Reference<T>::toValue(const void *addr, uint64_t offset) const {
    if (unlikely(addr == nullptr)) {
        return T();
    }
    autil::PackOffset pOffset;
    pOffset.fromUInt64(offset);
    assert(!pOffset.isImpactFormat());
    return *reinterpret_cast<const T *>((const char *)addr + pOffset.getOffset());
}

#define SPECIALIZE_MULTI(T)                                                                                            \
    template <>                                                                                                        \
    inline T Reference<T>::toValue(const void *addr, uint64_t offset) const {                                          \
        if (unlikely(addr == nullptr)) {                                                                               \
            return T();                                                                                                \
        } else if (unlikely(isMount())) {                                                                              \
            using single_type = typename T::value_type;                                                                \
            return autil::PackDataReader::readMultiValue<single_type>((const char *)addr, offset);                     \
        } else {                                                                                                       \
            autil::PackOffset pOffset;                                                                                 \
            pOffset.fromUInt64(offset);                                                                                \
            assert(!pOffset.isImpactFormat());                                                                         \
            return *reinterpret_cast<const T *>((const char *)addr + pOffset.getOffset());                             \
        }                                                                                                              \
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

template <typename T>
inline void Reference<T>::set(MatchDoc doc, const T &value) const {
    *getPointer(doc) = value;
}

template <typename T>
inline T *Reference<T>::getRealPointer(MatchDoc doc) const {
    if (!isMount()) {
        return (T *)(_docStorage->get(doc.getIndex()) + _offset);
    }
    char *mountAddr = getMountAddress(doc);
    if (mountAddr == NULL) {
        return NULL;
    }
    autil::PackOffset pOffset;
    pOffset.fromUInt64(_mountOffset);
    assert(!pOffset.isImpactFormat());
    return (T *)(mountAddr + pOffset.getOffset());
}

template <typename T>
inline T *Reference<T>::getMountRealPointer(MatchDoc doc) const {
    assert(isMount());
    char *mountAddr = getMountAddress(doc);
    if (likely(mountAddr != NULL)) {
        autil::PackOffset pOffset;
        pOffset.fromUInt64(_mountOffset);
        assert(!pOffset.isImpactFormat());
        return (T *)(mountAddr + pOffset.getOffset());
    }
    return NULL;
}

template <>
inline MatchDoc *Reference<MatchDoc>::getMountRealPointer(MatchDoc doc) const {
    assert(false);
    return NULL;
}

template <typename T>
inline T *Reference<T>::getNonMountRealPointer(MatchDoc doc) const {
    assert(!isMount());
    return (T *)(_docStorage->get(doc.getIndex()) + _offset);
}

template <>
inline MatchDoc *Reference<MatchDoc>::getPointer(MatchDoc doc) const {
    assert(!isMount());
    MatchDoc targetDoc =
        (!_current) ? doc : *(MatchDoc *)(_current->_docStorage->get(doc.getIndex()) + _current->_offset);
    return (MatchDoc *)(_docStorage->get(targetDoc.getIndex()) + _offset);
}

template <>
inline MatchDoc *Reference<MatchDoc>::getRealPointer(MatchDoc doc) const {
    assert(!isMount());
    return (MatchDoc *)(_docStorage->get(doc.getIndex()) + _offset);
}

inline MatchDoc ReferenceBase::getCurrentSubDocMaybe(MatchDoc doc) const {
    return (!_current) ? doc : _current->get(doc);
}

} // namespace matchdoc
