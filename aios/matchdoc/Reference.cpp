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
#include "matchdoc/Reference.h"

#include <sstream>

namespace matchdoc {

ReferenceMeta::ReferenceMeta() : allocateSize(0), needDestruct(true), useAlias(false), serializeLevel(0), optFlag(0) {}

static bool isAutilMultiValueType(ValueType valueType) {
    if (!valueType.isBuiltInType() || valueType.isStdType()) {
        return false;
    }
    return valueType.isMultiValue() || valueType.getBuiltinType() == bt_string;
}

void ReferenceMeta::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(vt);
    if (isAutilMultiValueType(valueType)) {
        // autil::MultiValueType 兼容逻辑
        // 老版本 sizeof(autil::MultiValueType) = sizeof(int64_t)
        uint32_t oldVersionAllocSize = sizeof(int64_t);
        dataBuffer.write(oldVersionAllocSize);
    } else {
        dataBuffer.write(allocateSize);
    }
    dataBuffer.write(needDestruct);
    dataBuffer.write(serializeLevel);
    if (useAlias) {
        dataBuffer.write(alias);
    } else {
        dataBuffer.write(name);
    }
    dataBuffer.write(valueType.getType());
}

void ReferenceMeta::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(vt);
    dataBuffer.read(allocateSize);
    dataBuffer.read(needDestruct);
    dataBuffer.read(serializeLevel);
    dataBuffer.read(name);
    uint32_t i = 0;
    dataBuffer.read(i);
    valueType.setType(i);
    if (isAutilMultiValueType(valueType)) {
        assert(allocateSize == sizeof(int64_t));
        allocateSize = sizeof(autil::MultiChar);
    }
}

bool ReferenceMeta::operator==(const ReferenceMeta &other) const {
    return vt == other.vt && allocateSize == other.allocateSize && needDestruct == other.needDestruct &&
           useAlias == other.useAlias && serializeLevel == other.serializeLevel && name == other.name &&
           alias == other.alias && valueType.getTypeIgnoreConstruct() == other.valueType.getTypeIgnoreConstruct();
}

bool ReferenceMeta::operator!=(const ReferenceMeta &other) const { return !(*this == other); }

///////////////////////////////////////////////////////////////////////
ReferenceBase::ReferenceBase(const VariableType &type, const std::string &groupName)
    : _offset(INVALID_OFFSET)
    , _mountOffset(INVALID_OFFSET)
    , _docStorage(nullptr)
    , _current(nullptr)
    , _groupName(groupName) {
    meta.vt = type;
}

ReferenceBase::ReferenceBase(const VariableType &type,
                             VectorStorage *docStorage,
                             uint32_t allocateSize,
                             const std::string &groupName)
    : ReferenceBase(type, docStorage, docStorage->getDocSize(), allocateSize, groupName) {}

ReferenceBase::ReferenceBase(const VariableType &type,
                             VectorStorage *docStorage,
                             uint32_t offset,
                             uint32_t allocateSize,
                             const std::string &groupName)
    : ReferenceBase(type, docStorage, offset, INVALID_OFFSET, allocateSize, groupName) {}

ReferenceBase::ReferenceBase(const VariableType &type,
                             VectorStorage *docStorage,
                             uint32_t offset,
                             uint64_t mountOffset,
                             uint32_t allocateSize,
                             const std::string &groupName)
    : _offset(offset), _mountOffset(mountOffset), _docStorage(docStorage), _current(nullptr), _groupName(groupName) {
    meta.vt = type;
    meta.allocateSize = allocateSize;
    if (isMount()) {
        setIsMountReference();
    }
}

ReferenceBase::~ReferenceBase() {}

std::string ReferenceBase::toDebugString() const {
    std::stringstream ss;
    ss << "offset (" << _offset << ") "
       << "mount offset (" << _mountOffset << ") "
       << "type (" << meta.vt << ") "
       << "allocateSize (" << meta.allocateSize << ") "
       << "needDestruct (" << meta.needDestruct << ") "
       << "serializeLevel (" << int(meta.serializeLevel) << ") "
       << "name (" << meta.name << ") "
       << "valueType (" << meta.valueType.toInt() << ") "
       << "group name (" << _groupName << ") " << std::endl;
    return ss.str();
}

ReferenceBase *ReferenceBase::copyWith(const ReferenceMeta &meta,
                                       VectorStorage *docStorage,
                                       uint64_t offset,
                                       uint64_t mountOffset,
                                       const std::string &groupName) const {
    ReferenceBase *copy = createInstance();
    copy->_offset = offset;
    copy->_mountOffset = mountOffset;
    copy->_docStorage = docStorage;
    copy->_groupName = groupName;
    copy->meta = meta;
    copy->meta.optFlag = 0; // should remove this flag
    return copy;
}

bool ReferenceBase::equals(const ReferenceBase &other) const {
    return meta == other.meta && _offset == other._offset && _mountOffset == other._mountOffset;
}

} // namespace matchdoc
