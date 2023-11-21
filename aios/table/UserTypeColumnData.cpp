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
#include "table/UserTypeColumnData.h"

#include "autil/DataBuffer.h"
#include "autil/Log.h"

namespace table {
AUTIL_DECLARE_AND_SETUP_LOGGER(table, UserTypeColumnData);

UserTypeColumnData::~UserTypeColumnData() {
    if (!_ref || !_ref->needDestructMatchDoc()) {
        return;
    }
    // TODO: should remove lazy construct
    for (uint32_t i = 0; i < _data->getSize(); ++i) {
        _ref->destruct(matchdoc::MatchDoc(i));
    }
}

void UserTypeColumnData::resize(uint32_t size) {
    if (!_data) {
        return;
    }
    uint32_t sizeBefore = _data->getSize();
    _data->growToSize(size);
    if (_ref->needConstructMatchDoc()) {
        uint32_t sizeAfter = _data->getSize();
        for (uint32_t i = sizeBefore; i < sizeAfter; ++i) {
            _ref->construct(matchdoc::MatchDoc(i));
        }
    }
}

bool UserTypeColumnData::merge(BaseColumnData &other) {
    auto &typedOther = static_cast<UserTypeColumnData &>(other);
    if (!_ref || !_data || !typedOther._ref || !typedOther._data) {
        return false;
    }
    if (_ref->getOffset() != typedOther._ref->getOffset()) {
        return false;
    }
    _data->append(*typedOther._data);
    if (_ref->needDestructMatchDoc()) {
        typedOther.reset();
    }
    return true;
}

bool UserTypeColumnData::copy(size_t startIndex, const BaseColumnData &other, size_t srcStartIndex, size_t count) {
    AUTIL_LOG(ERROR, "UserTypeColumnData::copy is not supported");
    return false;
}

std::unique_ptr<BaseColumnData> UserTypeColumnData::share(const std::vector<Row> *rows) {
    AUTIL_LOG(ERROR, "UserTypeColumnData::copy is not supported");
    return nullptr;
}

std::string UserTypeColumnData::toString(size_t rowIndex) const {
    auto row = getRow(rowIndex);
    return _ref->toString(row);
}

std::string UserTypeColumnData::toOriginString(size_t rowIndex) const { return toString(rowIndex); }

std::unique_ptr<matchdoc::ReferenceBase> UserTypeColumnData::toReference() const {
    assert(_ref != nullptr);
    auto ref = _ref->copyWith(_ref->getReferenceMeta(), _ref->mutableStorage(), _ref->getOffset());
    // MatchDocAllocator::~MatchDocAllocator() will force destruct all docs
    // set this flag to avoid double free
    ref->setNeedDestructMatchDoc(false);
    return std::unique_ptr<matchdoc::ReferenceBase>(ref);
}

std::unique_ptr<UserTypeColumnData> UserTypeColumnData::fromReference(matchdoc::ReferenceBase *ref) {
    auto storage = ref->mutableStorage();
    if (!storage) {
        return nullptr;
    }
    auto columnData = std::make_unique<UserTypeColumnData>();
    columnData->_data = storage->shared_from_this();
    columnData->_ref.reset(ref->copyWith(ref->getReferenceMeta(), columnData->_data.get(), ref->getOffset()));
    columnData->_ref->setGroupName(ref->getName());
    return columnData;
}

void UserTypeColumnData::serialize(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows) const {
    for (const auto &row : rows) {
        _ref->serialize(row, dataBuffer);
    }
}

void UserTypeColumnData::deserialize(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows) {
    for (const auto &row : rows) {
        _ref->deserialize(row, dataBuffer, _data->getPool());
    }
}

uint32_t UserTypeColumnData::capacity() const { return _data->getSize(); }
uint32_t UserTypeColumnData::usedBytes(uint32_t rowCount) const { return _data->getDocSize() * rowCount; }
uint32_t UserTypeColumnData::allocatedBytes() const { return _data->getDocSize() * capacity(); }

void UserTypeColumnData::reset() {
    _ref.reset();
    _data.reset();
}

} // namespace table
