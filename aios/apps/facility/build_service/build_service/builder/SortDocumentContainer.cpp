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
#include "build_service/builder/SortDocumentContainer.h"

#include "autil/DataBuffer.h"
#include "build_service/builder/KKVSortDocConvertor.h"
#include "build_service/builder/KKVSortDocSorter.h"
#include "build_service/builder/KVSortDocConvertor.h"
#include "build_service/builder/KVSortDocSorter.h"
#include "build_service/builder/NormalSortDocConvertor.h"
#include "build_service/builder/NormalSortDocSorter.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::document;

namespace build_service { namespace builder {
BS_LOG_SETUP(builder, SortDocumentContainer);

class DocumentComparatorWrapper
{
public:
    typedef SortDocumentContainer::DocumentVector DocumentVector;

public:
    DocumentComparatorWrapper(const DocumentVector& documentVec) : _documentVec(documentVec) {}
    ~DocumentComparatorWrapper() {}

public:
    inline bool operator()(uint32_t left, uint32_t right)
    {
        const SortDocument& leftDoc = _documentVec[left];
        const SortDocument& rightDoc = _documentVec[right];
        assert(leftDoc._sortKey.size() == rightDoc._sortKey.size());
        int r = memcmp(leftDoc._sortKey.data(), rightDoc._sortKey.data(), rightDoc._sortKey.size());
        if (r < 0) {
            return true;
        } else if (r > 0) {
            return false;
        } else {
            return left < right;
        }
    }

private:
    const DocumentVector& _documentVec;
    BS_LOG_DECLARE();
};

BS_LOG_SETUP(builder, DocumentComparatorWrapper);
const size_t SortDocumentContainer::RESERVED_QUEUE_SIZE = 128 * 1024 * 1024;

SortDocumentContainer::SortDocumentContainer()
    : _hasPrimaryKey(false)
    , _hasSub(false)
    , _hasInvertedIndexUpdate(false)
    , _processCursor(0)
    , _sortQueueMemUse(0)
{
    _documentVec.reserve(RESERVED_QUEUE_SIZE);
    _orderVec.reserve(RESERVED_QUEUE_SIZE);
}

SortDocumentContainer::~SortDocumentContainer() {}

bool SortDocumentContainer::init(const indexlibv2::config::SortDescriptions& sortDesc,
                                 const IndexPartitionSchemaPtr& schema, const common::Locator& firstLocator)
{
    _firstLocator = firstLocator;
    _lastLocator = firstLocator;
    _hasPrimaryKey = false;
    const IndexSchemaPtr& indexSchemaPtr = schema->GetIndexSchema();
    if (indexSchemaPtr) {
        _hasPrimaryKey = indexSchemaPtr->HasPrimaryKeyIndex();
        _hasInvertedIndexUpdate = indexSchemaPtr->AnyIndexUpdatable();
    }
    _hasSub = schema->GetSubIndexPartitionSchema().get();
    _tableType = schema->GetTableType();
    if (_tableType == indexlib::tt_kkv) {
        _converter.reset(new KKVSortDocConvertor());
        resetSortResource();
        return true;
    }

    if (_tableType == indexlib::tt_kv) {
        KVSortDocConvertor* convertor = new KVSortDocConvertor();
        _converter.reset(convertor);
        if (!convertor->init(sortDesc, schema)) {
            return false;
        }
        resetSortResource();
        return true;
    }

    NormalSortDocConvertor* convertor = new NormalSortDocConvertor();
    _converter.reset(convertor);
    if (!convertor->init(sortDesc, schema)) {
        return false;
    }
    resetSortResource();
    return true;
}

void SortDocumentContainer::sortDocument()
{
    if (empty()) {
        return;
    }
    if (!_hasPrimaryKey) {
        for (size_t i = 0; i < _documentVec.size(); ++i) {
            _orderVec.push_back(i);
        }
        std::sort(_orderVec.begin(), _orderVec.end(), DocumentComparatorWrapper(_documentVec));
        return;
    }
    _sortDocumentSorter->sort(_orderVec);
    resetSortResource();
}

void SortDocumentContainer::clear()
{
    _processCursor = 0;
    _sortQueueMemUse = 0;
    _documentVec.clear();
    _orderVec.clear();
    _converter->clear();
    _firstLocator = _lastLocator;
}

void SortDocumentContainer::resetSortResource()
{
    _sortDocumentSorter.reset(NULL);
    if (_tableType == indexlib::tt_kkv) {
        _sortDocumentSorter.reset(new KKVSortDocSorter());
    } else if (_tableType == indexlib::tt_kv) {
        _sortDocumentSorter.reset(new KVSortDocSorter(_documentVec));
    } else {
        _sortDocumentSorter.reset(new NormalSortDocSorter(_documentVec, (NormalSortDocConvertor*)_converter.get(),
                                                          _hasSub, _hasInvertedIndexUpdate));
    }
}

DocumentPtr SortDocumentContainer::operator[](size_t idx)
{
    assert(idx < sortedDocCount());
    const SortDocument& sortDoc = _documentVec[_orderVec[idx]];
    DocumentPtr doc = _converter->getDocument(sortDoc);
    common::Locator locator = _firstLocator;
    if (idx + 1 == sortedDocCount()) {
        locator = _lastLocator;
    }
    doc->SetLocator(locator.Serialize());
    return doc;
}

void SortDocumentContainer::updateLocator(const StringView& locatorStr)
{
    common::Locator locator;
    if (!locator.Deserialize(locatorStr)) {
        string errorMsg = "deserialize locator[" + string(locatorStr.data(), locatorStr.size()) + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return;
    }
    _lastLocator = locator;
}

void SortDocumentContainer::printMemoryInfo() const
{
    BS_LOG(INFO, "SortDocumentContainer: sortQueue memory use [%lu], sorter memory use [%lu]", _sortQueueMemUse,
           _sortDocumentSorter->getMemUse());
}

}} // namespace build_service::builder
