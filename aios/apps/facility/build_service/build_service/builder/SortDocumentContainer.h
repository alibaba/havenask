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

#include <algorithm>
#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <utility>
#include <vector>

#include "autil/Span.h"
#include "build_service/builder/SortDocumentConverter.h"
#include "build_service/builder/SortDocumentSorter.h"
#include "build_service/common/Locator.h"
#include "build_service/util/Log.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/locator.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace builder {

class SortDocumentContainer
{
public:
    typedef std::vector<SortDocument> DocumentVector;

public:
    SortDocumentContainer();
    ~SortDocumentContainer();

private:
    SortDocumentContainer(const SortDocumentContainer&);
    SortDocumentContainer& operator=(const SortDocumentContainer&);

public:
    bool init(const indexlibv2::config::SortDescriptions& sortDesc,
              const indexlib::config::IndexPartitionSchemaPtr& attrSchemaPtr, const common::Locator& locator);

public:
    inline void pushDocument(const indexlib::document::DocumentPtr& doc)
    {
        updateLocator(doc->GetLocator().GetLocator());
        doc->ResetLocator(); // set locator null, reduce memory
        SortDocument sortDoc;
        if (!_converter->convert(doc, sortDoc)) {
            return;
        }

        if (!_hasPrimaryKey) {
            _documentVec.push_back(sortDoc);
            _sortQueueMemUse += sortDoc.size();
            return;
        }

        _documentVec.push_back(sortDoc);
        _sortQueueMemUse += sortDoc.size();
        uint32_t currentCursor = _documentVec.size() - 1;
        _sortDocumentSorter->push(sortDoc, currentCursor);
    }

    void swap(SortDocumentContainer& other)
    {
        std::swap(_hasPrimaryKey, other._hasPrimaryKey);
        std::swap(_hasSub, other._hasSub);
        std::swap(_hasInvertedIndexUpdate, other._hasInvertedIndexUpdate);
        std::swap(_processCursor, other._processCursor);
        std::swap(_sortQueueMemUse, other._sortQueueMemUse);
        std::swap(_documentVec, other._documentVec);
        std::swap(_orderVec, other._orderVec);
        _converter.swap(other._converter);
        std::swap(_firstLocator, other._firstLocator);
        std::swap(_lastLocator, other._lastLocator);
        std::swap(_tableType, other._tableType);
        resetSortResource();
        other.resetSortResource();
    }
    void sortDocument();
    void clear();
    inline size_t allDocCount() const { return _documentVec.size(); }
    inline size_t sortedDocCount() const { return _orderVec.size(); }
    inline size_t memUse() const { return (_sortQueueMemUse + _sortDocumentSorter->getMemUse()) / 1024 / 1024; }
    inline size_t nonDocumentMemUse() const { return _sortDocumentSorter->getMemUse() / 1024 / 1024; }
    inline bool empty() const { return _documentVec.empty(); }

    void printMemoryInfo() const;

    indexlib::document::DocumentPtr operator[](size_t idx);

    inline void incProcessCursor()
    {
        _processCursor++;
        assert(_processCursor <= _orderVec.size());
    }
    inline size_t getUnprocessedCount() const { return _orderVec.size() - _processCursor; }

private:
    void updateLocator(const autil::StringView& locatorStr);
    void resetSortResource();

private:
    static const size_t RESERVED_QUEUE_SIZE;

private:
    bool _hasPrimaryKey;
    bool _hasSub;
    bool _hasInvertedIndexUpdate;
    size_t _processCursor;
    size_t _sortQueueMemUse;
    DocumentVector _documentVec;
    std::vector<uint32_t> _orderVec;
    SortDocumentConverterPtr _converter;
    common::Locator _firstLocator;
    common::Locator _lastLocator;
    std::unique_ptr<SortDocumentSorter> _sortDocumentSorter;
    indexlib::TableType _tableType;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::builder
