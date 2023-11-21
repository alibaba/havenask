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

#include <memory>
#include <stddef.h>

#include "autil/DataBuffer.h"
#include "autil/Span.h"
#include "autil/mem_pool/Pool.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/document.h"
#include "indexlib/document/kv_document/kv_document.h"

namespace build_service { namespace builder {

struct SortDocument {
public:
    SortDocument() {}
    size_t size()
    {
        size_t size = sizeof(*this) + _pk.size() + _sortKey.size() + _docStr.size();
        if (_kvDoc) {
            size += _kvDoc->EstimateMemory();
        }
        return size;
    }

    template <typename DocumentTypePtr>
    DocumentTypePtr deserailize() const
    {
        autil::DataBuffer dataBuffer(const_cast<char*>(_docStr.data()), _docStr.length());
        DocumentTypePtr doc;
        dataBuffer.read(doc);
        return doc;
    }

public:
    DocOperateType _docType;
    autil::StringView _pk;
    autil::StringView _sortKey;
    autil::StringView _docStr;
    indexlib::document::KVDocumentPtr _kvDoc;
};

class SortDocumentConverter
{
public:
    SortDocumentConverter();
    virtual ~SortDocumentConverter();

private:
    SortDocumentConverter(const SortDocumentConverter&);
    SortDocumentConverter& operator=(const SortDocumentConverter&);

public:
    size_t size() const { return _pool->getUsedBytes(); }
    virtual bool convert(const indexlib::document::DocumentPtr& doc, SortDocument& sortDoc) = 0;
    virtual indexlib::document::DocumentPtr getDocument(const SortDocument& sortDoc) = 0;
    virtual void clear() = 0;
    virtual void swap(SortDocumentConverter& other) { std::swap(_pool, other._pool); }

protected:
    typedef std::shared_ptr<autil::mem_pool::Pool> PoolPtr;
    PoolPtr _pool;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SortDocumentConverter);
}} // namespace build_service::builder
