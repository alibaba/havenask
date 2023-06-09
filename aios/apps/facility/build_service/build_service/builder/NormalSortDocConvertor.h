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
#ifndef ISEARCH_BS_NORMALSORTDOCCONVERTOR_H
#define ISEARCH_BS_NORMALSORTDOCCONVERTOR_H

#include "build_service/builder/SortDocumentConverter.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/common/SortValueConvertor.h"

namespace build_service { namespace builder {

class NormalSortDocConvertor : public SortDocumentConverter
{
public:
    NormalSortDocConvertor();
    ~NormalSortDocConvertor();

private:
    NormalSortDocConvertor(const NormalSortDocConvertor&);
    NormalSortDocConvertor& operator=(const NormalSortDocConvertor&);

public:
    bool init(const indexlibv2::config::SortDescriptions& sortDesc,
              const indexlib::config::IndexPartitionSchemaPtr& schema);

    bool convert(const indexlib::document::DocumentPtr& document, SortDocument& sortDoc) override
    {
        indexlib::document::NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(indexlib::document::NormalDocument, document);
        if (!doc || !doc->GetIndexDocument() || !doc->GetAttributeDocument()) {
            return false;
        }
        sortDoc._docType = doc->GetDocOperateType();
        sortDoc._pk = autil::MakeCString(doc->GetIndexDocument()->GetPrimaryKey(), _pool.get());
        indexlib::document::AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
        std::string sortKey;
        for (size_t i = 0; i < _sortDesc.size(); i++) {
            bool isNull = false;
            const autil::StringView& field = attrDoc->GetField(_fieldIds[i], isNull);
            sortKey += _convertFuncs[i](field, isNull);
        }
        sortDoc._sortKey = autil::MakeCString(sortKey, _pool.get());
        _dataBuffer->write(doc);
        sortDoc._docStr = autil::MakeCString(_dataBuffer->getData(), _dataBuffer->getDataLen(), _pool.get());
        _dataBuffer->clear();
        return true;
    }

    indexlib::document::DocumentPtr getDocument(const SortDocument& sortDoc) override
    {
        indexlib::document::DocumentPtr doc;
        doc = sortDoc.deserailize<indexlib::document::NormalDocumentPtr>();
        return doc;
    }

    void swap(SortDocumentConverter& other) override { SortDocumentConverter::swap(other); }

    void clear() override { _pool->reset(); }

private:
    indexlibv2::index::SortValueConvertor::ConvertFunc
    initConvertFunc(const indexlibv2::config::SortDescription& sortDesc);

private:
    indexlibv2::config::SortDescriptions _sortDesc;
    indexlib::config::AttributeSchemaPtr _attrSchema;
    std::vector<indexlibv2::index::SortValueConvertor::ConvertFunc> _convertFuncs;
    std::vector<fieldid_t> _fieldIds;
    autil::DataBuffer* _dataBuffer;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(NormalSortDocConvertor);

}} // namespace build_service::builder

#endif // ISEARCH_BS_NORMALSORTDOCCONVERTOR_H
