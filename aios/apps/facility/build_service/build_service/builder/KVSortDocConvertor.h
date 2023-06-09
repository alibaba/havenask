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
#ifndef ISEARCH_BS_KVSORTDOCCONVERTOR_H
#define ISEARCH_BS_KVSORTDOCCONVERTOR_H

#include "autil/ConstString.h"
#include "build_service/builder/SortDocumentConverter.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/base/BinaryStringUtil.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/index/common/SortValueConvertor.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(common, PlainFormatEncoder);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(common, AttributeReference);
DECLARE_REFERENCE_CLASS(common, PackAttributeFormatter);

namespace build_service { namespace builder {

class KVSortDocConvertor : public SortDocumentConverter
{
public:
    KVSortDocConvertor();
    ~KVSortDocConvertor();

private:
    KVSortDocConvertor(const KVSortDocConvertor&);
    KVSortDocConvertor& operator=(const KVSortDocConvertor&);

public:
    bool init(const indexlibv2::config::SortDescriptions& sortDesc,
              const indexlib::config::IndexPartitionSchemaPtr& schema);

    bool convert(const indexlib::document::DocumentPtr& doc, SortDocument& sortDoc) override;

    indexlib::document::DocumentPtr getDocument(const SortDocument& sortDoc) override
    {
        indexlib::document::DocumentPtr doc;
        doc = sortDoc.deserailize<indexlib::document::KVDocumentPtr>();
        return doc;
    }

    void swap(SortDocumentConverter& other) override { SortDocumentConverter::swap(other); }

    void clear() override { _pool->reset(); }

private:
    indexlib::common::PackAttributeFormatterPtr _packAttrFormatter;
    indexlib::common::PlainFormatEncoderPtr _plainFormatEncoder;
    indexlib::common::AttributeConvertorPtr _attrConvertor;

    std::vector<indexlib::common::AttributeReference*> _sortAttrRefs;
    std::vector<indexlibv2::index::SortValueConvertor::ConvertFunc> _convertFuncs;
    autil::DataBuffer* _dataBuffer;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(KVSortDocConvertor);

}} // namespace build_service::builder

#endif // ISEARCH_BS_KVSORTKEYGENERATOR_H
