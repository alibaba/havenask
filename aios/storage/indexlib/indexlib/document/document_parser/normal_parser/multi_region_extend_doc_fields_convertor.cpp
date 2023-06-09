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
#include "indexlib/document/document_parser/normal_parser/multi_region_extend_doc_fields_convertor.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, MultiRegionExtendDocFieldsConvertor);

MultiRegionExtendDocFieldsConvertor::MultiRegionExtendDocFieldsConvertor(const IndexPartitionSchemaPtr& schema)
{
    for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++) {
        ExtendDocFieldsConvertorPtr exFieldsConvertor(new ExtendDocFieldsConvertor(schema, id));
        _innerExtendFieldsConvertors.push_back(exFieldsConvertor);
    }
    assert(_innerExtendFieldsConvertors.size() >= 1);
}

void MultiRegionExtendDocFieldsConvertor::convertIndexField(const IndexlibExtendDocumentPtr& document,
                                                            const FieldConfigPtr& fieldConfig)
{
    _innerExtendFieldsConvertors[document->getRegionId()]->convertIndexField(document, fieldConfig);
}

void MultiRegionExtendDocFieldsConvertor::convertAttributeField(const IndexlibExtendDocumentPtr& document,
                                                                const FieldConfigPtr& fieldConfig,
                                                                bool emptyFieldNotEncode)
{
    _innerExtendFieldsConvertors[document->getRegionId()]->convertAttributeField(document, fieldConfig,
                                                                                 emptyFieldNotEncode);
}

void MultiRegionExtendDocFieldsConvertor::convertSummaryField(const IndexlibExtendDocumentPtr& document,
                                                              const FieldConfigPtr& fieldConfig)
{
    _innerExtendFieldsConvertors[document->getRegionId()]->convertSummaryField(document, fieldConfig);
}
}} // namespace indexlib::document
