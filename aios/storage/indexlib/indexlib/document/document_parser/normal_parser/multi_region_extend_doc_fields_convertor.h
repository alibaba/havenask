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

#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/normal_parser/extend_doc_fields_convertor.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class MultiRegionExtendDocFieldsConvertor
{
public:
    MultiRegionExtendDocFieldsConvertor(const config::IndexPartitionSchemaPtr& schema);
    ~MultiRegionExtendDocFieldsConvertor() {}

private:
    MultiRegionExtendDocFieldsConvertor(const MultiRegionExtendDocFieldsConvertor&);
    MultiRegionExtendDocFieldsConvertor& operator=(const MultiRegionExtendDocFieldsConvertor&);

public:
    void convertIndexField(const IndexlibExtendDocumentPtr& document, const config::FieldConfigPtr& fieldConfig);
    void convertAttributeField(const IndexlibExtendDocumentPtr& document, const config::FieldConfigPtr& fieldConfig,
                               bool emptyFieldNotEncode = false);
    void convertSummaryField(const IndexlibExtendDocumentPtr& document, const config::FieldConfigPtr& fieldConfig);

private:
    std::vector<ExtendDocFieldsConvertorPtr> _innerExtendFieldsConvertors;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionExtendDocFieldsConvertor);
}} // namespace indexlib::document
