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
#ifndef __INDEXLIB_ATTRIBUTE_DOCUMENT_FIELD_EXTRACTOR_H
#define __INDEXLIB_ATTRIBUTE_DOCUMENT_FIELD_EXTRACTOR_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(document, AttributeDocument);
DECLARE_REFERENCE_CLASS(common, PackAttributeFormatter);

namespace indexlib { namespace document {

class AttributeDocumentFieldExtractor
{
public:
    AttributeDocumentFieldExtractor();
    ~AttributeDocumentFieldExtractor();

public:
    bool Init(const config::AttributeSchemaPtr& attributeSchema);
    autil::StringView GetField(const document::AttributeDocumentPtr& attrDoc, fieldid_t fid,
                               autil::mem_pool::Pool* pool);

private:
    std::vector<common::PackAttributeFormatterPtr> mPackFormatters;
    config::AttributeSchemaPtr mAttrSchema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeDocumentFieldExtractor);
}} // namespace indexlib::document

#endif //__INDEXLIB_ATTRIBUTE_DOCUMENT_FIELD_EXTRACTOR_H
