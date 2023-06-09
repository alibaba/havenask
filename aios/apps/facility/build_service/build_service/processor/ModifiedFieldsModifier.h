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
#ifndef ISEARCH_BS_MODIFIEDFIELDSMODIFIER_H
#define ISEARCH_BS_MODIFIEDFIELDSMODIFIER_H

#include "build_service/common_define.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/util/Log.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"

namespace build_service { namespace processor {

class ModifiedFieldsModifier
{
public:
    enum SchemaType {
        ST_MAIN = 0,
        ST_SUB,
        ST_UNKNOWN,
    };
    typedef indexlib::document::IndexlibExtendDocument::FieldIdSet FieldIdSet;

public:
    ModifiedFieldsModifier(fieldid_t src, SchemaType srcType, fieldid_t dst, SchemaType dstType);
    virtual ~ModifiedFieldsModifier();

public:
    virtual bool process(const document::ExtendDocumentPtr& document, FieldIdSet& unknownFieldIdSet);

public:
    fieldid_t GetSrcFieldId() const { return _src; }
    fieldid_t GetDstFieldId() const { return _dst; }

protected:
    bool match(const document::ExtendDocumentPtr& document, FieldIdSet& unknownFieldIdSet) const;

protected:
    fieldid_t _src;
    SchemaType _srcType;
    fieldid_t _dst;
    SchemaType _dstType;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ModifiedFieldsModifier);

}} // namespace build_service::processor

#endif // ISEARCH_BS_MODIFIEDFIELDSMODIFIER_H
