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
#ifndef ISEARCH_BS_MODIFIEDFIELDSSUBRELATIONMODIFIER_H
#define ISEARCH_BS_MODIFIEDFIELDSSUBRELATIONMODIFIER_H

#include "build_service/common_define.h"
#include "build_service/processor/ModifiedFieldsModifier.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class ModifiedFieldsSubRelationModifier : public ModifiedFieldsModifier
{
public:
    ModifiedFieldsSubRelationModifier(fieldid_t src, SchemaType srcType, fieldid_t dst, SchemaType dstType);
    ~ModifiedFieldsSubRelationModifier();

public:
    virtual bool process(const document::ExtendDocumentPtr& document, FieldIdSet& unknownFieldIdSet);

private:
    typedef document::ExtendDocument::ExtendDocumentVec ExtendDocumentVec;
    bool matchSubDocs(const document::ExtendDocumentPtr& document, ExtendDocumentVec& matchedSubDocs);

    void addToAllDocs(const ExtendDocumentVec& docs);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ModifiedFieldsSubRelationModifier);

}} // namespace build_service::processor

#endif // ISEARCH_BS_MODIFIEDFIELDSSUBRELATIONMODIFIER_H
