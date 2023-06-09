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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_creator.h"
#include "indexlib/partition/operation_queue/update_field_operation.h"

DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(index, UpdateFieldExtractor);

namespace indexlib { namespace partition {

class UpdateFieldOperationCreator : public OperationCreator
{
private:
    typedef std::vector<common::AttributeConvertorPtr> FieldIdToConvertorMap;

public:
    UpdateFieldOperationCreator(const config::IndexPartitionSchemaPtr& schema);
    ~UpdateFieldOperationCreator();

public:
    bool Create(const document::NormalDocumentPtr& doc, autil::mem_pool::Pool* pool,
                OperationBase** operation) override;

private:
    bool CreateOperationItems(autil::mem_pool::Pool* pool, const document::NormalDocumentPtr& doc,
                              OperationItem** items, uint32_t* itemCount);
    void CreateAttrOperationItems(autil::mem_pool::Pool* pool, const document::AttributeDocumentPtr& doc,
                                  index::UpdateFieldExtractor& fieldExtractor, OperationItem* itemBase);
    void CreateIndexOperationItems(autil::mem_pool::Pool* pool, const document::IndexDocumentPtr& doc,
                                   const std::vector<fieldid_t>& validFields, OperationItem* itemBase);

    autil::StringView DeserializeField(fieldid_t fieldId, const autil::StringView& fieldValue);

    void InitAttributeConvertorMap(const config::IndexPartitionSchemaPtr& schema);

private:
    FieldIdToConvertorMap mFieldId2ConvertorMap;

    bool CreateUpdateFieldOperation(const document::NormalDocumentPtr& doc, autil::mem_pool::Pool* pool,
                                    OperationItem* items, uint32_t itemCount, OperationBase** operation);

private:
    friend class UpdateFieldOperationCreatorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(UpdateFieldOperationCreator);
}} // namespace indexlib::partition
