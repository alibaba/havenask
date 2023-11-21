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

#include "indexlib/index/common/BuildWorkItem.h"
#include "indexlib/index/field_meta/SingleFieldMetaBuilder.h"

namespace indexlib::index {

// User of work item should ensure lifetime of indexer and data out live the work item.
class FieldMetaBuildWorkItem : public indexlib::index::BuildWorkItem
{
public:
    FieldMetaBuildWorkItem(SingleFieldMetaBuilder* builder, indexlibv2::document::IDocumentBatch* documentBatch);
    ~FieldMetaBuildWorkItem();

public:
    Status doProcess() override;

private:
    SingleFieldMetaBuilder* _builder;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
