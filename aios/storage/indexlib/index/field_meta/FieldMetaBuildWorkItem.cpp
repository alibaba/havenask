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
#include "indexlib/index/field_meta/FieldMetaBuildWorkItem.h"

#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/index/field_meta/FieldMetaMemIndexer.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, FieldMetaBuildWorkItem);

FieldMetaBuildWorkItem::FieldMetaBuildWorkItem(SingleFieldMetaBuilder* builder,
                                               indexlibv2::document::IDocumentBatch* documentBatch)
    : BuildWorkItem(/*name=*/"_FIELD_META_", BuildWorkItemType::FIELD_META, documentBatch)
    , _builder(builder)
{
}

FieldMetaBuildWorkItem::~FieldMetaBuildWorkItem() {}

Status FieldMetaBuildWorkItem::doProcess()
{
    return _builder->Build(const_cast<indexlibv2::document::IDocumentBatch*>(_documentBatch));
}

} // namespace indexlib::index
