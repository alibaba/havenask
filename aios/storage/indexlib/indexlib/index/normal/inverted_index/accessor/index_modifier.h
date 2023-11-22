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
#include "indexlib/document/index_document/normal_document/modified_tokens.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(document, IndexDocument);

namespace indexlib { namespace index {
class IndexUpdateTermIterator;
class IndexModifier
{
public:
    IndexModifier(config::IndexPartitionSchemaPtr schema, util::BuildResourceMetrics* buildResourceMetrics);
    virtual ~IndexModifier();

    IndexModifier(const IndexModifier&) = delete;
    IndexModifier& operator=(const IndexModifier&) = delete;

public:
    virtual void Update(docid_t docId, const document::IndexDocumentPtr& indexDoc) = 0;
    virtual bool UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens) = 0;
    virtual bool UpdateIndex(IndexUpdateTermIterator* iterator) = 0;

protected:
    config::IndexPartitionSchemaPtr _schema;
    util::BuildResourceMetrics* _buildResourceMetrics;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
