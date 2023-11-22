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
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/document/document_rewriter/multi_region_pack_attribute_appender.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);

namespace indexlib { namespace document {

class PackAttributeRewriter : public document::DocumentRewriter
{
public:
    PackAttributeRewriter();
    ~PackAttributeRewriter();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema);

    void Rewrite(const document::DocumentPtr& doc) override;

private:
    void RewriteAttributeDocument(const MultiRegionPackAttributeAppenderPtr& appender,
                                  const document::NormalDocumentPtr& document);

private:
    MultiRegionPackAttributeAppenderPtr mMainDocAppender;
    MultiRegionPackAttributeAppenderPtr mSubDocAppender;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributeRewriter);
}} // namespace indexlib::document
