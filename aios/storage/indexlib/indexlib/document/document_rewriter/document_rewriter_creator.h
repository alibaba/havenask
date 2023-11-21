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
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, AddToUpdateDocumentRewriter);

namespace indexlib { namespace document {

class DocumentRewriterCreator
{
public:
    DocumentRewriterCreator();
    ~DocumentRewriterCreator();

public:
    static document::DocumentRewriter* CreateTimestampRewriter(const config::IndexPartitionSchemaPtr& schema);

    static document::DocumentRewriter*
    CreateAddToUpdateDocumentRewriter(const config::IndexPartitionSchemaPtr& schema,
                                      const config::IndexPartitionOptions& options,
                                      const indexlibv2::config::SortDescriptions& sortDesc);

    static document::DocumentRewriter*
    CreateAddToUpdateDocumentRewriter(const config::IndexPartitionSchemaPtr& schema,
                                      const std::vector<config::IndexPartitionOptions>& optionVec,
                                      const std::vector<indexlibv2::config::SortDescriptions>& sortDescVec);

    static document::DocumentRewriter* CreateSectionAttributeRewriter(const config::IndexPartitionSchemaPtr& schema);

    static document::DocumentRewriter* CreatePackAttributeRewriter(const config::IndexPartitionSchemaPtr& schema);

    static document::DocumentRewriter* CreateDeleteToAddDocumentRewriter();

    static document::DocumentRewriter* CreateHashIdDocumentRewriter(const config::IndexPartitionSchemaPtr& schema);

private:
    static AddToUpdateDocumentRewriter*
    DoCreateAddToUpdateDocumentRewriter(const config::IndexPartitionSchemaPtr& schema,
                                        const std::vector<config::TruncateOptionConfigPtr>& truncateOptionConfigs,
                                        const std::vector<indexlibv2::config::SortDescriptions>& sortDescVec);

    static std::vector<config::TruncateOptionConfigPtr>
    GetTruncateOptionConfigs(const std::vector<config::IndexPartitionOptions>& optionVec);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentRewriterCreator);
}} // namespace indexlib::document
