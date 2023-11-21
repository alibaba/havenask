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
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, DocumentRewriter);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace document {

class DocumentRewriterChain
{
public:
    DocumentRewriterChain(
        const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
        const indexlibv2::config::SortDescriptions& sortDesc = indexlibv2::config::SortDescriptions());

    ~DocumentRewriterChain();

public:
    void Init();
    void Rewrite(const document::DocumentPtr& doc);

private:
    void PushAddToUpdateRewriter();
    void PushTimestampRewriter();
    void PushSectionAttributeRewriter();
    void PushPackAttributeRewriter();
    void PushDeleteToAddDocumentRewriter();
    void PushHashIdAttributeRewriter();

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    indexlibv2::config::SortDescriptions mSortDescriptions;
    std::vector<document::DocumentRewriterPtr> mRewriters;

private:
    friend class DocumentRewriterChainTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentRewriterChain);
}} // namespace indexlib::document
