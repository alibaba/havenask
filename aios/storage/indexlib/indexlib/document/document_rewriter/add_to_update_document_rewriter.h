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

#include <functional>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/document/document.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/document/index_document/normal_document/attribute_document_field_extractor.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Bitmap.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);

namespace indexlib { namespace document {

class AddToUpdateDocumentRewriter : public document::DocumentRewriter
{
public:
    AddToUpdateDocumentRewriter();
    ~AddToUpdateDocumentRewriter();

public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
              const std::vector<config::TruncateOptionConfigPtr>& truncateOptionConfigs,
              const std::vector<indexlibv2::config::SortDescriptions>& sortDescVec);

    void Rewrite(const document::DocumentPtr& doc) override;

    std::function<void()> TryRewrite(const document::NormalDocumentPtr& doc);
    void RewriteIndexDocument(const document::NormalDocumentPtr& doc);

private:
    void AddUpdatableFields(const config::IndexPartitionSchemaPtr& schema,
                            const std::vector<indexlibv2::config::SortDescriptions>& sortDescVec);

    void FilterTruncateSortFields(const config::TruncateOptionConfigPtr& truncOptionConfig);
    void FilterTruncateProfileField(const config::TruncateProfilePtr& profile);
    void FilterTruncateStrategyField(const config::TruncateStrategyPtr& strategy);

    bool SetField(const std::string& fieldName, bool isUpdatable);

    bool SetUselessField(const std::string& uselessFieldName);

    void AllocFieldBitmap();

    bool IsSortField(const std::string& fieldName,
                     const std::vector<indexlibv2::config::SortDescriptions>& sortDescVec);

    bool NeedRewrite(const NormalDocumentPtr& doc);

private:
    config::IndexSchemaPtr mIndexSchema;
    config::AttributeSchemaPtr mAttrSchema;
    config::FieldSchemaPtr mFieldSchema;
    util::Bitmap mAttrUpdatableFieldIds;
    util::Bitmap mIndexUpdatableFieldIds;
    util::Bitmap mUselessFieldIds;
    document::AttributeDocumentFieldExtractorPtr mAttrFieldExtractor;

private:
    friend class AddToUpdateDocumentRewriterTest;
    friend class DocumentRewriterCreatorTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AddToUpdateDocumentRewriter);
}} // namespace indexlib::document
