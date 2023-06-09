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

#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/document/normal/rewriter/NormalDocumentRewriterBase.h"
#include "indexlib/index/inverted_index/config/TruncateOptionConfig.h"
#include "indexlib/index/inverted_index/config/TruncateStrategy.h"
#include "indexlib/util/Bitmap.h"

namespace indexlibv2::config {
class TabletSchema;
class IIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::document {

class NormalDocument;
class AttributeDocumentFieldExtractor;

class AddToUpdateDocumentRewriter : public NormalDocumentRewriterBase
{
public:
    AddToUpdateDocumentRewriter();
    ~AddToUpdateDocumentRewriter();

public:
    Status Init(const std::shared_ptr<config::TabletSchema>& schema,
                const std::vector<std::shared_ptr<config::TruncateOptionConfig>>& truncateOptionConfigs,
                const std::vector<config::SortDescriptions>& sortDescVec);
    std::function<void()> TryRewrite(const std::shared_ptr<NormalDocument>& doc);
    void RewriteIndexDocument(const std::shared_ptr<NormalDocument>& doc);

protected:
    Status RewriteOneDoc(const std::shared_ptr<NormalDocument>& doc) override;

private:
    void AllocFieldBitmap();
    void AddUpdatableFields(const std::vector<config::SortDescriptions>& sortDescVec);
    void SetUselessField(fieldid_t fieldId);
    void SetField(fieldid_t fieldId, bool inAttribute, bool inInvertedIndex);
    bool SetField(const std::string& fieldName);
    std::pair<bool, bool> CheckAttribute(const std::shared_ptr<config::IIndexConfig>& indexConfig) const;
    std::pair<bool, bool> CheckInvertedIndex(const std::shared_ptr<config::IIndexConfig>& indexConfig) const;
    bool NeedRewrite(const std::shared_ptr<NormalDocument>& doc);

    void FilterTruncateSortFields(const std::shared_ptr<config::TruncateOptionConfig>& truncateOptionConfig);
    void FilterTruncateProfileField(const std::shared_ptr<config::TruncateProfile>& profile);
    void FilterTruncateStrategyField(const std::shared_ptr<config::TruncateStrategy>& strategy);
    bool IsSortField(const std::string& fieldName, const std::vector<config::SortDescriptions>& sortDescVec);

private:
    std::shared_ptr<config::TabletSchema> _schema;
    indexlib::util::Bitmap _attributeUpdatableFieldIds;
    indexlib::util::Bitmap _invertedIndexUpdatableFieldIds;
    indexlib::util::Bitmap _uselessFieldIds;
    std::unique_ptr<AttributeDocumentFieldExtractor> _attrFieldExtractor;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
