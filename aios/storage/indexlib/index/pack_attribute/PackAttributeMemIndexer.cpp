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
#include "indexlib/index/pack_attribute/PackAttributeMemIndexer.h"

#include "autil/CommonMacros.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PackAttributeMemIndexer);

PackAttributeMemIndexer::PackAttributeMemIndexer(const IndexerParameter& indexerParam)
    : MultiValueAttributeMemIndexer<char>(indexerParam)
{
}

PackAttributeMemIndexer::~PackAttributeMemIndexer() {}

Status PackAttributeMemIndexer::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                     document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    _packAttributeConfig = std::dynamic_pointer_cast<PackAttributeConfig>(indexConfig);
    _updatable = _packAttributeConfig->IsPackAttributeUpdatable();
    const auto& attrConfig = _packAttributeConfig->CreateAttributeConfig();
    if (attrConfig == nullptr) {
        AUTIL_LOG(ERROR, "attr config is nullptr, index name [%s]", indexConfig->GetIndexName().c_str());
        assert(false);
        return Status::InternalError();
    }
    if (auto status = MultiValueAttributeMemIndexer<char>::Init(attrConfig, docInfoExtractorFactory); !status.IsOK()) {
        return status;
    }
    // TOOD: need refactory
    auto packAttrId = std::make_any<packattrid_t>(_packAttributeConfig->GetPackAttrId());
    assert(docInfoExtractorFactory);
    _docInfoExtractor = docInfoExtractorFactory->CreateDocumentInfoExtractor(
        indexConfig, document::extractor::DocumentInfoExtractorType::PACK_ATTRIBUTE_FIELD, packAttrId);
    return Status::OK();
}

std::string PackAttributeMemIndexer::GetIndexName() const { return _packAttributeConfig->GetIndexName(); }
autil::StringView PackAttributeMemIndexer::GetIndexType() const { return PACK_ATTRIBUTE_INDEX_TYPE_STR; }

} // namespace indexlibv2::index
