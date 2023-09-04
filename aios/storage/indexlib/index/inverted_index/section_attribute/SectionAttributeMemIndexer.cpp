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
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeMemIndexer.h"

#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/SectionAttributeConfig.h"

namespace indexlib::index {
namespace {
using indexlibv2::document::IDocumentBatch;
} // namespace
AUTIL_LOG_SETUP(indexlib.index, SectionAttributeMemIndexer);

SectionAttributeMemIndexer::SectionAttributeMemIndexer(const indexlibv2::index::IndexerParameter& indexerParam)
    : _indexerParam(indexerParam)
{
}

SectionAttributeMemIndexer::~SectionAttributeMemIndexer() {}

Status SectionAttributeMemIndexer::Init(
    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
    indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    _config = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(indexConfig);
    assert(_config);
    std::shared_ptr<indexlibv2::config::SectionAttributeConfig> sectionAttrConf = _config->GetSectionAttributeConfig();
    assert(sectionAttrConf);
    std::shared_ptr<indexlibv2::index::AttributeConfig> attrConfig =
        sectionAttrConf->CreateAttributeConfig(_config->GetIndexName());
    if (attrConfig == nullptr) {
        AUTIL_LOG(ERROR, "create attr config failed, index name[%s]", _config->GetIndexName().c_str());
        assert(false);
        return Status::InternalError();
    }

    _attrMemIndexer = std::make_unique<indexlibv2::index::MultiValueAttributeMemIndexer<char>>(_indexerParam);
    auto status = _attrMemIndexer->Init(attrConfig, docInfoExtractorFactory);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "section attribute mem indexer init failed, indexName[%s]", _config->GetIndexName().c_str());
        return status;
    }
    return Status::OK();
}

Status SectionAttributeMemIndexer::Build(IDocumentBatch* docBatch)
{
    assert(false);
    return Status::OK();
}
Status SectionAttributeMemIndexer::Build(const indexlibv2::document::IIndexFields* indexFields, size_t n)
{
    assert(false);
    return Status::OK();
}

bool SectionAttributeMemIndexer::IsDirty() const
{
    assert(_attrMemIndexer);
    return _attrMemIndexer->IsDirty();
}

Status SectionAttributeMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                                        const std::shared_ptr<file_system::Directory>& indexDirectory,
                                        const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams)
{
    assert(_attrMemIndexer);
    return _attrMemIndexer->Dump(dumpPool, indexDirectory, dumpParams);
}

void SectionAttributeMemIndexer::ValidateDocumentBatch(IDocumentBatch* docBatch) { assert(false); }

void SectionAttributeMemIndexer::UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater)
{
    _attrMemIndexer->UpdateMemUse(memUpdater);
}

std::string SectionAttributeMemIndexer::GetIndexName() const { return _config->GetIndexName(); }
autil::StringView SectionAttributeMemIndexer::GetIndexType() const
{
    assert(false);
    return {};
}

void SectionAttributeMemIndexer::EndDocument(const document::IndexDocument& indexDocument)
{
    docid_t docId = indexDocument.GetDocId();
    if (docId < 0) {
        AUTIL_LOG(WARN, "Invalid doc id(%d).", docId);
        return;
    }

    const autil::StringView& sectionAttrStr = indexDocument.GetSectionAttribute(_config->GetIndexId());
    assert(sectionAttrStr != autil::StringView::empty_instance());
    assert(_attrMemIndexer);
    _attrMemIndexer->AddField(docId, sectionAttrStr, /*isNull*/ false);
}

bool SectionAttributeMemIndexer::IsValidField(const indexlibv2::document::IIndexFields* fields)
{
    assert(false);
    return true;
}

} // namespace indexlib::index
