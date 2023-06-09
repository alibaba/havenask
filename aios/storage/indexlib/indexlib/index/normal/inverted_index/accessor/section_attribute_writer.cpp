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
#include "indexlib/index/normal/inverted_index/accessor/section_attribute_writer.h"

#include "indexlib/common/field_format/attribute/string_attribute_convertor.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_writer.h"
#include "indexlib/util/NumericUtil.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib { namespace index {

IE_LOG_SETUP(index, SectionAttributeWriter);

SectionAttributeWriter::SectionAttributeWriter(const config::PackageIndexConfigPtr& config)
    : _config(config)
    , _indexId(INVALID_INDEXID)
{
    assert(_config);
    _indexId = _config->GetIndexId();
}

void SectionAttributeWriter::Init(const config::IndexConfigPtr& indexConfig,
                                  util::BuildResourceMetrics* buildResourceMetrics)

{
    config::SectionAttributeConfigPtr sectionAttrConf = _config->GetSectionAttributeConfig();
    assert(sectionAttrConf);
    config::AttributeConfigPtr attrConfig = sectionAttrConf->CreateAttributeConfig(_config->GetIndexName());

    common::AttributeConvertorPtr attrConvertor(
        new common::StringAttributeConvertor(attrConfig->IsUniqEncode(), attrConfig->GetAttrName()));

    _attributeWriter.reset(new index::StringAttributeWriter(attrConfig));
    _attributeWriter->SetAttrConvertor(attrConvertor);
    _attributeWriter->Init(FSWriterParamDeciderPtr(), buildResourceMetrics);
}

void SectionAttributeWriter::EndDocument(const document::IndexDocument& indexDocument)
{
    docid_t docId = indexDocument.GetDocId();
    if (docId < 0) {
        IE_LOG(WARN, "Invalid doc id(%d).", docId);
        return;
    }

    const autil::StringView& sectionAttrStr = indexDocument.GetSectionAttribute(_indexId);
    assert(sectionAttrStr != autil::StringView::empty_instance());
    assert(_attributeWriter);
    _attributeWriter->AddField(docId, sectionAttrStr);
}

void SectionAttributeWriter::Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool)
{
    assert(_attributeWriter);
    _attributeWriter->Dump(dir, dumpPool);
}

index::AttributeSegmentReaderPtr SectionAttributeWriter::CreateInMemAttributeReader() const
{
    return _attributeWriter->CreateInMemReader();
}

void SectionAttributeWriter::GetDumpEstimateFactor(std::priority_queue<double>& factors,
                                                   std::priority_queue<size_t>& minMemUses) const
{
    if (_attributeWriter) {
        _attributeWriter->GetDumpEstimateFactor(factors, minMemUses);
    }
}

void SectionAttributeWriter::SetTemperatureLayer(const std::string& layer)
{
    if (_attributeWriter) {
        _attributeWriter->SetTemperatureLayer(layer);
    }
}

// not implemented methods
index::IndexSegmentReaderPtr SectionAttributeWriter::CreateInMemReader()
{
    assert(false);
    return nullptr;
}
size_t SectionAttributeWriter::EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                                                  const index_base::PartitionSegmentIteratorPtr& segIter)
{
    assert(false);
    return 0;
}

void SectionAttributeWriter::AddField(const document::Field* field) {}
void SectionAttributeWriter::EndSegment() {}
uint32_t SectionAttributeWriter::GetDistinctTermCount() const
{
    assert(false);
    return 0;
}
void SectionAttributeWriter::UpdateBuildResourceMetrics() { assert(false); }

}} // namespace indexlib::index
