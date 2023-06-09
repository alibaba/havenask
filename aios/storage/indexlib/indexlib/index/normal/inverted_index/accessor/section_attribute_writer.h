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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, PackageIndexConfig);
DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index, StringAttributeWriter);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);

namespace indexlib { namespace index {

// SectionAttributeWriter is a special index writer that is used by pack/expack type index(as of
// 12-15-2021) to save section related data. SectionAttributeWriter is actually an IndexWriter, though
// internally it uses an AttributeWriter to reuse its data structure.
class SectionAttributeWriter : public IndexWriter
{
public:
    SectionAttributeWriter(const config::PackageIndexConfigPtr& config);
    ~SectionAttributeWriter() {}

public:
    void Init(const config::IndexConfigPtr& indexConfig, util::BuildResourceMetrics* buildResourceMetrics) override;
    void EndDocument(const document::IndexDocument& indexDocument) override;
    void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) override;
    void GetDumpEstimateFactor(std::priority_queue<double>& factors,
                               std::priority_queue<size_t>& minMemUses) const override;
    void FillDistinctTermCount(std::shared_ptr<framework::SegmentMetrics> mSegmentMetrics) override {}
    void SetTemperatureLayer(const std::string& layer) override;

    index::AttributeSegmentReaderPtr CreateInMemAttributeReader() const;

public:
    // not implemented methods
    index::IndexSegmentReaderPtr CreateInMemReader() override;
    size_t EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter = index_base::PartitionSegmentIteratorPtr()) override;
    void AddField(const document::Field* field) override;
    void EndSegment() override;
    uint32_t GetDistinctTermCount() const override;
    void UpdateBuildResourceMetrics() override;

private:
    config::PackageIndexConfigPtr _config;
    index::StringAttributeWriterPtr _attributeWriter;
    indexid_t _indexId;

    IE_LOG_DECLARE();
};

typedef std::shared_ptr<SectionAttributeWriter> SectionAttributeWriterPtr;
}} // namespace indexlib::index
