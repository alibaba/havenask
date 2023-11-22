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
#include <queue>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_writer.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, BuildProfilingMetrics);
DECLARE_REFERENCE_CLASS(common, DumpItem);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(index, AttributeWriter);
DECLARE_REFERENCE_CLASS(index, UpdateFieldExtractor);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index {

class InMemoryAttributeSegmentWriter
{
public:
    typedef std::vector<AttributeWriterPtr> AttrWriterVector;
    typedef std::vector<PackAttributeWriterPtr> PackAttrWriterVector;
    typedef std::vector<util::AccumulativeCounterPtr> AttrCounterVector;

public:
    InMemoryAttributeSegmentWriter(bool isVirtual = false);
    virtual ~InMemoryAttributeSegmentWriter();

public:
    void Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
              util::BuildResourceMetrics* buildResourceMetrics = NULL,
              const util::CounterMapPtr& counterMap = util::CounterMapPtr());

    bool AddDocument(const document::NormalDocumentPtr& doc);
    // must be thread-safe
    void AddDocumentAttribute(const document::AttributeDocumentPtr& attributeDocument, attrid_t attrId);
    // must be thread-safe
    void AddDocumentPackAttribute(const document::AttributeDocumentPtr& attributeDocument, packattrid_t packAttrId);

    bool UpdateDocument(docid_t docId, const document::NormalDocumentPtr& doc);
    // must be thread-safe
    void UpdateDocumentAttribute(docid_t docId, const document::AttributeDocumentPtr& attributeDocument,
                                 attrid_t attrId);
    // must be thread-safe
    void UpdateDocumentPackAttribute(docid_t docId, const document::AttributeDocumentPtr& attributeDocument,
                                     packattrid_t packAttrId);

    void CreateDumpItems(const file_system::DirectoryPtr& directory,
                         std::vector<std::unique_ptr<common::DumpItem>>& dumpItems);

    bool UpdateEncodedFieldValue(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull = false);

    void CreateInMemSegmentReaders(index_base::InMemorySegmentReader::String2AttrReaderMap& attrReaders);

    // for sub table offline join
    AttributeWriterPtr GetAttributeWriter(const std::string& attrName);

    void GetDumpEstimateFactor(std::priority_queue<double>& factors, std::priority_queue<size_t>& minMemUses);

    void SetBuildProfilingMetrics(const index::BuildProfilingMetricsPtr& metrics) { mProfilingMetrics = metrics; }

    void SetTemperatureLayer(const std::string& layer);

    AttributeWriterPtr GetAttributeWriter(attrid_t attrId) const;
    PackAttributeWriterPtr GetPackAttributeWriter(packattrid_t packAttrId) const;

protected:
    void InitAttributeWriters(const config::AttributeSchemaPtr& attrSchema,
                              const config::IndexPartitionOptions& options,
                              util::BuildResourceMetrics* buildResourceMetrics,
                              const util::CounterMapPtr& counterMapContent);

    virtual const config::AttributeSchemaPtr& GetAttributeSchema() const;

private:
    // virtual for test
    virtual AttributeWriterPtr CreateAttributeWriter(const config::AttributeConfigPtr& attrConfig,
                                                     const config::IndexPartitionOptions& options,
                                                     util::BuildResourceMetrics* buildResourceMetrics) const;

    void UpdateAttributeFields(docid_t docId, const UpdateFieldExtractor& extractor,
                               const document::AttributeDocumentPtr& attrDocument);

    void UpdatePackAttributeFields(docid_t docId);
    void UpdatePackAttributeField(docid_t docId, packattrid_t packAttrId);

protected:
    typedef std::vector<PackAttributeWriter::PackAttributeFields> PackAttrFieldsVector;

    config::IndexPartitionSchemaPtr mSchema;
    AttrWriterVector mAttrIdToAttributeWriters;
    PackAttrWriterVector mPackIdToPackAttrWriters;
    PackAttrFieldsVector mPackIdToPackFields;

    AttrCounterVector mAttrIdToCounters;
    bool mIsVirtual;
    bool mEnableCounters;
    index::BuildProfilingMetricsPtr mProfilingMetrics;

private:
    friend class InMemoryAttributeSegmentWriterTest;
    friend class MockInMemorySegmentWriter;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemoryAttributeSegmentWriter);
}} // namespace indexlib::index
