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
#include "indexlib/index/normal/attribute/accessor/in_memory_attribute_segment_writer.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_factory.h"
#include "indexlib/index/normal/attribute/update_field_extractor.h"
#include "indexlib/index/util/build_profiling_metrics.h"
#include "indexlib/index/util/dump_item_typed.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil::mem_pool;
using namespace autil;

namespace {
class ScopeBuildProfilingReporter
{
public:
    ScopeBuildProfilingReporter(indexlib::index::BuildProfilingMetricsPtr& metric) : _beginTime(0), _metric(metric)
    {
        if (_metric) {
            _beginTime = autil::TimeUtility::currentTime();
        }
    }

    ~ScopeBuildProfilingReporter()
    {
        if (_metric) {
            int64_t endTime = autil::TimeUtility::currentTime();
            _metric->CollectAddAttributeTime(endTime - _beginTime);
        }
    }

private:
    int64_t _beginTime;
    indexlib::index::BuildProfilingMetricsPtr& _metric;
};
} // namespace

using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, InMemoryAttributeSegmentWriter);

InMemoryAttributeSegmentWriter::InMemoryAttributeSegmentWriter(bool isVirtual)
    : mIsVirtual(isVirtual)
    , mEnableCounters(false)
{
}

InMemoryAttributeSegmentWriter::~InMemoryAttributeSegmentWriter() {}

void InMemoryAttributeSegmentWriter::Init(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                                          BuildResourceMetrics* buildResourceMetrics, const CounterMapPtr& counterMap)
{
    assert(schema);
    mSchema = schema;
    const AttributeSchemaPtr& attrSchema = GetAttributeSchema();
    if (!attrSchema) {
        return;
    }
    InitAttributeWriters(attrSchema, options, buildResourceMetrics, counterMap);
}

void InMemoryAttributeSegmentWriter::InitAttributeWriters(const AttributeSchemaPtr& attrSchema,
                                                          const IndexPartitionOptions& options,
                                                          BuildResourceMetrics* buildResourceMetrics,
                                                          const CounterMapPtr& counterMap)
{
    assert(attrSchema);
    string counterPrefix;
    if (options.IsOffline() && counterMap) {
        mEnableCounters = true;
        counterPrefix = "offline.updateField.";
    }

    AttrWriterVector attributeWriters;
    uint32_t count = attrSchema->GetAttributeCount();
    mAttrIdToAttributeWriters.resize(count);
    if (mEnableCounters) {
        mAttrIdToCounters.resize(count);
    }

    AttributeConfigIteratorPtr attrConfIter = attrSchema->CreateIterator();
    for (AttributeSchema::Iterator iter = attrConfIter->Begin(); iter != attrConfIter->End(); iter++) {
        AttributeConfigPtr attrConfig = *iter;
        if (mEnableCounters) {
            string counterName = counterPrefix + attrConfig->GetAttrName();
            auto counter = counterMap->GetAccCounter(counterName);
            if (!counter) {
                INDEXLIB_FATAL_ERROR(InconsistentState, "init counter[%s] failed", counterName.c_str());
            }
            mAttrIdToCounters[attrConfig->GetAttrId()] = counter;
        }
        if (attrConfig->GetPackAttributeConfig() != NULL) {
            // skip attributes defined in pack attribute
            continue;
        }
        AttributeWriterPtr attrWriter = CreateAttributeWriter(attrConfig, options, buildResourceMetrics);

        mAttrIdToAttributeWriters[attrConfig->GetAttrId()] = attrWriter;
    }

    size_t packAttrCount = attrSchema->GetPackAttributeCount();
    mPackIdToPackAttrWriters.reserve(packAttrCount);
    mPackIdToPackFields.resize(packAttrCount);

    for (size_t i = 0; i < packAttrCount; ++i) {
        const PackAttributeConfigPtr& packConfig = attrSchema->GetPackAttributeConfig(packattrid_t(i));
        assert(packConfig);
        PackAttributeWriterPtr packAttrWriter;
        if (!packConfig->IsDisabled()) {
            packAttrWriter.reset(AttributeWriterFactory::GetInstance()->CreatePackAttributeWriter(
                packConfig, options, buildResourceMetrics));
            assert(packAttrWriter);
            mPackIdToPackFields[i].reserve(packConfig->GetAttributeConfigVec().size());
        }
        mPackIdToPackAttrWriters.push_back(packAttrWriter);
    }
}

AttributeWriterPtr InMemoryAttributeSegmentWriter::GetAttributeWriter(attrid_t attrId) const
{
    return mAttrIdToAttributeWriters[attrId];
}
PackAttributeWriterPtr InMemoryAttributeSegmentWriter::GetPackAttributeWriter(packattrid_t packAttrId) const
{
    return mPackIdToPackAttrWriters[packAttrId];
}

AttributeWriterPtr
InMemoryAttributeSegmentWriter::CreateAttributeWriter(const AttributeConfigPtr& attrConfig,
                                                      const IndexPartitionOptions& options,
                                                      BuildResourceMetrics* buildResourceMetrics) const
{
    assert(attrConfig);
    AttributeWriterPtr attrWriter(
        AttributeWriterFactory::GetInstance()->CreateAttributeWriter(attrConfig, options, buildResourceMetrics));

    assert(attrWriter);
    return attrWriter;
}

bool InMemoryAttributeSegmentWriter::AddDocument(const NormalDocumentPtr& doc)
{
    ScopeBuildProfilingReporter reporter(mProfilingMetrics);

    assert(doc);
    assert(doc->GetAttributeDocument());

    const AttributeDocumentPtr& attributeDocument = doc->GetAttributeDocument();
    docid_t docId = attributeDocument->GetDocId();
    const AttributeSchemaPtr& attrSchema = GetAttributeSchema();
    AttributeConfigIteratorPtr attrConfigs = attrSchema->CreateIterator(IndexStatus::is_normal);
    for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++) {
        const AttributeConfigPtr& attrConfig = *iter;
        assert(attrConfig);
        if (attrConfig->GetPackAttributeConfig() != NULL) {
            continue;
        }

        int64_t beginTs = 0;
        if (mProfilingMetrics) {
            beginTs = autil::TimeUtility::currentTime();
        }

        fieldid_t fieldId = attrConfig->GetFieldId();
        bool isNull = false;
        const StringView& field = attributeDocument->GetField(fieldId, isNull);
        attrid_t attrId = attrConfig->GetAttrId();
        mAttrIdToAttributeWriters[attrId]->AddField(docId, field, isNull);
        if (mProfilingMetrics) {
            int64_t endTime = autil::TimeUtility::currentTime();
            mProfilingMetrics->CollectAddSingleAttributeTime(attrId, endTime - beginTs);
        }
    }

    size_t packAttrCount = attrSchema->GetPackAttributeCount();
    for (packattrid_t i = 0; i < (packattrid_t)packAttrCount; ++i) {
        const StringView& packField = attributeDocument->GetPackField(i);
        if (mPackIdToPackAttrWriters[i]) {
            mPackIdToPackAttrWriters[i]->AddField(docId, packField);
        }
    }

    return true;
}

void InMemoryAttributeSegmentWriter::AddDocumentAttribute(const AttributeDocumentPtr& attributeDocument,
                                                          attrid_t attrId)
{
    docid_t docId = attributeDocument->GetDocId();
    const AttributeSchemaPtr& attrSchema = GetAttributeSchema();
    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(attrId);
    assert(attrConfig && attrConfig->IsNormal());

    if (attrConfig->GetPackAttributeConfig() != NULL) {
        return;
    }

    fieldid_t fieldId = attrConfig->GetFieldId();
    bool isNull = false;
    const StringView& field = attributeDocument->GetField(fieldId, isNull);
    mAttrIdToAttributeWriters[attrId]->AddField(docId, field, isNull);
}

void InMemoryAttributeSegmentWriter::AddDocumentPackAttribute(const AttributeDocumentPtr& attributeDocument,
                                                              packattrid_t packAttrId)
{
    assert(packAttrId < mPackIdToPackAttrWriters.size());

    docid_t docId = attributeDocument->GetDocId();
    const StringView& packField = attributeDocument->GetPackField(packAttrId);
    mPackIdToPackAttrWriters[packAttrId]->AddField(docId, packField);
}

bool InMemoryAttributeSegmentWriter::UpdateDocument(docid_t docId, const NormalDocumentPtr& doc)
{
    assert(doc);
    assert(doc->GetAttributeDocument());

    if (mIsVirtual) {
        IE_LOG(ERROR, "virtual attribute not support updataDocument!");
        return false;
    }

    const AttributeDocumentPtr& attrDocument = doc->GetAttributeDocument();
    UpdateFieldExtractor extractor(mSchema);
    extractor.Init(attrDocument);

    if (extractor.GetFieldCount() == 0) {
        return true;
    }
    UpdateAttributeFields(docId, extractor, attrDocument);
    return true;
}

void InMemoryAttributeSegmentWriter::UpdateDocumentAttribute(docid_t docId,
                                                             const AttributeDocumentPtr& attributeDocument,
                                                             attrid_t attrId)
{
    assert(attributeDocument);
    assert(!mIsVirtual);
    if (unlikely(mIsVirtual)) {
        IE_LOG(ERROR, "virtual attribute not support updataDocument!");
        return;
    }

    const AttributeSchemaPtr& attrSchema = GetAttributeSchema();
    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(attrId);
    AttributeWriterPtr& writer = mAttrIdToAttributeWriters[attrId];
    assert(attrConfig && attrConfig->IsNormal() && writer);

    bool isNull = false;
    autil::StringView value;
    if (!UpdateFieldExtractor::GetFieldValue(mSchema, attributeDocument, attrConfig->GetFieldId(), &isNull, &value)) {
        return;
        ;
    }
    bool ret = writer->UpdateField(docId, value, isNull);
    if (mEnableCounters && ret) {
        mAttrIdToCounters[attrId]->Increase(1);
    }
    assert(ret);
}

void InMemoryAttributeSegmentWriter::UpdateDocumentPackAttribute(docid_t docId,
                                                                 const AttributeDocumentPtr& attributeDocument,
                                                                 packattrid_t packAttrId)
{
    assert(!mIsVirtual);
    if (unlikely(mIsVirtual)) {
        IE_LOG(ERROR, "virtual attribute not support updataDocument!");
        return;
    }

    const AttributeSchemaPtr& attrSchema = GetAttributeSchema();
    const PackAttributeConfigPtr& packAttrConfig = attrSchema->GetPackAttributeConfig(packAttrId);
    assert(packAttrConfig && mPackIdToPackAttrWriters[packAttrId]);
    for (const AttributeConfigPtr& attrConfig : packAttrConfig->GetAttributeConfigVec()) {
        if (!attrConfig->IsNormal()) {
            continue;
        }
        bool isNull = false;
        autil::StringView value;
        if (!UpdateFieldExtractor::GetFieldValue(mSchema, attributeDocument, attrConfig->GetFieldId(), &isNull,
                                                 &value)) {
            continue;
        }
        mPackIdToPackFields[packAttrId].push_back(make_pair(attrConfig->GetAttrId(), value));
    }
    UpdatePackAttributeField(docId, packAttrId);
}

void InMemoryAttributeSegmentWriter::UpdateAttributeFields(docid_t docId, const UpdateFieldExtractor& extractor,
                                                           const AttributeDocumentPtr& attrDocument)
{
    UpdateFieldExtractor::Iterator iter = extractor.CreateIterator();
    while (iter.HasNext()) {
        fieldid_t fieldId = INVALID_FIELDID;
        bool isNull = false;
        const StringView& field = iter.Next(fieldId, isNull);
        UpdateEncodedFieldValue(docId, fieldId, field, isNull);
    }

    UpdatePackAttributeFields(docId);
}

bool InMemoryAttributeSegmentWriter::UpdateEncodedFieldValue(docid_t docId, fieldid_t fieldId,
                                                             const autil::StringView& value, bool isNull)
{
    const AttributeSchemaPtr& attrSchema = GetAttributeSchema();
    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfigByFieldId(fieldId);
    if (!attrConfig) {
        IE_LOG(ERROR, "fieldId [%d] is not in Attribute!", fieldId);
        ERROR_COLLECTOR_LOG(ERROR, "fieldId [%d] is not in Attribute!", fieldId);
        return false;
    }
    if (attrConfig->IsDisabled()) {
        return true;
    }
    PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    if (packAttrConfig) {
        mPackIdToPackFields[packAttrConfig->GetPackAttrId()].push_back(make_pair(attrConfig->GetAttrId(), value));
    } else {
        auto attrId = attrConfig->GetAttrId();
        AttributeWriterPtr& writer = mAttrIdToAttributeWriters[attrId];
        if (writer) {
            auto ret = writer->UpdateField(docId, value, isNull);
            if (mEnableCounters && ret) {
                mAttrIdToCounters[attrId]->Increase(1);
            }
            return ret;
        }
        IE_LOG(ERROR, "Update field failed! attribute writer for [%s] is NULL", attrConfig->GetAttrName().c_str());
        ERROR_COLLECTOR_LOG(ERROR, "Update field failed! attribute writer for [%s] is NULL",
                            attrConfig->GetAttrName().c_str());
    }
    return false;
}

void InMemoryAttributeSegmentWriter::CreateInMemSegmentReaders(
    index_base::InMemorySegmentReader::String2AttrReaderMap& attrReaders)
{
    for (uint32_t i = 0; i < mAttrIdToAttributeWriters.size(); ++i) {
        if (mAttrIdToAttributeWriters[i]) {
            AttributeSegmentReaderPtr attrSegReader = mAttrIdToAttributeWriters[i]->CreateInMemReader();
            AttributeConfigPtr attrConfig = mAttrIdToAttributeWriters[i]->GetAttrConfig();
            attrReaders.insert(make_pair(attrConfig->GetAttrName(), attrSegReader));
        }
    }

    for (size_t i = 0; i < mPackIdToPackAttrWriters.size(); ++i) {
        if (mPackIdToPackAttrWriters[i]) {
            AttributeSegmentReaderPtr attrSegReader = mPackIdToPackAttrWriters[i]->CreateInMemReader();
            AttributeConfigPtr attrConfig = mPackIdToPackAttrWriters[i]->GetAttrConfig();
            attrReaders.insert(make_pair(attrConfig->GetAttrName(), attrSegReader));
        }
    }
}

void InMemoryAttributeSegmentWriter::CreateDumpItems(const file_system::DirectoryPtr& directory,
                                                     vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    for (AttrWriterVector::iterator it = mAttrIdToAttributeWriters.begin(); it != mAttrIdToAttributeWriters.end();
         ++it) {
        if (*it) {
            dumpItems.push_back(std::make_unique<AttributeDumpItem>(directory, *it));
        }
    }

    for (PackAttrWriterVector::iterator it = mPackIdToPackAttrWriters.begin(); it != mPackIdToPackAttrWriters.end();
         ++it) {
        if (*it) {
            dumpItems.push_back(std::make_unique<AttributeDumpItem>(directory, *it));
        }
    }
}

AttributeWriterPtr InMemoryAttributeSegmentWriter::GetAttributeWriter(const string& attrName)
{
    const AttributeSchemaPtr& attrSchema = GetAttributeSchema();
    if (!attrSchema) {
        return AttributeWriterPtr();
    }

    AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(attrName);
    if (!attrConfig) {
        return AttributeWriterPtr();
    }

    attrid_t attrId = attrConfig->GetAttrId();
    assert(attrId < mAttrIdToAttributeWriters.size());
    return mAttrIdToAttributeWriters[attrId];
}

const AttributeSchemaPtr& InMemoryAttributeSegmentWriter::GetAttributeSchema() const
{
    if (mIsVirtual) {
        return mSchema->GetVirtualAttributeSchema();
    }
    return mSchema->GetAttributeSchema();
}

void InMemoryAttributeSegmentWriter::GetDumpEstimateFactor(priority_queue<double>& factors,
                                                           priority_queue<size_t>& minMemUses)
{
    for (size_t i = 0; i < mAttrIdToAttributeWriters.size(); ++i) {
        if (mAttrIdToAttributeWriters[i]) {
            mAttrIdToAttributeWriters[i]->GetDumpEstimateFactor(factors, minMemUses);
        }
    }
    for (size_t i = 0; i < mPackIdToPackAttrWriters.size(); ++i) {
        if (mPackIdToPackAttrWriters[i]) {
            mPackIdToPackAttrWriters[i]->GetDumpEstimateFactor(factors, minMemUses);
        }
    }
}

void InMemoryAttributeSegmentWriter::UpdatePackAttributeFields(docid_t docId)
{
    for (size_t i = 0; i < mPackIdToPackFields.size(); ++i) {
        UpdatePackAttributeField(docId, i);
    }
}

void InMemoryAttributeSegmentWriter::UpdatePackAttributeField(docid_t docId, packattrid_t packAttrId)
{
    if (mPackIdToPackFields[packAttrId].empty()) {
        return;
    }
    if (!mPackIdToPackAttrWriters[packAttrId]) {
        return;
    }
    bool ret = mPackIdToPackAttrWriters[packAttrId]->UpdateEncodeFields(docId, mPackIdToPackFields[packAttrId]);

    if (mEnableCounters && ret) {
        for (const auto& inPackAttr : mPackIdToPackFields[packAttrId]) {
            mAttrIdToCounters[inPackAttr.first]->Increase(1);
        }
    }
    mPackIdToPackFields[packAttrId].clear();
}

void InMemoryAttributeSegmentWriter::SetTemperatureLayer(const string& layer)
{
    for (size_t i = 0; i < mAttrIdToAttributeWriters.size(); ++i) {
        if (mAttrIdToAttributeWriters[i]) {
            mAttrIdToAttributeWriters[i]->SetTemperatureLayer(layer);
        }
    }

    for (size_t i = 0; i < mPackIdToPackAttrWriters.size(); ++i) {
        if (mPackIdToPackAttrWriters[i]) {
            mPackIdToPackAttrWriters[i]->SetTemperatureLayer(layer);
        }
    }
}
}} // namespace indexlib::index
