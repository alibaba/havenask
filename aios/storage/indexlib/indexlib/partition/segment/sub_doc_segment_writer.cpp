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
#include "indexlib/partition/segment/sub_doc_segment_writer.h"

#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/default_attribute_field_appender.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/modifier/in_memory_segment_modifier.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::plugin;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SubDocSegmentWriter);

SubDocSegmentWriter::SubDocSegmentWriter(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options)
    : SegmentWriter(schema, options)
    , _subSchema(schema->GetSubIndexPartitionSchema())
{
    assert(_subSchema);
}

SubDocSegmentWriter::~SubDocSegmentWriter()
{
    mMainWriter.reset();
    mSubWriter.reset();
    mBuildResourceMetrics.reset();
}

SubDocSegmentWriter::SubDocSegmentWriter(const SubDocSegmentWriter& other, BuildingSegmentData& segmentData,
                                         bool isShared)
    : SegmentWriter(other)
    , mMainWriter(other.mMainWriter->CloneWithNewSegmentData(segmentData, isShared))
    , mSubWriter(other.mSubWriter->CloneWithNewSegmentData(segmentData, isShared))
    , mMainModifier(other.mMainModifier)
{
    if (!isShared) {
        mMainModifier = mMainWriter->GetInMemorySegmentModifier();
    }
}

void SubDocSegmentWriter::Init(const index_base::SegmentData& segmentData,
                               const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics,
                               const CounterMapPtr& counterMap, const PluginManagerPtr& pluginManager,
                               const PartitionSegmentIteratorPtr& partSegIter)
{
    mBuildResourceMetrics.reset(new BuildResourceMetrics());
    mBuildResourceMetrics->Init();
    mCounterMap = counterMap;
    mSegmentMetrics = metrics;
    mMainWriter = CreateSingleSegmentWriter(mSchema, mOptions, segmentData, metrics, pluginManager, partSegIter, false);

    SegmentDataPtr subSegmentData = segmentData.GetSubSegmentData();
    PartitionSegmentIteratorPtr subPartSegIter;
    if (partSegIter) {
        subPartSegIter.reset(partSegIter->CreateSubPartitionSegmentItertor());
    }
    assert(subSegmentData);
    mSubWriter = CreateSingleSegmentWriter(mSchema->GetSubIndexPartitionSchema(), mOptions, *subSegmentData, metrics,
                                           pluginManager, subPartSegIter, true);

    mMainModifier = mMainWriter->GetInMemorySegmentModifier();
}

SubDocSegmentWriter* SubDocSegmentWriter::CloneWithNewSegmentData(BuildingSegmentData& segmentData, bool isShared) const
{
    return new SubDocSegmentWriter(*this, segmentData, isShared);
}

SingleSegmentWriterPtr SubDocSegmentWriter::CreateSingleSegmentWriter(
    const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
    const SegmentData& segmentData, const std::shared_ptr<framework::SegmentMetrics>& metrics,
    const PluginManagerPtr& pluginManager, const index_base::PartitionSegmentIteratorPtr& partSegIter, bool isSub)
{
    assert(schema);
    assert(mBuildResourceMetrics);
    SingleSegmentWriterPtr segmentWriter(new SingleSegmentWriter(schema, options, isSub));
    segmentWriter->Init(segmentData, metrics, mBuildResourceMetrics, mCounterMap, pluginManager, partSegIter);
    return segmentWriter;
}

// AddDocument for main+sub type doc cannot be converted to update doc.
bool SubDocSegmentWriter::AddDocument(const document::DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);

    // step 1: add main doc, not set main join
    bool success = mMainWriter->AddDocument(document);
    // success is guaranteed by preprocess checks.
    assert(success);

    // step 2: add sub doc, set sub join
    const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocs.size(); i++) {
        const NormalDocumentPtr& subDoc = subDocs[i];
        success = mSubWriter->AddDocument(subDoc);
        assert(success);
    }

    // // step 3: update main join
    // update main join value is no longer needed.
    // We can calculate which sub docs are valid and added before calling AddDocument(subDoc).
    // UpdateMainDocJoinValue();

    return success;
}

// void SubDocSegmentWriter::UpdateMainDocJoinValue(docid_t mainDocId, joinValue)
// {
//     const SegmentInfoPtr& mainSegmentInfo = mMainWriter->GetSegmentInfo();
//     docid_t updateMainDocId = (docid_t)mainSegmentInfo->docCount - 1;

//     const SegmentInfoPtr& subSegmentInfo = mSubWriter->GetSegmentInfo();
//     string updateJoinValueStr = StringUtil::toString(subSegmentInfo->docCount);

//     string mainConvertedJoinValue = _mainJoinFieldConverter->Encode(updateJoinValueStr);

//     mMainModifier->UpdateEncodedFieldValue(updateMainDocId, _mainJoinFieldId, StringView(mainConvertedJoinValue));
// }

void SubDocSegmentWriter::CollectSegmentMetrics()
{
    mMainWriter->CollectSegmentMetrics();
    mSubWriter->CollectSegmentMetrics();
}

void SubDocSegmentWriter::EndSegment() { mMainWriter->EndSegment(); }

void SubDocSegmentWriter::CreateDumpItems(const file_system::DirectoryPtr& directory,
                                          vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    mMainWriter->CreateDumpItems(directory, dumpItems);
}

index_base::InMemorySegmentReaderPtr SubDocSegmentWriter::CreateInMemSegmentReader()
{
    return mMainWriter->CreateInMemSegmentReader();
}

bool SubDocSegmentWriter::IsDirty() const { return mMainWriter->IsDirty() || mSubWriter->IsDirty(); }

const SegmentInfoPtr& SubDocSegmentWriter::GetSegmentInfo() const { return mMainWriter->GetSegmentInfo(); }

size_t SubDocSegmentWriter::EstimateInitMemUse(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                               const config::IndexPartitionSchemaPtr& schema,
                                               const config::IndexPartitionOptions& options,
                                               const plugin::PluginManagerPtr& pluginManager,
                                               const index_base::PartitionSegmentIteratorPtr& partSegIter)
{
    size_t memUse = 0;
    memUse += SingleSegmentWriter::EstimateInitMemUse(metrics, schema, options, pluginManager, partSegIter);

    index_base::PartitionSegmentIteratorPtr subSegIter;
    if (partSegIter) {
        subSegIter.reset(partSegIter->CreateSubPartitionSegmentItertor());
    }
    memUse += SingleSegmentWriter::EstimateInitMemUse(metrics, schema->GetSubIndexPartitionSchema(), options,
                                                      pluginManager, subSegIter);
    return memUse;
}
}} // namespace indexlib::partition
