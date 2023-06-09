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
#include "indexlib/partition/segment/single_segment_writer.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/normal/attribute/accessor/in_memory_attribute_segment_writer.h"
#include "indexlib/index/normal/attribute/default_attribute_field_appender.h"
#include "indexlib/index/normal/deletionmap/deletion_map_dump_item.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/in_memory_index_segment_writer.h"
#include "indexlib/index/normal/source/in_mem_source_segment_reader.h"
#include "indexlib/index/normal/source/source_writer_impl.h"
#include "indexlib/index/normal/summary/summary_writer_impl.h"
#include "indexlib/index/segment_metrics_updater/multi_segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/temperature_segment_metrics_updater.h"
#include "indexlib/index/util/dump_item_typed.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/modifier/in_memory_segment_modifier.h"
#include "indexlib/plugin/plugin_factory_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/BuildResourceCalculator.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::document;
using namespace indexlib::common;
using namespace indexlib::plugin;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SingleSegmentWriter);

SingleSegmentWriter::SingleSegmentWriter(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                                         bool isSub)
    : SegmentWriter(schema, options)
    , mIsSub(isSub)
{
}

SingleSegmentWriter::~SingleSegmentWriter()
{
    _summaryWriter.reset();
    _inMemoryIndexSegmentWriter.reset();
    _inMemoryAttributeSegmentWriter.reset();
    _virtualInMemoryAttributeSegmentWriter.reset();
    mDeletionMapSegmentWriter.reset();
    mModifier.reset();
}

void SingleSegmentWriter::InitWriter(segmentid_t segmentId,
                                     const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics,
                                     const BuildResourceMetricsPtr& buildResourceMetrics,
                                     const CounterMapPtr& counterMap, const PluginManagerPtr& pluginManager,
                                     const PartitionSegmentIteratorPtr& partSegIter)
{
    // init index writer
    _inMemoryIndexSegmentWriter.reset(new InMemoryIndexSegmentWriter());
    _inMemoryIndexSegmentWriter->Init(segmentId, mSchema, mOptions, metrics, mBuildResourceMetrics.get(), pluginManager,
                                      partSegIter);
    // init attribute writer
    AttributeSchemaPtr attributeSchema = mSchema->GetAttributeSchema();
    if (attributeSchema) {
        _inMemoryAttributeSegmentWriter.reset(new InMemoryAttributeSegmentWriter(false));
        _inMemoryAttributeSegmentWriter->Init(mSchema, mOptions, mBuildResourceMetrics.get(), counterMap);
    }

    // init virtual attribute writer
    AttributeSchemaPtr virtualAttributeSchema = mSchema->GetVirtualAttributeSchema();
    if (virtualAttributeSchema) {
        _virtualInMemoryAttributeSegmentWriter.reset(new InMemoryAttributeSegmentWriter(true));
        _virtualInMemoryAttributeSegmentWriter->Init(mSchema, mOptions, mBuildResourceMetrics.get());
    }

    // init summary writer
    if (mSchema->NeedStoreSummary()) {
        _summaryWriter.reset(new SummaryWriterImpl());
        IE_LOG(INFO, "Open summary writer.");
        _summaryWriter->Init(mSchema->GetSummarySchema(), mBuildResourceMetrics.get());
    }

    // init source writer
    if (mSchema->GetSourceSchema()) {
        _sourceWriter.reset(new SourceWriterImpl());
        IE_LOG(INFO, "Open source writer.");
        _sourceWriter->Init(mSchema->GetSourceSchema(), mBuildResourceMetrics.get());
    }
    // init deletionmap writer
    mDeletionMapSegmentWriter.reset(new DeletionMapSegmentWriter);
    mDeletionMapSegmentWriter->Init(DeletionMapSegmentWriter::GetDeletionMapSize(), mBuildResourceMetrics.get());
    mModifier = CreateInMemSegmentModifier();
}

void SingleSegmentWriter::Init(const SegmentData& segmentData,
                               const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics,
                               const BuildResourceMetricsPtr& buildResourceMetrics, const CounterMapPtr& counterMap,
                               const PluginManagerPtr& pluginManager, const PartitionSegmentIteratorPtr& partSegIter)
{
    mCounterMap = counterMap;
    mSegmentMetrics = metrics;
    assert(mSegmentMetrics);
    mSegmentData = segmentData;
    mCurrentSegmentInfo.reset(new SegmentInfo(*segmentData.GetSegmentInfo()));
    mCurrentSegmentInfo->schemaId = mSchema->GetSchemaVersionId();
    if (!buildResourceMetrics) {
        mBuildResourceMetrics.reset(new BuildResourceMetrics());
        mBuildResourceMetrics->Init();
    } else {
        mBuildResourceMetrics = buildResourceMetrics;
    }
    InitWriter(segmentData.GetSegmentId(), metrics, buildResourceMetrics, counterMap, pluginManager, partSegIter);
    if (!mIsSub) {
        const auto& updaterConfigs = mOptions.GetBuildConfig().GetSegmentMetricsUpdaterConfig();
        std::unique_ptr<MultiSegmentMetricsUpdater> updater(new MultiSegmentMetricsUpdater(util::MetricProviderPtr()));
        updater->Init(mSchema, mOptions, updaterConfigs, pluginManager);
        if (updater->GetSegmentMetricUpdaterSize() != 0) {
            mSegMetricsUpdater.reset(updater.release());
        }
    }
}

// void SingleSegmentWriter::Open(indexlibv2::framework::SegmentMeta* segmentMeta, const util::CounterMapPtr&
// counterMap)
// {
//     mCounterMap = counterMap;
//     mSegmentMetrics = segmentMeta->segmentMetrics;
//     mSegmentMeta = segmentMeta;
//     assert(mSegmentMetrics);
//     mCurrentSegmentInfo = mSegmentMeta->segmentInfo;
//     mCurrentSegmentInfo->schemaId = mSchema->GetSchemaVersionId();
//     mSegmentMeta->segmentInfo->schemaId = mCurrentSegmentInfo->schemaId;

//     // TODO(yonghao.fyh): confirm build resource metrics init
//     mBuildResourceMetrics.reset(new BuildResourceMetrics());
//     mBuildResourceMetrics->Init();

//     InitWriter(mSegmentMeta->segmentId, mSegmentMeta->segmentMetrics, mBuildResourceMetrics, counterMap,
//                PluginManagerPtr(), PartitionSegmentIteratorPtr());

//     if (!mIsSub) {
//         const auto& updaterConfigs = mOptions.GetBuildConfig().GetSegmentMetricsUpdaterConfig();
//         std::unique_ptr<MultiSegmentMetricsUpdater> updater(new
//         MultiSegmentMetricsUpdater(util::MetricProviderPtr())); updater->Init(mSchema, mOptions, updaterConfigs,
//         PluginManagerPtr()); if (updater->GetSegmentMetricUpdaterSize() != 0) {
//             mSegMetricsUpdater.reset(updater.release());
//         }
//     }
// }

SingleSegmentWriter* SingleSegmentWriter::CloneWithNewSegmentData(BuildingSegmentData& segmentData, bool isShared) const
{
    SingleSegmentWriter* newSegmentWriter = new SingleSegmentWriter(*this);
    newSegmentWriter->mSegmentData = segmentData;
    if (!isShared) {
        // reinit deletion map segment writer in private mode
        DeletionMapSegmentWriterPtr newDeletionMapSegmentWriter(new DeletionMapSegmentWriter);
        newDeletionMapSegmentWriter->Init(DeletionMapSegmentWriter::GetDeletionMapSize(), mBuildResourceMetrics.get());
        newDeletionMapSegmentWriter->MergeBitmap(mDeletionMapSegmentWriter->GetBitmap(), true);
        newSegmentWriter->mDeletionMapSegmentWriter = newDeletionMapSegmentWriter;
        newSegmentWriter->mModifier = newSegmentWriter->CreateInMemSegmentModifier();
    }
    return newSegmentWriter;
}

size_t SingleSegmentWriter::EstimateInitMemUse(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics,
                                               const config::IndexPartitionSchemaPtr& schema,
                                               const config::IndexPartitionOptions& options,
                                               const plugin::PluginManagerPtr& pluginManager,
                                               const index_base::PartitionSegmentIteratorPtr& partSegIter)
{
    return InMemoryIndexSegmentWriter::EstimateInitMemUse(segmentMetrics, schema, options, pluginManager, partSegIter);
}

bool SingleSegmentWriter::AddDocument(const DocumentPtr& document)
{
    assert(document);
    assert(mSchema->GetIndexSchema());

    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);

    // assert(doc->GetDocId() != INVALID_DOCID); // for some ut
    docid_t globalDocId = doc->GetDocId();
    doc->SetDocId(mCurrentSegmentInfo->docCount);

    if (_summaryWriter) {
        _summaryWriter->AddDocument(doc->GetSummaryDocument());
    }

    if (_sourceWriter) {
        _sourceWriter->AddDocument(doc->GetSourceDocument());
    }

    if (_inMemoryAttributeSegmentWriter) {
        _inMemoryAttributeSegmentWriter->AddDocument(doc);
    }

    if (_virtualInMemoryAttributeSegmentWriter) {
        _virtualInMemoryAttributeSegmentWriter->AddDocument(doc);
    }

    _inMemoryIndexSegmentWriter->AddDocument(doc);
    doc->SetDocId(globalDocId);
    EndAddDocument(document);
    return true;
}

void SingleSegmentWriter::EndAddDocument(const DocumentPtr& document)
{
    mCurrentSegmentInfo->docCount = mCurrentSegmentInfo->docCount + 1;
    if (mSegMetricsUpdater) {
        mSegMetricsUpdater->Update(document);
    }
}

void SingleSegmentWriter::CollectSegmentMetrics()
{
    _inMemoryIndexSegmentWriter->CollectSegmentMetrics();
    assert(mSegmentMetrics);
    if (mSegMetricsUpdater) {
        auto jsonMap = mSegMetricsUpdater->Dump();
        mSegmentMetrics->SetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP, jsonMap);
    }
}

void SingleSegmentWriter::EndSegment()
{
    if (!IsDirty()) {
        return;
    }

    index_base::SegmentTemperatureMeta temperatureMeta;
    if (GetSegmentTemperatureMeta(temperatureMeta)) {
        if (_inMemoryAttributeSegmentWriter) {
            _inMemoryAttributeSegmentWriter->SetTemperatureLayer(temperatureMeta.segTemperature);
        }
        _inMemoryIndexSegmentWriter->SetTemperatureLayer(temperatureMeta.segTemperature);
        if (_summaryWriter) {
            _summaryWriter->SetTemperatureLayer(temperatureMeta.segTemperature);
        }
        if (_sourceWriter) {
            _sourceWriter->SetTemperatureLayer(temperatureMeta.segTemperature);
        }
    }

    _inMemoryIndexSegmentWriter->EndSegment();
    mDeletionMapSegmentWriter->EndSegment(mCurrentSegmentInfo->docCount);
    IE_LOG(INFO, "End segment: doc count [%lu]", (uint64_t)mCurrentSegmentInfo->docCount);
}

bool SingleSegmentWriter::HasCustomizeMetrics(
    std::shared_ptr<indexlib::framework::SegmentGroupMetrics>& customizeMetrics) const
{
    if (!mSegMetricsUpdater) {
        return false;
    }
    customizeMetrics = mSegmentMetrics->GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
    return true;
}

void SingleSegmentWriter::CreateDumpItems(const file_system::DirectoryPtr& directory,
                                          vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    if (!IsDirty()) {
        return;
    }

    assert(!directory->IsExist(SEGMETN_METRICS_FILE_NAME));
    mSegmentMetrics->Store(directory);

    CreateDeletionMapDumpItems(directory, dumpItems);
    CreateSummaryDumpItems(directory, dumpItems);
    CreateSourceDumpItems(directory, dumpItems);
    CreateAttributeDumpItems(directory, dumpItems);
    CreateIndexDumpItems(directory, dumpItems);
}

void SingleSegmentWriter::CreateIndexDumpItems(const file_system::DirectoryPtr& directory,
                                               vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    file_system::DirectoryPtr indexDirectory = directory->MakeDirectory(INDEX_DIR_NAME);
    _inMemoryIndexSegmentWriter->CreateDumpItems(indexDirectory, dumpItems);
}

void SingleSegmentWriter::CreateAttributeDumpItems(const file_system::DirectoryPtr& directory,
                                                   vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    if (!_inMemoryAttributeSegmentWriter && !_virtualInMemoryAttributeSegmentWriter) {
        return;
    }

    file_system::DirectoryPtr attributeDirectory = directory->MakeDirectory(ATTRIBUTE_DIR_NAME);

    if (_inMemoryAttributeSegmentWriter) {
        _inMemoryAttributeSegmentWriter->CreateDumpItems(attributeDirectory, dumpItems);
    }

    if (_virtualInMemoryAttributeSegmentWriter) {
        _virtualInMemoryAttributeSegmentWriter->CreateDumpItems(attributeDirectory, dumpItems);
    }
}

void SingleSegmentWriter::CreateSummaryDumpItems(const file_system::DirectoryPtr& directory,
                                                 vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    if (!_summaryWriter) {
        return;
    }
    file_system::DirectoryPtr summaryDirectory = directory->MakeDirectory(SUMMARY_DIR_NAME);
    std::unique_ptr<DumpItem> summaryDumpItem(new SummaryDumpItem(summaryDirectory, _summaryWriter));
    dumpItems.push_back(std::move(summaryDumpItem));
}

void SingleSegmentWriter::CreateSourceDumpItems(const file_system::DirectoryPtr& directory,
                                                vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    if (!_sourceWriter) {
        return;
    }
    file_system::DirectoryPtr sourceDir = directory->MakeDirectory(SOURCE_DIR_NAME);
    std::unique_ptr<DumpItem> sourceDumpItem(new SourceDumpItem(sourceDir, _sourceWriter));
    dumpItems.push_back(std::move(sourceDumpItem));
}

void SingleSegmentWriter::CreateDeletionMapDumpItems(const file_system::DirectoryPtr& directory,
                                                     vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    assert(mDeletionMapSegmentWriter);
    file_system::DirectoryPtr delMapDirectory = directory->MakeDirectory(DELETION_MAP_DIR_NAME);
    std::unique_ptr<DumpItem> delMapDumpItem(
        new DeletionMapDumpItem(delMapDirectory, mDeletionMapSegmentWriter, mSegmentData.GetSegmentId()));
    dumpItems.push_back(std::move(delMapDumpItem));
}

InMemorySegmentReaderPtr SingleSegmentWriter::CreateInMemSegmentReader()
{
    assert(_inMemoryIndexSegmentWriter);
    MultiFieldIndexSegmentReaderPtr indexSegmentReader = _inMemoryIndexSegmentWriter->CreateInMemSegmentReader();

    InMemorySegmentReader::String2AttrReaderMap attrReaders;
    if (_inMemoryAttributeSegmentWriter) {
        _inMemoryAttributeSegmentWriter->CreateInMemSegmentReaders(attrReaders);
    }
    if (_virtualInMemoryAttributeSegmentWriter) {
        _virtualInMemoryAttributeSegmentWriter->CreateInMemSegmentReaders(attrReaders);
    }

    SummarySegmentReaderPtr summaryReader;
    if (_summaryWriter) {
        summaryReader = _summaryWriter->CreateInMemSegmentReader();
    }

    InMemSourceSegmentReaderPtr sourceReader;
    if (_sourceWriter) {
        sourceReader = _sourceWriter->CreateInMemSegmentReader();
    }

    InMemDeletionMapReaderPtr deletionMapReader;
    if (mDeletionMapSegmentWriter) {
        deletionMapReader = mDeletionMapSegmentWriter->CreateInMemDeletionMapReader(mCurrentSegmentInfo.get(),
                                                                                    mSegmentData.GetSegmentId());
    }

    InMemorySegmentReaderPtr reader(new InMemorySegmentReader(mSegmentData.GetSegmentId()));

    reader->Init(indexSegmentReader, attrReaders, summaryReader, sourceReader, deletionMapReader, *mCurrentSegmentInfo);

    return reader;
}

bool SingleSegmentWriter::IsDirty() const
{
    return mCurrentSegmentInfo->docCount > 0 || mDeletionMapSegmentWriter->IsDirty();
}

InMemorySegmentModifierPtr SingleSegmentWriter::CreateInMemSegmentModifier() const
{
    InMemorySegmentModifierPtr segModifier(new InMemorySegmentModifier);
    segModifier->Init(mDeletionMapSegmentWriter, _inMemoryAttributeSegmentWriter, _inMemoryIndexSegmentWriter);
    return segModifier;
}

bool SingleSegmentWriter::GetSegmentTemperatureMeta(index_base::SegmentTemperatureMeta& temperatureMeta)
{
    if (!mSegMetricsUpdater) {
        return false;
    }
    MultiSegmentMetricsUpdaterPtr multiUpdater = static_pointer_cast<MultiSegmentMetricsUpdater>(mSegMetricsUpdater);
    SegmentMetricsUpdaterPtr baseUpdater =
        multiUpdater->GetMetricUpdate(TemperatureSegmentMetricsUpdater::UPDATER_NAME);
    if (!baseUpdater) {
        IE_LOG(ERROR, "cannot get temperature mertric updater");
        return false;
    }
    TemperatureSegmentMetricsUpdaterPtr temperatureUpdater =
        static_pointer_cast<TemperatureSegmentMetricsUpdater>(baseUpdater);
    temperatureMeta.segmentId = mSegmentData.GetSegmentId();
    assert(temperatureUpdater);
    temperatureUpdater->GetSegmentTemperatureMeta(temperatureMeta);
    return true;
}

void SingleSegmentWriter::SetBuildProfilingMetrics(const BuildProfilingMetricsPtr& metrics)
{
    mProfilingMetrics = metrics;
    if (_inMemoryIndexSegmentWriter) {
        _inMemoryIndexSegmentWriter->SetBuildProfilingMetrics(metrics);
    }
    if (_inMemoryAttributeSegmentWriter) {
        _inMemoryAttributeSegmentWriter->SetBuildProfilingMetrics(metrics);
    }
    if (_summaryWriter) {
        _summaryWriter->SetBuildProfilingMetrics(metrics);
    }
}

index::SummaryWriterPtr SingleSegmentWriter::GetSummaryWriter() const { return _summaryWriter; }
index::SourceWriterPtr SingleSegmentWriter::GetSourceWriter() const { return _sourceWriter; }
index::InMemoryIndexSegmentWriterPtr SingleSegmentWriter::GetInMemoryIndexSegmentWriter() const
{
    return _inMemoryIndexSegmentWriter;
}

}} // namespace indexlib::partition
