#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/pack_directory.h"
#include "indexlib/index/dump_item_typed.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/in_memory_attribute_segment_writer.h"
#include "indexlib/index/normal/attribute/format/default_attribute_field_appender.h"
#include "indexlib/index/normal/deletionmap/deletion_map_dump_item.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/in_memory_index_segment_writer.h"
#include "indexlib/index/normal/summary/summary_writer_impl.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/index/segment_metrics_updater.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index/multi_segment_metrics_updater.h"
#include "indexlib/partition/segment/in_memory_segment_modifier.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/memory_control/build_resource_calculator.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/util/mmap_allocator.h"
#include "indexlib/plugin/plugin_factory_loader.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SingleSegmentWriter);

SingleSegmentWriter::SingleSegmentWriter(
    const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options, bool isSub)
    : SegmentWriter(schema, options)
    , mIsSub(isSub)
{
}

SingleSegmentWriter::~SingleSegmentWriter()
{
    mSummaryWriter.reset();
    mIndexWriters.reset();
    mAttributeWriters.reset();
    mVirtualAttributeWriters.reset();
    mDeletionMapSegmentWriter.reset();
    mDefaultAttrFieldAppender.reset();
    mModifier.reset();
}

void SingleSegmentWriter::Init(const SegmentData& segmentData,
                               const SegmentMetricsPtr& metrics,
                               const BuildResourceMetricsPtr& buildResourceMetrics,
                               const CounterMapPtr& counterMap,
                               const PluginManagerPtr& pluginManager,
                               const PartitionSegmentIteratorPtr& partSegIter)
{
    mCounterMap = counterMap;
    mSegmentMetrics = metrics;
    mSegmentData = segmentData;
    mCurrentSegmentInfo.reset(new SegmentInfo(segmentData.GetSegmentInfo()));
    mCurrentSegmentInfo->schemaId = mSchema->GetSchemaVersionId();
    if (!buildResourceMetrics)
    {
        mBuildResourceMetrics.reset(new BuildResourceMetrics());
        mBuildResourceMetrics->Init();
    }
    else
    {
        mBuildResourceMetrics = buildResourceMetrics;
    }

    // init index writer
    mIndexWriters.reset(new InMemoryIndexSegmentWriter());
    mIndexWriters->Init(mSchema, mOptions, metrics,
                        mBuildResourceMetrics.get(),
                        pluginManager, partSegIter);
    // init attribute writer
    AttributeSchemaPtr attributeSchema = mSchema->GetAttributeSchema();
    if (attributeSchema)
    {
        mAttributeWriters.reset(new InMemoryAttributeSegmentWriter(false));
        mAttributeWriters->Init(mSchema, mOptions, mBuildResourceMetrics.get(), counterMap);
    }

    // init virtual attribute writer
    AttributeSchemaPtr virtualAttributeSchema = mSchema->GetVirtualAttributeSchema();
    if (virtualAttributeSchema)
    {
        mVirtualAttributeWriters.reset(new InMemoryAttributeSegmentWriter(true));
        mVirtualAttributeWriters->Init(mSchema, mOptions, mBuildResourceMetrics.get());
    }

    // init summary writer
    if (mSchema->NeedStoreSummary())
    {
        mSummaryWriter.reset(new SummaryWriterImpl());
        IE_LOG(INFO, "Open summary writer.");
        mSummaryWriter->Init(mSchema->GetSummarySchema(), mBuildResourceMetrics.get());
    }

    // init deletionmap writer
    mDeletionMapSegmentWriter.reset(new DeletionMapSegmentWriter);
    mDeletionMapSegmentWriter->Init(
        DeletionMapSegmentWriter::DEFAULT_BITMAP_SIZE, mBuildResourceMetrics.get());
    mModifier = CreateInMemSegmentModifier();
    mDefaultAttrFieldAppender = CreateDefaultAttributeFieldAppender(InMemorySegmentReaderPtr());


    if (!mIsSub)
    {
        const auto& updaterConfigs = mOptions.GetBuildConfig().GetSegmentMetricsUpdaterConfig();
        std::unique_ptr<MultiSegmentMetricsUpdater> updater(new MultiSegmentMetricsUpdater);
        updater->Init(mSchema, mOptions, updaterConfigs, pluginManager);
        mSegAttrUpdater.reset(updater.release());
    }
}

SingleSegmentWriter* SingleSegmentWriter::CloneWithNewSegmentData(
    BuildingSegmentData& segmentData) const
{
    SingleSegmentWriter* newSegmentWriter = new SingleSegmentWriter(*this);
    newSegmentWriter->mSegmentData = segmentData;
    return newSegmentWriter;
}

size_t SingleSegmentWriter::EstimateInitMemUse(
        const index_base::SegmentMetricsPtr& segmentMetrics,
        const util::QuotaControlPtr& buildMemoryQuotaControler,
        const plugin::PluginManagerPtr& pluginManager,
        const index_base::PartitionSegmentIteratorPtr& partSegIter)        
{
    return InMemoryIndexSegmentWriter::EstimateInitMemUse(
            segmentMetrics, mSchema, mOptions, pluginManager, partSegIter);
}

bool SingleSegmentWriter::AddDocument(const DocumentPtr& document)
{
    assert(document);
    assert(mSchema->GetIndexSchema());

    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    if (!IsValidDocument(doc))
    {
        return false;
    }

    docid_t oldDocId = doc->GetDocId();
    doc->SetDocId(mCurrentSegmentInfo->docCount);

    assert(mDefaultAttrFieldAppender);
    mDefaultAttrFieldAppender->AppendDefaultFieldValues(doc);

    if (mSummaryWriter)
    {
        mSummaryWriter->AddDocument(doc->GetSummaryDocument());
    }

    if (mAttributeWriters)
    {
        mAttributeWriters->AddDocument(doc);
    }

    if (mVirtualAttributeWriters)
    {
        mVirtualAttributeWriters->AddDocument(doc);
    }

    mIndexWriters->AddDocument(doc);
    mCurrentSegmentInfo->docCount++;
    doc->SetDocId(oldDocId);
    if (mSegAttrUpdater)
    {
        mSegAttrUpdater->Update(document);
    }

    return true;
}

void SingleSegmentWriter::CollectSegmentMetrics()
{
    mIndexWriters->CollectSegmentMetrics();
    assert(mSegmentMetrics);
    if (mSegAttrUpdater)
    {
        auto jsonMap = mSegAttrUpdater->Dump();
        mSegmentMetrics->SetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP, jsonMap);
    }
}

void SingleSegmentWriter::EndSegment()
{
    if (!IsDirty())
    {
        return;
    }

    mIndexWriters->EndSegment();
    mDeletionMapSegmentWriter->EndSegment(mCurrentSegmentInfo->docCount);
    IE_LOG(INFO, "End segment: doc count [%lu]", (uint64_t)mCurrentSegmentInfo->docCount);
}

void SingleSegmentWriter::CreateDumpItems(
    const file_system::DirectoryPtr& directory, vector<common::DumpItem*>& dumpItems)
{
    if (!IsDirty())
    {
        return;
    }

    assert(!directory->IsExist(SEGMETN_METRICS_FILE_NAME));
    mSegmentMetrics->Store(directory);

    CreateDeletionMapDumpItems(directory, dumpItems);
    CreateSummaryDumpItems(directory, dumpItems);
    CreateAttributeDumpItems(directory, dumpItems);
    CreateIndexDumpItems(directory, dumpItems);
}

void SingleSegmentWriter::CreateIndexDumpItems(
    const file_system::DirectoryPtr& directory, vector<common::DumpItem*>& dumpItems)
{
    file_system::DirectoryPtr indexDirectory = directory->MakeDirectory(INDEX_DIR_NAME);
    mIndexWriters->CreateDumpItems(indexDirectory, dumpItems);
}

void SingleSegmentWriter::CreateAttributeDumpItems(
    const file_system::DirectoryPtr& directory, vector<common::DumpItem*>& dumpItems)
{
    if (!mAttributeWriters && !mVirtualAttributeWriters)
    {
        return;
    }

    file_system::DirectoryPtr attributeDirectory = directory->MakeDirectory(ATTRIBUTE_DIR_NAME);

    if (mAttributeWriters)
    {
        mAttributeWriters->CreateDumpItems(attributeDirectory, dumpItems);
    }

    if (mVirtualAttributeWriters)
    {
        mVirtualAttributeWriters->CreateDumpItems(attributeDirectory, dumpItems);
    }
}

void SingleSegmentWriter::CreateSummaryDumpItems(
    const file_system::DirectoryPtr& directory, vector<common::DumpItem*>& dumpItems)
{
    if (!mSummaryWriter)
    {
        return;
    }
    file_system::DirectoryPtr summaryDirectory = directory->MakeDirectory(SUMMARY_DIR_NAME);
    DumpItem* summaryDumpItem = new SummaryDumpItem(summaryDirectory, mSummaryWriter);
    dumpItems.push_back(summaryDumpItem);
}

void SingleSegmentWriter::CreateDeletionMapDumpItems(
    const file_system::DirectoryPtr& directory, vector<common::DumpItem*>& dumpItems)
{
    assert(mDeletionMapSegmentWriter);
    file_system::DirectoryPtr delMapDirectory = directory->MakeDirectory(DELETION_MAP_DIR_NAME);
    DumpItem* delMapDumpItem = new DeletionMapDumpItem(
        delMapDirectory, mDeletionMapSegmentWriter, mSegmentData.GetSegmentId());
    dumpItems.push_back(delMapDumpItem);
}

InMemorySegmentReaderPtr SingleSegmentWriter::CreateInMemSegmentReader()
{
    assert(mIndexWriters);
    MultiFieldIndexSegmentReaderPtr indexSegmentReader = mIndexWriters->CreateInMemSegmentReader();

    InMemorySegmentReader::String2AttrReaderMap attrReaders;
    if (mAttributeWriters)
    {
        mAttributeWriters->CreateInMemSegmentReaders(attrReaders);
    }
    if (mVirtualAttributeWriters)
    {
        mVirtualAttributeWriters->CreateInMemSegmentReaders(attrReaders);
    }

    SummarySegmentReaderPtr summaryReader;
    if (mSummaryWriter)
    {
        summaryReader = mSummaryWriter->CreateInMemSegmentReader();
    }

    InMemDeletionMapReaderPtr deletionMapReader;
    if (mDeletionMapSegmentWriter)
    {
        deletionMapReader = mDeletionMapSegmentWriter->CreateInMemDeletionMapReader(
            mCurrentSegmentInfo.get(), mSegmentData.GetSegmentId());
    }

    InMemorySegmentReaderPtr reader(new InMemorySegmentReader(mSegmentData.GetSegmentId()));

    reader->Init(
        indexSegmentReader, attrReaders, summaryReader, deletionMapReader, *mCurrentSegmentInfo);

    return reader;
}

bool SingleSegmentWriter::IsValidDocument(const NormalDocumentPtr& doc) const
{
    if (doc->GetIndexDocument() == NULL)
    {
        IE_LOG(WARN, "AddDocument fail: Doc has no index document");
        ERROR_COLLECTOR_LOG(WARN, "AddDocument fail: Doc has no index document");
        return false;
    }
    if (mSchema->GetIndexSchema()->HasPrimaryKeyIndex())
    {
        if (!doc->HasPrimaryKey())
        {
            IE_LOG(WARN, "AddDocument fail: Doc has no primary key");
            ERROR_COLLECTOR_LOG(WARN, "AddDocument fail: Doc has no primary key");
            return false;
        }
        const string& keyStr = doc->GetPrimaryKey();
        if (!mIndexWriters->IsPrimaryKeyStrValid(keyStr))
        {
            IE_LOG(WARN, "AddDocument fail: Doc primary key [%s] is not valid", keyStr.c_str());
            ERROR_COLLECTOR_LOG(
                WARN, "AddDocument fail: Doc primary key [%s] is not valid", keyStr.c_str());
            return false;
        }
    }

    if (mSummaryWriter && doc->GetSummaryDocument() == NULL)
    {
        IE_LOG(WARN, "AddDocument fail: Doc has no summary document");
        ERROR_COLLECTOR_LOG(WARN, "AddDocument fail: Doc has no summary document");
        return false;
    }

    if (!CheckAttributeDocument(doc->GetAttributeDocument()))
    {
        return false;
    }

    return true;
}

bool SingleSegmentWriter::CheckAttributeDocument(const AttributeDocumentPtr& attrDoc) const
{
    if (!mAttributeWriters)
    {
        return true;
    }

    if (attrDoc == NULL)
    {
        IE_LOG(WARN, "AddDocument fail: Doc has no attribute document");
        ERROR_COLLECTOR_LOG(WARN, "AddDocument fail: Doc has no attribute document");
        return false;
    }

    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    size_t schemaPackFieldCount = attrSchema->GetPackAttributeCount();
    if (schemaPackFieldCount == 0)
    {
        return true;
    }

    size_t docPackFieldCount = attrDoc->GetPackFieldCount();
    if (docPackFieldCount != schemaPackFieldCount)
    {
        IE_LOG(ERROR,
            "AddDocument fail: pack attribute fields "
            "do not match schema");
        ERROR_COLLECTOR_LOG(ERROR,
            "AddDocument fail: pack attribute fields "
            "do not match schema");
        return false;
    }
    for (size_t i = 0; i < docPackFieldCount; ++i)
    {
        const ConstString& packField = attrDoc->GetPackField(packattrid_t(i));
        if (packField.empty())
        {
            IE_LOG(ERROR, "AddDocument fail: pack attribute format error");
            ERROR_COLLECTOR_LOG(ERROR, "AddDocument fail: pack attribute format error");
            return false;
        }
    }
    return true;
}

bool SingleSegmentWriter::IsDirty() const
{
    return mCurrentSegmentInfo->docCount > 0 || mDeletionMapSegmentWriter->IsDirty();
}

InMemorySegmentModifierPtr SingleSegmentWriter::CreateInMemSegmentModifier() const
{
    InMemorySegmentModifierPtr segModifier(new InMemorySegmentModifier);
    segModifier->Init(mDeletionMapSegmentWriter, mAttributeWriters);
    return segModifier;
}

DefaultAttributeFieldAppenderPtr SingleSegmentWriter::CreateDefaultAttributeFieldAppender(
    const InMemorySegmentReaderPtr& inMemSegmentReader)
{
    DefaultAttributeFieldAppenderPtr appender(new DefaultAttributeFieldAppender);
    appender->Init(mSchema, inMemSegmentReader);
    return appender;
}

IE_NAMESPACE_END(partition);
