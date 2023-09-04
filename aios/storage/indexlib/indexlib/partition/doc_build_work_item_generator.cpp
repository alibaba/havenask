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
#include "indexlib/partition/doc_build_work_item_generator.h"

#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/source_schema.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/document/document_collector.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_build_work_item.h"
#include "indexlib/index/normal/attribute/accessor/inplace_attribute_modifier.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/in_memory_index_segment_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_build_work_item.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include "indexlib/index/normal/source/source_build_work_item.h"
#include "indexlib/index/normal/summary/summary_build_work_item.h"
#include "indexlib/index/normal/util/build_work_item.h"
#include "indexlib/partition/main_sub_util.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/segment/sub_doc_segment_writer.h"
#include "indexlib/util/GroupedThreadPool.h"

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, DocBuildWorkItemGenerator);

// Needs to be called after segment writer and modifier Init().
void DocBuildWorkItemGenerator::Generate(PartitionWriter::BuildMode buildMode,
                                         const config::IndexPartitionSchemaPtr& schema,
                                         const index_base::SegmentWriterPtr& segmentWriter,
                                         const partition::PartitionModifierPtr& modifier,
                                         const std::shared_ptr<document::DocumentCollector>& docCollector,
                                         util::GroupedThreadPool* threadPool)
{
    assert(buildMode == PartitionWriter::BUILD_MODE_CONSISTENT_BATCH ||
           buildMode == PartitionWriter::BUILD_MODE_INCONSISTENT_BATCH);
    if (!schema->GetSubIndexPartitionSchema()) {
        SingleSegmentWriterPtr singleSegmentWriter = DYNAMIC_POINTER_CAST(SingleSegmentWriter, segmentWriter);
        InplaceModifierPtr inplaceModifier = DYNAMIC_POINTER_CAST(InplaceModifier, modifier);
        assert(singleSegmentWriter && inplaceModifier);

        DoGenerate(buildMode, schema, singleSegmentWriter, inplaceModifier, false, docCollector, threadPool);
    } else {
        SubDocSegmentWriterPtr mainSubSegmentWriter = DYNAMIC_POINTER_CAST(SubDocSegmentWriter, segmentWriter);
        SubDocModifierPtr mainSubModifier = DYNAMIC_POINTER_CAST(SubDocModifier, modifier);
        InplaceModifierPtr mainModifier = DYNAMIC_POINTER_CAST(InplaceModifier, mainSubModifier->GetMainModifier());
        InplaceModifierPtr subModifier = DYNAMIC_POINTER_CAST(InplaceModifier, mainSubModifier->GetSubModifier());
        assert(mainSubSegmentWriter && mainModifier && subModifier);

        DoGenerate(buildMode, schema, mainSubSegmentWriter->GetMainWriter(), mainModifier, false, docCollector,
                   threadPool);
        DoGenerate(buildMode, schema->GetSubIndexPartitionSchema(), mainSubSegmentWriter->GetSubWriter(), subModifier,
                   true, docCollector, threadPool);
    }
}

void DocBuildWorkItemGenerator::DoGenerate(PartitionWriter::BuildMode buildMode,
                                           const config::IndexPartitionSchemaPtr& schema,
                                           const partition::SingleSegmentWriterPtr& segmentWriter,
                                           const partition::InplaceModifierPtr& modifier, bool isSub,
                                           const std::shared_ptr<document::DocumentCollector>& docCollector,
                                           util::GroupedThreadPool* threadPool)
{
    for (const document::DocumentPtr& doc : docCollector->GetDocuments()) {
        if (doc->GetDocOperateType() != ADD_DOC) {
            continue;
        }
        if (!isSub) {
            doc->SetDocId(doc->GetDocId() - modifier->GetBuildingSegmentBaseDocId());
        } else {
            document::NormalDocumentPtr mainDoc = DYNAMIC_POINTER_CAST(document::NormalDocument, doc);
            assert(mainDoc);
            for (const document::NormalDocumentPtr& subDoc : mainDoc->GetSubDocuments()) {
                subDoc->SetDocId(subDoc->GetDocId() - modifier->GetBuildingSegmentBaseDocId());
            }
        }
    }

    CreateIndexWorkItem(buildMode, docCollector, segmentWriter, modifier, schema->GetIndexSchema(), /*isSub=*/isSub,
                        threadPool);
    CreateAttributeWorkItem(buildMode, docCollector, segmentWriter, modifier, schema->GetAttributeSchema(),
                            /*isVirtual=*/false,
                            /*isSub=*/isSub, threadPool);
    CreateAttributeWorkItem(buildMode, docCollector, segmentWriter, modifier, schema->GetVirtualAttributeSchema(),
                            /*isVirtual=*/true,
                            /*isSub=*/isSub, threadPool);
    CreateSummaryWorkItem(docCollector, segmentWriter, schema->GetSummarySchema(), /*isSub=*/isSub, threadPool);
    CreateSourceWorkItem(docCollector, segmentWriter, schema->GetSourceSchema(), /*isSub=*/isSub, threadPool);
}

void DocBuildWorkItemGenerator::CreateAttributeWorkItem(
    PartitionWriter::BuildMode buildMode, const std::shared_ptr<document::DocumentCollector>& docCollector,
    const SingleSegmentWriterPtr& segmentWriter, const InplaceModifierPtr& modifier,
    const config::AttributeSchemaPtr& attrSchema, bool isVirtual, bool isSub, util::GroupedThreadPool* threadPool)
{
    if (attrSchema == nullptr) {
        return;
    }

    docid_t buildingSegmentBaseDocId = modifier->GetBuildingSegmentBaseDocId();
    index::InMemoryAttributeSegmentWriterPtr attrSegmentWriter =
        segmentWriter->GetInMemoryAttributeSegmentWriter(isVirtual);
    index::AttributeModifierPtr attrModifier = modifier->GetAttributeModifier();

    config::AttributeConfigIteratorPtr attrConfIter = attrSchema->CreateIterator();
    for (config::AttributeSchema::Iterator iter = attrConfIter->Begin(); iter != attrConfIter->End(); iter++) {
        config::AttributeConfigPtr attrConfig = *iter;
        if (attrConfig->GetPackAttributeConfig() != NULL) {
            // skip attributes defined in pack attribute
            continue;
        }
        auto item = std::make_unique<index::legacy::AttributeBuildWorkItem>(attrConfig.get(), attrModifier.get(),
                                                                            attrSegmentWriter.get(), isSub,
                                                                            buildingSegmentBaseDocId, docCollector);
        const std::string& itemName = item->Name();
        if (buildMode == PartitionWriter::BUILD_MODE_INCONSISTENT_BATCH) {
            // JOIN 字段在主线程中直接执行，来保证下一轮正确分配DocId，其它索引进线程池。
            // 这样一来，外部无需 threadPool.WaitFinish()，可以避免 batch 内有性能长尾，利于形成流水
            if (attrConfig->GetAttrName() == MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME ||
                attrConfig->GetAttrName() == SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME) {
                item->process();
                continue;
            }
        }
        threadPool->PushWorkItem(itemName, std::move(item));
    }

    size_t packAttrCount = attrSchema->GetPackAttributeCount();
    for (size_t i = 0; i < packAttrCount; ++i) {
        const config::PackAttributeConfigPtr& packAttrConfig = attrSchema->GetPackAttributeConfig(packattrid_t(i));
        assert(packAttrConfig);
        if (packAttrConfig->IsDisabled()) {
            IE_LOG(DEBUG, "attribute [%s] is disable", packAttrConfig->GetPackName().c_str());
            continue;
        }

        auto item = std::make_unique<index::legacy::AttributeBuildWorkItem>(packAttrConfig.get(), attrModifier.get(),
                                                                            attrSegmentWriter.get(), isSub,
                                                                            buildingSegmentBaseDocId, docCollector);
        const std::string& itemName = item->Name();
        threadPool->PushWorkItem(itemName, std::move(item));
    }
}

void DocBuildWorkItemGenerator::CreateIndexWorkItem(PartitionWriter::BuildMode buildMode,
                                                    const std::shared_ptr<document::DocumentCollector>& docCollector,
                                                    const SingleSegmentWriterPtr& segmentWriter,
                                                    const InplaceModifierPtr& modifier,
                                                    const config::IndexSchemaPtr& indexSchema, bool isSub,
                                                    util::GroupedThreadPool* threadPool)
{
    if (indexSchema == nullptr) {
        return;
    }

    docid_t buildingSegmentBaseDocId = modifier->GetBuildingSegmentBaseDocId();

    config::IndexConfigIteratorPtr indexConfigIter = indexSchema->CreateIterator(/*needVirtual=*/true);
    for (config::IndexSchema::Iterator iter = indexConfigIter->Begin(); iter != indexConfigIter->End(); iter++) {
        config::IndexConfigPtr indexConfig = *iter;
        if (index::InMemoryIndexSegmentWriter::IsTruncateIndex(indexConfig)) {
            continue;
        }
        config::IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
        if (shardingType == config::IndexConfig::IST_IS_SHARDING) {
            continue;
        }

        if (buildMode == PartitionWriter::BUILD_MODE_INCONSISTENT_BATCH) {
            // PK 字段在主线程中直接执行，来保证下一轮正确分配DocId，其它索引进线程池。
            // 这样一来，外部无需 threadPool.WaitFinish()，可以避免 batch 内有性能长尾，利于形成流水
            if (indexConfig->GetInvertedIndexType() == InvertedIndexType::it_primarykey64 ||
                indexConfig->GetInvertedIndexType() == InvertedIndexType::it_primarykey128) {
                assert(shardingType == config::IndexConfig::IST_NO_SHARDING);
                auto item = std::make_unique<index::legacy::IndexBuildWorkItem>(
                    indexConfig.get(), /*shardId=*/0, modifier->GetIndexModifier(),
                    segmentWriter->GetInMemoryIndexSegmentWriter().get(), isSub, buildingSegmentBaseDocId,
                    docCollector);
                item->process();
                continue;
            }
        }
        if (shardingType == config::IndexConfig::IST_NEED_SHARDING) {
            const std::vector<config::IndexConfigPtr>& shardingIndexConfigs = indexConfig->GetShardingIndexConfigs();
            for (int i = 0; i < shardingIndexConfigs.size(); i++) {
                auto item = std::make_unique<index::legacy::IndexBuildWorkItem>(
                    indexConfig.get(), /*shardId=*/i, modifier->GetIndexModifier(),
                    segmentWriter->GetInMemoryIndexSegmentWriter().get(), isSub, buildingSegmentBaseDocId,
                    docCollector);
                const std::string& itemName = item->Name();
                threadPool->PushWorkItem(itemName, std::move(item));
            }
            if (index::IndexWriterFactory::NeedSectionAttributeWriter(indexConfig)) {
                auto item = std::make_unique<index::legacy::IndexBuildWorkItem>(
                    indexConfig.get(), /*shardId=*/shardingIndexConfigs.size(), modifier->GetIndexModifier(),
                    segmentWriter->GetInMemoryIndexSegmentWriter().get(), isSub, buildingSegmentBaseDocId,
                    docCollector);
                const std::string& itemName = item->Name();
                threadPool->PushWorkItem(itemName, std::move(item));
            }
        } else {
            auto item = std::make_unique<index::legacy::IndexBuildWorkItem>(
                indexConfig.get(), /*shardId=*/0, modifier->GetIndexModifier(),
                segmentWriter->GetInMemoryIndexSegmentWriter().get(), isSub, buildingSegmentBaseDocId, docCollector);
            const std::string& itemName = item->Name();
            threadPool->PushWorkItem(itemName, std::move(item));
        }
    }
}

void DocBuildWorkItemGenerator::CreateSummaryWorkItem(const std::shared_ptr<document::DocumentCollector>& docCollector,
                                                      const SingleSegmentWriterPtr& segmentWriter,
                                                      const config::SummarySchemaPtr& summarySchema, bool isSub,
                                                      util::GroupedThreadPool* threadPool)
{
    if (summarySchema == nullptr or !summarySchema->NeedStoreSummary()) {
        return;
    }
    auto item = std::make_unique<index::legacy::SummaryBuildWorkItem>(segmentWriter->GetSummaryWriter().get(),
                                                                      docCollector, isSub);
    const std::string& itemName = item->Name();
    threadPool->PushWorkItem(itemName, std::move(item));
}

void DocBuildWorkItemGenerator::CreateSourceWorkItem(const std::shared_ptr<document::DocumentCollector>& docCollector,
                                                     const SingleSegmentWriterPtr& segmentWriter,
                                                     const config::SourceSchemaPtr& sourceSchema, bool isSub,
                                                     util::GroupedThreadPool* threadPool)
{
    if (!sourceSchema) {
        return;
    }
    auto item = std::make_unique<index::legacy::SourceBuildWorkItem>(segmentWriter->GetSourceWriter().get(),
                                                                     docCollector, isSub);
    const std::string& itemName = item->Name();
    threadPool->PushWorkItem(itemName, std::move(item));
}
}} // namespace indexlib::partition
