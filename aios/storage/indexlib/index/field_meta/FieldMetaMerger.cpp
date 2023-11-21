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
#include "indexlib/index/field_meta/FieldMetaMerger.h"

#include "autil/EnvUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/common/field_format/field_meta/FieldMetaConvertor.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/FieldMetaDiskIndexer.h"
#include "indexlib/index/field_meta/SourceFieldConfigGenerator.h"
#include "indexlib/index/field_meta/SourceFieldIndexFactory.h"
#include "indexlib/index/field_meta/meta/MetaFactory.h"
#include "indexlib/util/MemBuffer.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, FieldMetaMerger);

Status FieldMetaMerger::Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                             const std::map<std::string, std::any>& params)
{
    _fieldMetaConfig = std::dynamic_pointer_cast<FieldMetaConfig>(indexConfig);
    assert(_fieldMetaConfig != nullptr);
    _params = params;
    return Status::OK();
}

Status FieldMetaMerger::MergeWithSource(
    const SegmentMergeInfos& segMergeInfos,
    const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager)
{
    auto iter = _params.find(indexlibv2::index::DocMapper::GetDocMapperType());
    if (iter == _params.end()) {
        AUTIL_LOG(ERROR, "not find doc mapper name by type [%s] for index [%s]",
                  indexlibv2::index::DocMapper::GetDocMapperType().c_str(), _fieldMetaConfig->GetIndexName().c_str());
        return Status::Corruption("not find doc mapper name by type");
    }
    const std::string& docMapperName = std::any_cast<std::string>(iter->second);
    std::shared_ptr<indexlibv2::index::DocMapper> docMapper;
    auto status =
        taskResourceManager->LoadResource(docMapperName, indexlibv2::index::DocMapper::GetDocMapperType(), docMapper);
    RETURN_IF_STATUS_ERROR(status, "load doc mapper failed");

    std::map<segmentid_t, std::shared_ptr<ISourceFieldReader>> sourceFieldReaders;
    for (const auto& sourceSegment : segMergeInfos.srcSegments) {
        auto segmentId = sourceSegment.segment->GetSegmentId();
        auto [status, indexer] =
            sourceSegment.segment->GetIndexer(FIELD_META_INDEX_TYPE_STR, _fieldMetaConfig->GetIndexName());
        RETURN_IF_STATUS_ERROR(status, "get field meta indexer [%s] from segment [%d] failed",
                               _fieldMetaConfig->GetIndexName().c_str(), segmentId);
        auto fieldMetaDiskIndexer = std::dynamic_pointer_cast<FieldMetaDiskIndexer>(indexer);
        if (!fieldMetaDiskIndexer) {
            return Status::Corruption("cast field meta disk indexer failed");
        }
        auto [status1, reader] = fieldMetaDiskIndexer->GetSourceFieldReader();
        RETURN_IF_STATUS_ERROR(status1, "get source reader failed for index [%s], segId [%d]",
                               _fieldMetaConfig->GetIndexName().c_str(), segmentId);
        reader->PrepareReadContext();
        sourceFieldReaders[segmentId] = reader;
    }

    auto targetSegments = segMergeInfos.targetSegments;
    uint64_t baseDocId = 0;
    bool isNull = false;

    for (const auto& targetSegment : targetSegments) {
        auto docCount = docMapper->GetTargetSegmentDocCount(targetSegment->segmentId);
        auto [status, fieldMetas] = MetaFactory::CreateFieldMetas(_fieldMetaConfig);
        RETURN_IF_STATUS_ERROR(status, "create field meta failed");

        const std::string& indexName = _fieldMetaConfig->GetIndexName();
        auto segDir = targetSegment->segmentDir->GetIDirectory();

        auto [st, indexDir] =
            segDir->MakeDirectory(FIELD_META_INDEX_PATH, indexlib::file_system::DirectoryOption()).StatusWith();

        RETURN_IF_STATUS_ERROR(st, "create field meta dir failed [%s]", segDir->DebugString().c_str());
        auto [st2, fieldMetaDir] =
            indexDir->MakeDirectory(indexName, indexlib::file_system::DirectoryOption()).StatusWith();
        RETURN_IF_STATUS_ERROR(st2, "create field meta dir failed [%s]", indexDir->DebugString().c_str());

        for (docid_t i = 0; i < docCount; i++) {
            auto [srcSegmentId, srcLocalDocId] = docMapper->ReverseMap(baseDocId + i);
            auto iter = sourceFieldReaders.find(srcSegmentId);
            if (iter == sourceFieldReaders.end()) {
                return Status::Corruption("get reader failed for index [%s], srcSegId is [%d]",
                                          _fieldMetaConfig->GetIndexName().c_str(), srcSegmentId);
            }
            auto reader = iter->second;
            uint64_t tokenCount = 0;
            std::string fieldValue;
            if (!reader->GetFieldValue(srcLocalDocId, nullptr /*pool*/, fieldValue, isNull)) {
                return Status::Corruption("read source field failed for index [%s]",
                                          _fieldMetaConfig->GetIndexName().c_str());
            }

            if (reader->GetMetaStoreType() == FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT) {
                // not field
                bool ret = autil::StringUtil::fromString(fieldValue, tokenCount);
                if (!ret) {
                    assert(false);
                    return Status::Corruption("get field token count failed for index [%s]",
                                              _fieldMetaConfig->GetIndexName().c_str());
                }
                fieldValue = "";
            }
            FieldValueMeta fieldValueMeta {fieldValue, tokenCount};
            for (const auto& fieldMeta : fieldMetas) {
                RETURN_IF_STATUS_ERROR(fieldMeta->Build({{fieldValueMeta, isNull, i}}),
                                       "build for field meta [%s] failed", fieldMeta->GetFieldMetaName().c_str());
            }
        }
        autil::mem_pool::Pool dumpPool;
        for (const auto& fieldMeta : fieldMetas) {
            auto status = fieldMeta->Store(&dumpPool, fieldMetaDir);
            RETURN_IF_STATUS_ERROR(status, "dump field meta [%s] faild for path [%s]", indexName.c_str(),
                                   fieldMetaDir->DebugString().c_str());
        }
        baseDocId += docCount;
    }

    auto sourceFieldMerger = SourceFieldIndexFactory::CreateSourceFieldMerger(_fieldMetaConfig);
    if (!sourceFieldMerger) {
        return Status::Corruption("source field merger is null");
    }
    auto [status2, attributeConfig] = SourceFieldConfigGenerator::CreateSourceFieldConfig(_fieldMetaConfig);
    RETURN_IF_STATUS_ERROR(status2, "convert field meta [%s] config to attribute failed",
                           _fieldMetaConfig->GetIndexName().c_str());

    _params[SOURCE_FIELD_READER_PARAM] = sourceFieldReaders;

    RETURN_IF_STATUS_ERROR(sourceFieldMerger->Init(attributeConfig, _params), "source field init failed");
    RETURN_IF_STATUS_ERROR(sourceFieldMerger->Merge(segMergeInfos, taskResourceManager), "source field merge failed");
    return Status::OK();
}

Status FieldMetaMerger::MergeWithoutSource(
    const SegmentMergeInfos& segMergeInfos,
    const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager)
{
    auto srcSegments = segMergeInfos.srcSegments;
    auto [status, fieldMetas] = MetaFactory::CreateFieldMetas(_fieldMetaConfig);

    for (const auto& srcSegment : srcSegments) {
        auto [status, indexer] =
            srcSegment.segment->GetIndexer(FIELD_META_INDEX_TYPE_STR, _fieldMetaConfig->GetIndexName());
        RETURN_IF_STATUS_ERROR(status, "get field meta indexer [%s] from segment [%d] failed",
                               _fieldMetaConfig->GetIndexName().c_str(), srcSegment.segment->GetSegmentId());
        auto fieldMetaDiskIndexer = std::dynamic_pointer_cast<FieldMetaDiskIndexer>(indexer);
        if (!fieldMetaDiskIndexer) {
            return Status::Corruption("cast field meta disk indexer failed");
        }
        for (auto& fieldMeta : fieldMetas) {
            auto metaName = fieldMeta->GetFieldMetaName();
            auto srcFieldMeta = fieldMetaDiskIndexer->GetSegmentFieldMeta(metaName);
            if (!srcFieldMeta) {
                return Status::Corruption("get field meta [%s] from segment [%d] failed", metaName.c_str(),
                                          srcSegment.segment->GetSegmentId());
            }
            RETURN_IF_STATUS_ERROR(fieldMeta->Merge(srcFieldMeta), "field meta merge failed");
        }
    }

    // 没有 source 数据时， 无法保证获得准确信息，在多 target segment 场景， 以 srcSegment 合并做粗略估计
    auto targetSegments = segMergeInfos.targetSegments;
    for (const auto& targetSegment : targetSegments) {
        const std::string& indexName = _fieldMetaConfig->GetIndexName();
        auto segDir = targetSegment->segmentDir->GetIDirectory();

        auto [st, indexDir] =
            segDir->MakeDirectory(FIELD_META_INDEX_PATH, indexlib::file_system::DirectoryOption()).StatusWith();

        RETURN_IF_STATUS_ERROR(st, "create field meta dir failed [%s]", segDir->DebugString().c_str());
        auto [st2, fieldMetaDir] =
            indexDir->MakeDirectory(indexName, indexlib::file_system::DirectoryOption()).StatusWith();
        RETURN_IF_STATUS_ERROR(st2, "create field meta dir failed [%s]", indexDir->DebugString().c_str());
        autil::mem_pool::Pool dumpPool;
        for (const auto& fieldMeta : fieldMetas) {
            auto status = fieldMeta->Store(&dumpPool, fieldMetaDir);
            RETURN_IF_STATUS_ERROR(status, "dump field meta [%s] faild for path [%s]", indexName.c_str(),
                                   fieldMetaDir->DebugString().c_str());
        }
    }
    return Status::OK();
}
Status
FieldMetaMerger::Merge(const SegmentMergeInfos& segMergeInfos,
                       const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager)
{
    if (_fieldMetaConfig->GetStoreMetaSourceType() != FieldMetaConfig::MetaSourceType::MST_NONE) {
        return MergeWithSource(segMergeInfos, taskResourceManager);
    } else {
        return MergeWithoutSource(segMergeInfos, taskResourceManager);
    }
    return Status::OK();
}

} // namespace indexlib::index
