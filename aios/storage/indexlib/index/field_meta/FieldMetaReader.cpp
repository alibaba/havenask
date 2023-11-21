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
#include "indexlib/index/field_meta/FieldMetaReader.h"

#include "indexlib/index/field_meta/meta/MetaFactory.h"
#include "indexlib/util/MemBuffer.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, FieldMetaReader);

FieldMetaReader::FieldMetaReader() {}

FieldMetaReader::~FieldMetaReader() {}

Status FieldMetaReader::Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                             const indexlibv2::framework::TabletData* tabletData)
{
    assert(indexConfig != nullptr);
    std::string indexName = indexConfig->GetIndexName();
    AUTIL_LOG(INFO, "Start opening field meta(%s).", indexConfig->GetIndexName().c_str());
    auto segments = tabletData->CreateSlice();
    docid_t baseDocId = 0;
    for (const auto& segment : segments) {
        auto docCount = segment->GetSegmentInfo()->GetDocCount();
        auto [status, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        RETURN_IF_STATUS_ERROR(status, "no indexer for [%s] in segment [%d]", indexConfig->GetIndexName().c_str(),
                               segment->GetSegmentId());

        if (segment->GetSegmentStatus() == indexlibv2::framework::Segment::SegmentStatus::ST_BUILT) {
            auto fieldMetaDiskIndexer = std::dynamic_pointer_cast<FieldMetaDiskIndexer>(indexer);
            if (!fieldMetaDiskIndexer) {
                AUTIL_LOG(ERROR, "no field meta indexer for index [%s] segment [%d]", indexName.c_str(),
                          segment->GetSegmentId());
                return Status::InternalError("no field meta indexer for index [%s] segment [%d]", indexName.c_str(),
                                             segment->GetSegmentId());
            }
            _diskIndexers.emplace_back(std::make_pair(baseDocId, baseDocId + docCount), fieldMetaDiskIndexer);
        } else {
            assert(segment->GetSegmentStatus() == indexlibv2::framework::Segment::SegmentStatus::ST_DUMPING ||
                   segment->GetSegmentStatus() == indexlibv2::framework::Segment::SegmentStatus::ST_BUILDING);
            auto fieldMetaMemIndexer = std::dynamic_pointer_cast<FieldMetaMemIndexer>(indexer);
            if (!fieldMetaMemIndexer) {
                AUTIL_LOG(ERROR, "no field meta indexer for index [%s] segment [%d]", indexName.c_str(),
                          segment->GetSegmentId());
                return Status::InternalError("no field meta indexer for index [%s] segment [%d]", indexName.c_str(),
                                             segment->GetSegmentId());
            }
            _memIndexers.emplace_back(baseDocId, fieldMetaMemIndexer);
        }
        baseDocId += docCount;
    }
    auto fieldMetaConfig = std::dynamic_pointer_cast<FieldMetaConfig>(indexConfig);
    if (!fieldMetaConfig) {
        return Status::Corruption("cast index config [%s] to field meta config failed",
                                  indexConfig->GetIndexName().c_str());
    }
    RETURN_IF_STATUS_ERROR(InitTableFieldMetas(fieldMetaConfig), "init table field meta failed");
    AUTIL_LOG(INFO, "End opening field meta(%s).", indexConfig->GetIndexName().c_str());
    return Status::OK();
}

std::shared_ptr<IFieldMeta> FieldMetaReader::GetFieldMeta(const std::string& metaName) const
{
    auto iter = _tableFieldMetas.find(metaName);
    if (iter == _tableFieldMetas.end()) {
        return nullptr;
    }
    return iter->second;
}

Status FieldMetaReader::InitTableFieldMetas(const std::shared_ptr<FieldMetaConfig>& fieldMetaConfig)
{
    auto [status, fieldMetas] = MetaFactory::CreateFieldMetas(fieldMetaConfig);
    RETURN_IF_STATUS_ERROR(status, "create field metas failed for index [%s]", fieldMetaConfig->GetIndexName().c_str());
    for (auto& fieldMeta : fieldMetas) {
        auto metaName = fieldMeta->GetFieldMetaName();
        AUTIL_LOG(INFO, "init field meta: %s", metaName.c_str());
        for (const auto& [_, fieldMetaDiskIndexer] : _diskIndexers) {
            auto diskFieldMeta = fieldMetaDiskIndexer->GetSegmentFieldMeta(metaName);
            if (!diskFieldMeta) {
                assert(false);
                return Status::Corruption("get field meta [%s] failed", metaName.c_str());
            }
            RETURN_IF_STATUS_ERROR(fieldMeta->Merge(diskFieldMeta), "field meta merge failed");
        }
        _tableFieldMetas[metaName] = fieldMeta;
    }
    return Status::OK();
}

std::pair<Status, std::shared_ptr<IFieldMeta>>
FieldMetaReader::UpdateTableFieldMetas(const std::string& fieldMetaType,
                                       const std::shared_ptr<IFieldMeta>& fieldMeta) const
{
    std::shared_ptr<IFieldMeta> newFieldMeta;
    newFieldMeta.reset(fieldMeta->Clone());

    for (const auto& [_, fieldMetaMemIndexer] : _memIndexers) {
        auto memFieldMeta = fieldMetaMemIndexer->GetSegmentFieldMeta(fieldMetaType);
        if (!memFieldMeta) {
            assert(false);
            return {Status::Corruption("get field meta [%s] failed", fieldMetaType.c_str()), nullptr};
        }
        RETURN2_IF_STATUS_ERROR(newFieldMeta->Merge(memFieldMeta), nullptr, "field meta merge failed");
    }
    return {Status::OK(), newFieldMeta};
}

std::shared_ptr<IFieldMeta> FieldMetaReader::GetEstimateTableFieldMeta(const std::string& fieldMetaType) const
{
    auto fieldMeta = GetFieldMeta(fieldMetaType);
    if (!fieldMeta) {
        AUTIL_LOG(ERROR, "get table field meta [%s] failed", fieldMetaType.c_str());
        return nullptr;
    }
    if (fieldMetaType == DATA_STATISTICS_META_STR || fieldMetaType == HISTOGRAM_META_STR) {
        return fieldMeta;
    }
    AUTIL_LOG(ERROR, "not support table field meta [%s], use GetTableFieldMeta instead", fieldMetaType.c_str());
    return nullptr;
}

std::shared_ptr<IFieldMeta> FieldMetaReader::GetTableFieldMeta(const std::string& fieldMetaType) const
{
    if (fieldMetaType == DATA_STATISTICS_META_STR || fieldMetaType == HISTOGRAM_META_STR) {
        AUTIL_LOG(ERROR, "not support table field meta [%s], use GetEstimateTableFieldMeta instead",
                  fieldMetaType.c_str());
        return nullptr;
    }
    auto fieldMeta = GetFieldMeta(fieldMetaType);
    if (!fieldMeta) {
        AUTIL_LOG(ERROR, "get table field meta [%s] failed", fieldMetaType.c_str());
        return nullptr;
    }

    auto [status, newFieldMeta] = UpdateTableFieldMetas(fieldMetaType, fieldMeta);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get table field meta [%s] failed", fieldMetaType.c_str());
        return nullptr;
    }
    return newFieldMeta;
}

std::shared_ptr<IFieldMeta> FieldMetaReader::GetSegmentFieldMeta(const std::string& fieldMetaType,
                                                                 const DocIdRange& docIdRange) const
{
    for (const auto& [range, fieldMetaDiskIndexer] : _diskIndexers) {
        if (docIdRange == range) {
            return fieldMetaDiskIndexer->GetSegmentFieldMeta(fieldMetaType);
        }
    }
    for (const auto& [startDocid, fieldMetaMemIndexer] : _memIndexers) {
        if (docIdRange.first == startDocid) {
            return fieldMetaMemIndexer->GetSegmentFieldMeta(fieldMetaType);
        }
    }
    AUTIL_LOG(ERROR, "get segment field meta for meta type [%s] with doc range[%d,%d] failed", fieldMetaType.c_str(),
              docIdRange.first, docIdRange.second);
    return nullptr;
}

bool FieldMetaReader::GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool, uint64_t& fieldTokenCount)
{
    docid_t docId = key;
    for (const auto& [docIdRange, diskIndexer] : _diskIndexers) {
        if (docId >= docIdRange.first && docId < docIdRange.second) {
            bool ret = diskIndexer->GetFieldTokenCount(docId - docIdRange.first, pool, fieldTokenCount);
            if (!ret) {
                AUTIL_LOG(ERROR, "read field token count faild for docid [%d] in disk indexer", docId);
            }
            return ret;
        }
    }
    std::shared_ptr<FieldMetaMemIndexer> matchMemIndexer = nullptr;
    docid_t beginDocId = INVALID_DOCID;
    for (const auto& [startDocId, memIndexer] : _memIndexers) {
        if (docId >= startDocId) {
            matchMemIndexer = memIndexer;
            beginDocId = startDocId;
        }
    }
    if (matchMemIndexer) {
        bool ret = matchMemIndexer->GetFieldTokenCount(docId - beginDocId, pool, fieldTokenCount);
        if (!ret) {
            AUTIL_LOG(ERROR, "read field token count faild for docid [%d] in mem indexer", docId);
        }
        return ret;
    }
    AUTIL_LOG(ERROR, "query invalid docid [%d]", docId);
    return false;
}

std::string FieldMetaReader::TEST_GetSourceField(int64_t key)
{
    docid_t docId = key;
    for (const auto& [docIdRange, diskIndexer] : _diskIndexers) {
        if (docId >= docIdRange.first && docId < docIdRange.second) {
            auto [status, sourceFieldReader] = diskIndexer->GetSourceFieldReader();
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "disk indexer get source field reader for docid [%d] failed", docId);
                return "";
            }
            autil::mem_pool::Pool pool;
            std::string fieldValue;
            bool isNull;
            sourceFieldReader->GetFieldValue(docId - docIdRange.first, &pool, fieldValue, isNull);
            return fieldValue;
        }
    }
    AUTIL_LOG(ERROR, "not support query mem indexer source field yet");
    return "";
}

} // namespace indexlib::index
