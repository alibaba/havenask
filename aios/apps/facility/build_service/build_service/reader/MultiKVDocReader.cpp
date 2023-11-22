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
#include "build_service/reader/MultiKVDocReader.h"

#include <memory.h>
#include <sstream>

#include "autil/StringUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/reader/IdleDocumentParser.h"
#include "build_service/util/Monitor.h"
#include "build_service/util/RangeUtil.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/kkv/kkv_doc_reader.h"
#include "indexlib/index/kv/kv_doc_reader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/partition/offline_partition.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::partition;
using namespace build_service::util;
using namespace build_service::proto;
using namespace build_service::config;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, MultiKVDocReader);

MultiKVDocReader::MultiKVDocReader()
{
    memset(&_currentProgress, 0, sizeof(_currentProgress));
    memset(&_committedCheckPoint, 0, sizeof(_committedCheckPoint));
}

MultiKVDocReader::~MultiKVDocReader() {}

bool MultiKVDocReader::init(const ReaderInitParam& params)
{
    string indexRoot = getValueFromKeyValueMap(params.kvMap, READ_INDEX_ROOT);
    if (indexRoot.empty()) {
        BS_LOG(ERROR, "kv reader's param [%s] cannot be empty", READ_INDEX_ROOT.data());
        return false;
    }

    vector<pair<pair<uint32_t, uint32_t>, string>> rangePath;
    if (!RangeUtil::getPartitionRange(indexRoot, rangePath)) {
        BS_LOG(ERROR, "failed to get partition info from [%s]", indexRoot.c_str());
        return false;
    }

    string versionStr = getValueFromKeyValueMap(params.kvMap, READ_INDEX_VERSION);
    versionid_t versionId = indexlib::INVALID_VERSIONID;
    if (versionStr.empty() || !StringUtil::fromString(versionStr, versionId) || versionId < 0) {
        BS_LOG(WARN, "kv reader's version [%s] is invalid", versionStr.c_str());
        return false;
    }

    uint64_t blockCacheSizeInMB = 10;
    string blockCacheSizeInMBStr = getValueFromKeyValueMap(params.kvMap, READ_INDEX_CACHE_SIZE);
    if (!blockCacheSizeInMBStr.empty() && !StringUtil::fromString(blockCacheSizeInMBStr, blockCacheSizeInMB)) {
        BS_LOG(WARN, "kv reader's block cache size [%s]MB is invalid, use default 10 MB",
               blockCacheSizeInMBStr.c_str());
    }

    float cacheBlockSizeInMB = 0.5;
    string cacheBlockSizeInMBStr = getValueFromKeyValueMap(params.kvMap, READ_CACHE_BLOCK_SIZE);
    if (!cacheBlockSizeInMBStr.empty() && !StringUtil::fromString(cacheBlockSizeInMBStr, cacheBlockSizeInMB)) {
        BS_LOG(WARN, "kv reader's block cache block size [%s]MB is invalid, use default 0.5 MB",
               cacheBlockSizeInMBStr.c_str());
    }

    _options.SetIsOnline(false);
    OfflineConfig& offlineConfig = _options.GetOfflineConfig();
    offlineConfig.readerConfig.loadIndex = false;
    offlineConfig.readerConfig.indexCacheSize = blockCacheSizeInMB * 1024 * 1024;
    offlineConfig.readerConfig.cacheBlockSize = cacheBlockSizeInMB * 1024 * 1024;

    _timestamp = TimeUtility::currentTime();
    string timeStampStr = getValueFromKeyValueMap(params.kvMap, READER_TIMESTAMP);
    if (!timeStampStr.empty() && !StringUtil::fromString(timeStampStr, _timestamp)) {
        BS_LOG(WARN, "kv reader's timestamp [%s] is invalid, use current timestamp", timeStampStr.c_str());
    }

    string tableName;
    std::shared_ptr<indexlibv2::config::ITabletSchema> targetSchema;
    auto it = params.kvMap.find(RAW_DOCUMENT_SCHEMA_NAME);
    if (it != params.kvMap.end()) {
        tableName = it->second;
    }
    if (!tableName.empty() && params.resourceReader) {
        targetSchema = params.resourceReader->getTabletSchemaBySchemaTableName(tableName);
    }
    if (!targetSchema) {
        targetSchema = params.schemaV2;
    }
    if (targetSchema) {
        BS_LOG(INFO, "init using schema name [%s]", tableName.c_str());
        auto [status, ttlFromDoc] = targetSchema->GetRuntimeSettings().GetValue<bool>("ttl_from_doc");
        if (!status.IsOK() && !status.IsNotFound()) {
            BS_LOG(ERROR, "get ttl_from_doc failed, [%s]", status.ToString().c_str());
            return false;
        }
        if (ttlFromDoc) {
            auto [status, ttlFieldName] = targetSchema->GetRuntimeSettings().GetValue<std::string>("ttl_field_name");
            if (status.IsNotFound()) {
                ttlFieldName = DOC_TIME_TO_LIVE_IN_SECONDS;
            } else if (!status.IsOK()) {
                BS_LOG(ERROR, "get ttl_field_name failed, [%s]", status.ToString().c_str());
                return false;
            }
            _ttlFieldName = ttlFieldName;
            BS_LOG(INFO, "ttl from doc use field name [%s]", _ttlFieldName.c_str());
        }
    } else {
        BS_LOG(INFO, "initialize with empty target schema! ");
    }

    selectShardItems(params.range.from(), params.range.to(), versionId, rangePath);
    if (_totalItems.empty()) {
        BS_LOG(INFO, "read with range [%u %u] includes empty partitions!", params.range.from(), params.range.to());
        return true;
    }
    if (!switchPartitionShard(0, 0, 0)) {
        BS_LOG(ERROR, "switch reader to itemIdx[0], segmentIdx[0], offsetInSegment[0] failed");
        return false;
    }

    return true;
}

bool MultiKVDocReader::switchPartitionShard(int64_t itemIdx, int64_t segmentIdx, int64_t offsetInSegment)
{
    if (itemIdx == _currentProgress.itemIdx && _docReader) {
        if (segmentIdx == _currentProgress.segmentIdx && offsetInSegment == _currentProgress.offsetInSegment) {
            return true;
        } else {
            bool ret = _docReader->Seek(segmentIdx, offsetInSegment);
            auto idxPair = _docReader->GetCurrentOffset();
            _currentProgress.segmentIdx = idxPair.first;
            _currentProgress.offsetInSegment = idxPair.second;
            if (!ret) {
                BS_LOG(ERROR,
                       "can not switch to item idx [%ld], segment idx [%ld], offsetInSegment [%ld], "
                       "current progress [%ld %ld %ld]",
                       itemIdx, segmentIdx, offsetInSegment, _currentProgress.itemIdx, _currentProgress.segmentIdx,
                       _currentProgress.offsetInSegment);
            }
            return ret;
        }
    }

    if (itemIdx < 0 || itemIdx >= (int64_t)_totalItems.size()) {
        BS_LOG(ERROR, "can not switch to target idx [%ld] with vector size [%ld]", itemIdx, _totalItems.size());
        return false;
    }
    if (!loadSrcSchema(_totalItems[itemIdx].indexRootPath)) {
        return false;
    }

    OfflinePartitionPtr partition(new OfflinePartition(/*branch name legacy*/ true));
    if (IndexPartition::OpenStatus::OS_OK != partition->Open(_totalItems[itemIdx].indexRootPath, "", _srcSchema,
                                                             _options, _totalItems[itemIdx].indexVersion)) {
        IE_LOG(WARN, "open partition [%s] failed", _totalItems[itemIdx].indexRootPath.c_str());
        return false;
    }

    string schemaName = _srcSchema->GetSchemaName();
    BS_LOG(INFO, "get schema name [%s]", schemaName.c_str());

    if (indexlib::TableType::tt_kv == _srcSchema->GetTableType()) {
        _docReader.reset(new KVDocReader);
    } else if (indexlib::TableType::tt_kkv == _srcSchema->GetTableType()) {
        _docReader.reset(new KKVDocReader);
    } else {
        BS_LOG(ERROR, "unsupport table type [%u]", _srcSchema->GetTableType());
        return false;
    }
    if (!_docReader->Init(_srcSchema, partition->GetPartitionData(), _totalItems[itemIdx].shardIdx, _timestamp,
                          _ttlFieldName)) {
        BS_LOG(ERROR, "switch to item [%ld] failed due to reader init failed", itemIdx);
        return false;
    }
    if (!_docReader->Seek(segmentIdx, offsetInSegment)) {
        BS_LOG(ERROR, "seek to itemIdx[%ld], segmentIdx[%ld], offsetInSegment[%ld] failed", itemIdx, segmentIdx,
               offsetInSegment);
        return false;
    }
    _currentProgress.itemIdx = itemIdx;
    _currentProgress.segmentIdx = segmentIdx;
    _currentProgress.offsetInSegment = offsetInSegment;
    BS_LOG(INFO, "switch to itemIdx[%ld], segmentIdx[%ld], offsetInSegment[%ld] success", _currentProgress.itemIdx,
           _currentProgress.segmentIdx, _currentProgress.offsetInSegment);
    return true;
}

indexlib::document::RawDocumentParser* MultiKVDocReader::createRawDocumentParser(const ReaderInitParam& params)
{
    return new IdleDocumentParser("idle");
}

RawDocumentReader::ErrorCode MultiKVDocReader::getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint,
                                                             DocInfo& docInfo)
{
    if (_totalItems.size() == 0 || !_docReader) {
        return ERROR_EOF;
    }
    while (true) {
        uint32_t timestampInSecond;
        if (!_docReader->IsEof() && _docReader->Read(&rawDoc, timestampInSecond)) {
            rawDoc.setDocOperateType(ADD_DOC);
            reportRawDocSize(rawDoc);
            docInfo.timestamp = SecondToMicrosecond(timestampInSecond);
            if (_autoCommit) {
                updateCommitedCheckpoint(checkpoint);
            }
            return ERROR_NONE;
        }
        if (_docReader->IsEof()) {
            if (isEof()) {
                BS_LOG(INFO, "multi kv doc reader get EOF");
                return ERROR_EOF;
            } else {
                if (!switchPartitionShard(_currentProgress.itemIdx + 1, 0, 0)) {
                    string errorMsg = "switch partition failed";
                    REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg, BS_IGNORE);
                    return ERROR_EXCEPTION;
                }
            }
        } else {
            stringstream errorMsg;
            errorMsg << "read document failed with idx [" << _currentProgress.itemIdx << " "
                     << _currentProgress.segmentIdx << " " << _currentProgress.offsetInSegment << "]";
            REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg.str(), BS_IGNORE);
            return ERROR_EXCEPTION;
        }
    }
}

void MultiKVDocReader::reportRawDocSize(document::RawDocument& rawDoc)
{
    size_t rawDocSize = rawDoc.EstimateMemory();
    REPORT_METRIC(_rawDocSizeMetric, rawDocSize);
    INCREASE_VALUE(_rawDocSizePerSecMetric, rawDocSize);
}

bool MultiKVDocReader::seek(const Checkpoint& checkpoint)
{
    if (checkpoint.userData.size() != sizeof(_currentProgress)) {
        BS_LOG(ERROR, "checkpoint size error [%lu]", checkpoint.userData.size());
        return false;
    }

    KVCheckPoint tmpCheckpoint;
    memcpy(&tmpCheckpoint, checkpoint.userData.data(), sizeof(tmpCheckpoint));
    if (tmpCheckpoint.itemIdx < 0 || tmpCheckpoint.itemIdx >= (int64_t)_totalItems.size()) {
        BS_LOG(ERROR, "failed to seek checkpoint with item idx [%ld]", tmpCheckpoint.itemIdx);
        return false;
    }

    if (!switchPartitionShard(tmpCheckpoint.itemIdx, tmpCheckpoint.segmentIdx, tmpCheckpoint.offsetInSegment)) {
        BS_LOG(ERROR,
               "seek to partition [%u, %u], shard id [%u], "
               "checkpoint itemIdx[%ld], segmentIdx[%ld], offsetInSegment[%ld] failed",
               _totalItems[tmpCheckpoint.itemIdx].indexRange.first,
               _totalItems[tmpCheckpoint.itemIdx].indexRange.second, _totalItems[tmpCheckpoint.itemIdx].shardIdx,
               tmpCheckpoint.itemIdx, tmpCheckpoint.segmentIdx, tmpCheckpoint.offsetInSegment);
        return false;
    }

    _checkpointDocCounter = checkpoint.offset;
    _committedCheckPoint.itemIdx = tmpCheckpoint.itemIdx;
    _committedCheckPoint.segmentIdx = tmpCheckpoint.segmentIdx;
    _committedCheckPoint.offsetInSegment = tmpCheckpoint.offsetInSegment;

    BS_LOG(INFO,
           "seek to partition [%u, %u], shard id [%u], "
           "checkpoint itemIdx[%ld], segmentIdx[%ld], offsetInSegment[%ld] success",
           _totalItems[tmpCheckpoint.itemIdx].indexRange.first, _totalItems[tmpCheckpoint.itemIdx].indexRange.second,
           _totalItems[tmpCheckpoint.itemIdx].shardIdx, _committedCheckPoint.itemIdx, _committedCheckPoint.segmentIdx,
           _committedCheckPoint.offsetInSegment);
    return true;
}

void MultiKVDocReader::updateCommitedCheckpoint(Checkpoint* checkpoint)
{
    auto readerOffset = _docReader->GetCurrentOffset();
    _committedCheckPoint.itemIdx = _currentProgress.itemIdx;
    _committedCheckPoint.segmentIdx = readerOffset.first;
    _committedCheckPoint.offsetInSegment = readerOffset.second;
    getCommitedCheckpoint(checkpoint);
}

void MultiKVDocReader::getCommitedCheckpoint(Checkpoint* checkpoint)
{
    checkpoint->userData.assign((char*)&_committedCheckPoint, sizeof(_committedCheckPoint));
    checkpoint->offset = _checkpointDocCounter;
    _checkpointDocCounter++;
}

bool MultiKVDocReader::isEof() const
{
    assert(_docReader);
    return _currentProgress.itemIdx + 1 == (int32_t)_totalItems.size() && _docReader->IsEof();
}

void MultiKVDocReader::doFillDocTags(document::RawDocument& rawDoc) {}

bool MultiKVDocReader::loadSrcSchema(const string& indexRootPath)
{
    if (!_srcSchema) {
        FileSystemOptions fileSystemOptions;
        IFileSystemPtr fileSystem = FileSystemCreator::CreateForRead("schema_dir", indexRootPath).GetOrThrow();
        auto ec = fileSystem->MountVersion(indexRootPath, -1, "", FSMT_READ_ONLY, nullptr).Code();
        if (FSEC_OK != ec) {
            BS_LOG(ERROR, "load src schema failed, ec [%d]", ec);
            return false;
        }
        DirectoryPtr rootDir = Directory::Get(fileSystem);
        _srcSchema = SchemaAdapter::LoadAndRewritePartitionSchema(rootDir, _options);
        SchemaRewriter::Rewrite(_srcSchema, _options, rootDir);
    }
    return true;
}

void MultiKVDocReader::selectShardItems(uint32_t myFrom, uint32_t myTo, versionid_t versionId,
                                        vector<pair<pair<uint32_t, uint32_t>, string>>& rangePath)
{
    uint32_t shardCount = 0;
    for (const auto& range : rangePath) {
        if (!(range.first.second < myFrom || myTo < range.first.first)) {
            if (0 == shardCount) {
                Version version;
                VersionLoader::GetVersionS(range.second, version, versionId);
                shardCount = version.GetLevelInfo().GetShardCount();
            }
            assert((range.first.second - range.first.first + 1) % shardCount == 0);
            uint32_t rangePerShard = (range.first.second - range.first.first + 1) / shardCount;
            for (auto shardBegin = range.first.first; shardBegin <= range.first.second; shardBegin += rangePerShard) {
                if (shardBegin >= myFrom && shardBegin <= myTo) {
                    uint32_t shardIdx = (shardBegin - range.first.first) / rangePerShard;
                    _totalItems.emplace_back(range.second, versionId, shardIdx, range.first);
                    BS_LOG(INFO, "partition [%s] shard [%u] add to items for reader with range [%u %u]",
                           range.second.c_str(), shardIdx, myFrom, myTo);
                }
            }
        }
    }
}

}} // namespace build_service::reader
