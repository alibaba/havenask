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
#include "build_service/reader/IndexDocReader.h"

#include "autil/StringUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/reader/IdleDocumentParser.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/MemUtil.h"
#include "build_service/util/Monitor.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/config/BackgroundTaskConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/ReadResource.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/TaskScheduler.h"

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, IndexDocReader);

const std::string IndexDocReader::USER_REQUIRED_FIELDS = "read_index_required_fields";

class NoDataTabletDocIterator : public indexlibv2::framework::ITabletDocIterator
{
public:
    indexlib::Status Init(const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData,
                          std::pair<uint32_t /*0-99*/, uint32_t /*0-99*/>,
                          const std::shared_ptr<indexlibv2::framework::MetricsManager>&,
                          const std::optional<std::vector<std::string>>&,
                          const std::map<std::string, std::string>&) override
    {
        if (tabletData->GetSegmentCount() > 0) {
            assert(false);
            return indexlib::Status::Corruption("only support no data");
        }
        return indexlib::Status::OK();
    }

    indexlib::Status Next(indexlib::document::RawDocument*, std::string*,
                          indexlibv2::framework::Locator::DocInfo*) override
    {
        return indexlib::Status::Corruption("please call HasNext first");
    }
    bool HasNext() const override { return false; }
    indexlib::Status Seek(const std::string& checkpoint) override { return indexlib::Status::OK(); }
};

bool IndexDocReader::init(const ReaderInitParam& params)
{
    bool prepare = false;
    std::tie(prepare, _iterParams) = prepareIterParams(params);
    if (!prepare) {
        return false;
    }
    _parameters = params;
    _metricsManager = std::make_shared<indexlibv2::framework::MetricsManager>(
        "", _parameters.metricProvider ? _parameters.metricProvider->GetReporter() : nullptr);

    if (_iterParams.empty()) {
        BS_LOG(INFO, "reader [%d, %d] has no data to do", params.range.from(), params.range.to());
        return true;
    }
    if (!switchTablet(0).IsOK()) {
        BS_LOG(ERROR, "switch tablet failed");
        return false;
    }
    _readLatencyMetric = DECLARE_METRIC(params.metricProvider, "basic/readLatency", kmonitor::GAUGE, "ms");
    BS_LOG(INFO, "bs indexDocReader init success");
    return true;
}

void IndexDocReader::doFillDocTags(document::RawDocument& rawDoc) { assert(false); }

indexlib::document::RawDocumentParser* IndexDocReader::createRawDocumentParser(const ReaderInitParam& params)
{
    return new IdleDocumentParser("idle");
}

std::pair<bool, std::vector<IndexDocReader::IterParam>> IndexDocReader::prepareIterParams(const ReaderInitParam& params)
{
    std::pair<uint32_t, uint32_t> readerRange(params.range.from(), params.range.to());
    std::vector<IndexDocReader::IterParam> ret;
    std::string indexRoot = getValueFromKeyValueMap(params.kvMap, "read_index_path");
    if (indexRoot.empty()) {
        BS_LOG(ERROR, "index reader's read_index_path cannot be empty");
        return {false, ret};
    }
    std::string indexInfoPath = getValueFromKeyValueMap(params.kvMap, "index_version_info_path");
    if (indexInfoPath.empty()) {
        BS_LOG(ERROR, "index reader's index_version_info_path cannot be empty");
        return {false, ret};
    }
    std::string versionInfosStr;
    if (!indexlib::file_system::FslibWrapper::AtomicLoad(indexInfoPath, versionInfosStr).OK()) {
        BS_LOG(ERROR, "read index version infos failed from path [%s]", indexInfoPath.c_str());
        return {false, ret};
    }
    auto jsonMap =
        autil::legacy::AnyCast<autil::legacy::json::JsonMap>(autil::legacy::json::ParseJson(versionInfosStr));
    auto iter = jsonMap.find("version_id_mapping");
    if (iter == jsonMap.end()) {
        BS_LOG(ERROR, "cannot get version_id_mapping from path [%s]", indexInfoPath.c_str());
        return {false, ret};
    }
    std::map<std::string, versionid_t> versionInfos;
    try {
        autil::legacy::FromJson(versionInfos, iter->second);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "[version_id_mapping] is invalid in path [%s]", indexInfoPath.c_str());
        return {false, ret};
    }
    for (auto [range, versionId] : versionInfos) {
        auto rangeInfos = autil::StringUtil::split(range, "_");
        if (rangeInfos.size() != 2) {
            BS_LOG(ERROR, "invalid version info for content [%s], path is [%s]", range.c_str(), indexInfoPath.c_str());
            return {false, ret};
        }
        uint32_t indexFrom = 0, indexTo = 0;
        if (!autil::StringUtil::fromString(rangeInfos[0], indexFrom) ||
            !autil::StringUtil::fromString(rangeInfos[1], indexTo) || indexFrom >= indexTo) {
            BS_LOG(ERROR, "invalid version info for content [%s], path is [%s]", range.c_str(), indexInfoPath.c_str());
            return {false, ret};
        }
        auto rangeInRatio = calculateRange(readerRange, {indexFrom, indexTo});
        if (rangeInRatio != std::nullopt) {
            std::string partitionPath = indexlib::file_system::FslibWrapper::JoinPath(
                indexRoot, util::IndexPathConstructor::PARTITION_PREFIX + std::to_string(indexFrom) + "_" +
                               std::to_string(indexTo) + "/");
            ret.push_back({partitionPath, rangeInRatio.value(), versionId});
        }
    }
    return {true, ret};
}

bool IndexDocReader::seek(const Checkpoint& checkpoint)
{
    if (checkpoint.userData.size() < sizeof(_currentIterId)) {
        return false;
    }
    _checkpointDocCounter = checkpoint.offset;
    std::string iterCheckpoint = checkpoint.userData.substr(sizeof(_currentIterId));
    std::string iterIdStr = checkpoint.userData.substr(0, sizeof(_currentIterId));
    int32_t iterId = *((int32_t*)iterIdStr.data());
    auto status = switchTablet(iterId);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "seek failed as switch tablet failed");
        return false;
    }
    status = _iter->Seek(iterCheckpoint);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "tablet doc iter seek failed, offset [%ld]", _checkpointDocCounter);
        return false;
    }
    return true;
}

std::unique_ptr<indexlibv2::framework::ITabletDocIterator>
IndexDocReader::createTabletDocIterator(indexlib::framework::ITabletExporter* tabletExporter, size_t segmentCount)
{
    if (segmentCount == 0) {
        BS_LOG(INFO, "current tablet data has no segment, not create real tablet reader");
        return std::make_unique<NoDataTabletDocIterator>();
    }
    return tabletExporter->CreateTabletDocIterator();
}

std::shared_ptr<indexlibv2::config::TabletOptions>
IndexDocReader::createTabletOptions(indexlib::framework::ITabletExporter* tabletExporter)
{
    auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
    auto tabletOptionsString = tabletExporter->CreateTabletOptionsString();
    if (tabletOptionsString == std::nullopt) {
        BS_LOG(ERROR, "none of tablet options");
        return nullptr;
    }
    try {
        FromJsonString(*tabletOptions, tabletOptionsString.value());
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "invaild tablet option string, exception [%s]", e.what());
        return nullptr;
    }

    tabletOptions->SetIsLeader(false);
    tabletOptions->SetFlushRemote(false);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetIsOnline(true);
    tabletOptions->MutableBackgroundTaskConfig().DisableAll();
    return tabletOptions;
}

indexlib::Status IndexDocReader::switchTablet(int32_t iterId)
{
    if (iterId < 0 || iterId >= _iterParams.size()) {
        assert(false);
        RETURN_STATUS_ERROR(OutOfRange, "current id [%d] is out of range size [%d]", _currentIterId,
                            _iterParams.size());
    }
    if (iterId == _currentIterId) {
        assert(_iter);
        return indexlib::Status::OK();
    }

    _currentIterId = iterId;
    auto [sourceRoot, range, versionId] = _iterParams[_currentIterId];
    AUTIL_LOG(INFO,
              "begin load tablet idx is [%d] sourceRoot is [%s], versionId is [%d],"
              "range is Ratio is [%d, %d]",
              iterId, sourceRoot.c_str(), versionId, range.first, range.second);

    indexlibv2::framework::Version srcVersion;
    auto versionRootDir = indexlib::file_system::Directory::GetPhysicalDirectory(sourceRoot);
    auto status = indexlibv2::framework::VersionLoader::GetVersion(versionRootDir, versionId, &srcVersion);
    RETURN_IF_STATUS_ERROR(status, "load version [%d] from [%s] failed", versionId, sourceRoot.c_str());

    auto schemaId = srcVersion.GetReadSchemaId();
    auto schema = std::shared_ptr<indexlibv2::config::ITabletSchema>(
        indexlibv2::framework::TabletSchemaLoader::GetSchema(versionRootDir->GetIDirectory(), schemaId));
    if (!schema) {
        RETURN_IF_STATUS_ERROR(indexlibv2::Status::InternalError(), "get schema [%d] failed from [%s]", schemaId,
                               sourceRoot.c_str());
    }

    double allowRatio = 0.90;
    uint64_t machineTotalMem = util::MemUtil::getMachineTotalMem();
    int64_t memoryQuota = static_cast<int64_t>(machineTotalMem * allowRatio);
    auto memoryQuotaController =
        std::make_shared<indexlibv2::MemoryQuotaController>(schema->GetTableName(), memoryQuota);

    auto blockCacheParam = autil::EnvUtil::getEnv<std::string>(
        /*key*/ "BS_BLOCK_CACHE",
        /*defaultValue*/ "memory_size_in_mb=512;block_size=2097152;io_batch_size=1;num_shard_bits=0");

    auto fileBlockCacheContainer = std::make_shared<indexlib::file_system::FileBlockCacheContainer>();
    if (!fileBlockCacheContainer->Init(blockCacheParam, memoryQuotaController, nullptr, _parameters.metricProvider)) {
        RETURN_STATUS_ERROR(Corruption, "create file block cache failed, param: [%s]", blockCacheParam.c_str());
    }

    indexlibv2::framework::TabletResource resouce;
    resouce.fileBlockCacheContainer = fileBlockCacheContainer;
    resouce.tabletId = indexlib::framework::TabletId(schema->GetTableName());
    resouce.memoryQuotaController = memoryQuotaController;
    resouce.metricsReporter = _parameters.metricProvider ? _parameters.metricProvider->GetReporter() : nullptr;
    auto tablet = std::make_shared<indexlibv2::framework::Tablet>(resouce);
    auto tabletFactory = indexlibv2::framework::TabletFactoryCreator::GetInstance()->Create(schema->GetTableType());
    if (!tabletFactory ||
        !tabletFactory->Init(std::make_shared<indexlibv2::config::TabletOptions>(), _metricsManager.get())) {
        RETURN_STATUS_ERROR(Corruption, "init factory failed");
    }

    auto tabletExporter = tabletFactory->CreateTabletExporter();
    auto tabletOptions = createTabletOptions(tabletExporter.get());
    if (!tabletOptions) {
        RETURN_STATUS_ERROR(Corruption, "create tabletOptions failed");
    }

    char cwdPath[1024];
    if (getcwd(cwdPath, sizeof(cwdPath)) == NULL) {
        RETURN_STATUS_ERROR(Corruption, "get current worker path failed");
    }
    indexlibv2::framework::IndexRoot indexRoot(std::string(cwdPath), sourceRoot);

    status = tablet->Open(indexRoot, schema, tabletOptions, indexlibv2::framework::VersionCoord(versionId));
    RETURN_IF_STATUS_ERROR(status, "open tablet [%s] failed for root [%s]", schema->GetTableName().c_str(),
                           sourceRoot.c_str());

    _iter = createTabletDocIterator(tabletExporter.get(), tablet->GetTabletData()->GetSegmentCount());
    _currentTablet = std::move(tablet);
    std::optional<std::vector<std::string>> requiredFields;
    std::string requiredFieldsStr = getValueFromKeyValueMap(_parameters.kvMap, USER_REQUIRED_FIELDS);
    if (!requiredFieldsStr.empty()) {
        std::vector<std::string> fieldNames;
        try {
            autil::legacy::FromJsonString(fieldNames, requiredFieldsStr);
            requiredFields = fieldNames;
            AUTIL_LOG(INFO, "required [%lu] fields [%s]", fieldNames.size(), requiredFieldsStr.c_str());
        } catch (const autil::legacy::ExceptionBase& e) {
            RETURN_STATUS_ERROR(InvalidArgs, "parse required fields [%s] failed", requiredFieldsStr.c_str());
        }
    }

    status = _iter->Init(_currentTablet->GetTabletData(), range, _metricsManager, requiredFields, _parameters.kvMap);
    RETURN_IF_STATUS_ERROR(status, "init iter failed, _currentIterId [%d]", _currentIterId);
    AUTIL_LOG(INFO,
              "end load tablet idx is [%d] sourceRoot is [%s], versionId is [%d],"
              "range is Ratio is [%d, %d]",
              iterId, sourceRoot.c_str(), versionId, range.first, range.second);
    return indexlib::Status::OK();
}

std::optional<std::pair<uint32_t, uint32_t>> IndexDocReader::calculateRange(std::pair<uint32_t, uint32_t> readerRange,
                                                                            std::pair<uint32_t, uint32_t> indexRanges)
{
    auto calc = [](uint32_t p, uint32_t factor) -> uint32_t { return p * 100 / factor; };
    auto [indexLeft, indexRight] = indexRanges;
    uint32_t left = std::max(indexLeft, readerRange.first);
    uint32_t right = std::min(indexRight, readerRange.second);
    if (left <= right) {
        auto normalizeLeft = calc(left - indexLeft, indexRight - indexLeft);
        auto normalizeRight = right == indexRight ? 100 : calc(right + 1 - indexLeft, indexRight - indexLeft);
        if (normalizeLeft < normalizeRight) {
            assert(normalizeLeft <= 99);
            assert(normalizeRight >= 1 && normalizeRight <= 100);
            BS_LOG(INFO, "reader range [%d, %d], indexRange [%d, %d], normalize to [%d, %d]", readerRange.first,
                   readerRange.second, indexLeft, indexRight, normalizeLeft, normalizeRight - 1);
            return std::make_pair(normalizeLeft, normalizeRight - 1);
        }
    }
    BS_LOG(INFO, "reader range [%d, %d], indexRange [%d, %d], nothing to do", readerRange.first, readerRange.second,
           indexLeft, indexRight);
    return std::nullopt;
}

void IndexDocReader::setCheckpoint(const std::string& iterCkp, Checkpoint* checkpoint)
{
    std::string currentIterIdStr(sizeof(_currentIterId), ' ');
    *((uint32_t*)currentIterIdStr.data()) = _currentIterId;
    checkpoint->userData = currentIterIdStr + iterCkp;
    checkpoint->offset = _checkpointDocCounter++;
}

RawDocumentReader::ErrorCode IndexDocReader::getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint,
                                                           DocInfo& docInfo)
{
    indexlib::util::ScopeLatencyReporter reporter(_readLatencyMetric.get());
    do {
        if (isEof()) {
            BS_LOG(INFO, "index reader read get EOF");
            return ERROR_EOF;
        }

        if (_iter->HasNext()) {
            std::string ckpt;
            auto status = _iter->Next(&rawDoc, &ckpt, &docInfo);
            if (!status.IsOK()) {
                BS_LOG(ERROR, "iter get next doc failed");
                return ERROR_EXCEPTION;
            }
            setCheckpoint(ckpt, checkpoint);
            return ERROR_NONE;
        }

        auto status = switchTablet(_currentIterId + 1);
        if (!status.IsOK()) {
            BS_LOG(ERROR, "index reader switch to next tablet failed");
            return ERROR_EXCEPTION;
        }
    } while (true);
}

bool IndexDocReader::isEof() const
{
    return _iterParams.empty() || (_currentIterId == _iterParams.size() - 1 && !_iter->HasNext());
}

}} // namespace build_service::reader
