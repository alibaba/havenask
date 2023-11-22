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
#include "build_service/builder/BuilderV2Impl.h"

#include <assert.h>
#include <cstdint>
#include <map>
#include <unistd.h>
#include <vector>

#include "autil/Log.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common_define.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "fslib/common/common_type.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/ElementaryDocumentBatch.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/BuildDocumentMetrics.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/OpenOptions.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/framework/TabletMetrics.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"

namespace build_service::builder {

#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] " format, _buildId.ShortDebugString().c_str(), ##args)

BS_LOG_SETUP(builder, BuilderV2Impl);

BuilderV2Impl::BuilderV2Impl(std::shared_ptr<indexlibv2::framework::ITablet> tablet, const proto::BuildId& buildId)
    : BuilderV2(buildId)
    , _tablet(std::move(tablet))
    , _fatalError(false)
    , _needReconstruct(false)
    , _isSealed(false)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
    initBeeperTag(buildId);
}

BuilderV2Impl::~BuilderV2Impl() {}

bool BuilderV2Impl::init(const config::BuilderConfig& builderConfig,
                         std::shared_ptr<indexlib::util::MetricProvider> metricProvider)
{
    _builderConfig = builderConfig;
    if (_tablet) {
        auto counterMap = _tablet->GetTabletInfos()->GetCounterMap();
        if (!counterMap) {
            TABLET_LOG(WARN, "counterMap is NULL");
        } else {
            _totalDocCountCounter = GET_ACC_COUNTER(counterMap, totalDocCount);
            if (!_totalDocCountCounter) {
                TABLET_LOG(ERROR, "can not get totalDocCountCounter");
            }
            _builderDocCountCounter = GET_STATE_COUNTER(counterMap, builderDocCount);
            if (!_builderDocCountCounter) {
                TABLET_LOG(ERROR, "can not get builderDocCountCounter");
            } else {
                _builderDocCountCounter->Set(0);
            }
        }
        indexlibv2::framework::OpenOptions openOptions(indexlibv2::framework::OpenOptions::INCONSISTENT_BATCH,
                                                       _builderConfig.consistentModeBuildThreadCount,
                                                       _builderConfig.inconsistentModeBuildThreadCount);
        openOptions.SetUpdateControlFlowOnly(true);
        indexlibv2::framework::ReopenOptions reopenOptions(openOptions);
        if (!_tablet->Reopen(reopenOptions, indexlib::CONTROL_FLOW_VERSIONID).IsOK()) {
            TABLET_LOG(ERROR, "Set build mode in tablet failed");
            return false;
        }
        auto tabletDocCount = _tablet->GetTabletInfos()->GetTabletDocCount();
        if (_totalDocCountCounter) {
            _totalDocCountCounter->Increase(tabletDocCount);
        }
    }

    // _builderMetrics.declareMetrics(metricProvider);
    return true;
}

bool BuilderV2Impl::build(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch)
{
    assert(batch);
    std::lock_guard<std::mutex> _lock(_buildMutex);
    if (hasFatalError()) {
        return false;
    }
    auto elementaryBatch = std::dynamic_pointer_cast<indexlibv2::document::ElementaryDocumentBatch>(batch);
    if (elementaryBatch == nullptr) {
        for (size_t i = 0; i < batch->GetBatchSize(); ++i) {
            if ((*batch)[i]->GetDocOperateType() == DocOperateType::CHECKPOINT_DOC) {
                batch->DropDoc(i);
            }
        }
    }
    auto status = doBuild(batch);
    // TODO(hanyao): trace pk
    // _lastPk = batch->GetPrimaryKey();
    // if (status.IsOK()) {
    //     PkTracer::toBuildTrace(_lastPk, docOp);
    // } else {
    //     PkTracer::buildFailTrace(_lastPk, docOp);
    // }

    // build status:
    //  1. uninitialize: reconstruct realtime build flow (replay all docs from latest index locator)
    //  2. sealed: set sealed state, stop build flow
    //  3. internal error: set fatal state, stop build flow
    //  4. ok: doc consumed (if this doc should be skipped, return Status::OK() from tablet (skip doc is also treated as
    //  consumed ))
    //  5. otherwise retry this doc

    if (status.IsUninitialize()) {
        TABLET_LOG(INFO, "build uninitialize, maybe realtime is dropped: %s", status.ToString().c_str());
        setNeedReconstruct();
        return false;
    } else if (status.IsSealed()) {
        TABLET_LOG(INFO, "build sealed: %s", status.ToString().c_str());
        setSealed();
        return false;
    }
    if (!status.IsOK()) {
        proto::ServiceErrorCode ec = proto::BUILDER_ERROR_UNKNOWN;
        if (status.IsIOError()) {
            ec = proto::BUILDER_ERROR_BUILD_FILEIO;
        } else if (status.IsNoMem()) {
            ec = proto::BUILDER_ERROR_REACH_MAX_RESOURCE;
        }
        std::string errorMsg = status.ToString() + ". build id : " + _buildId.ShortDebugString();
        REPORT_ERROR_WITH_ADVICE(ec, errorMsg, proto::BS_RETRY);
        if (!status.IsNoMem()) {
            TABLET_LOG(ERROR, "build error: %s", status.ToString().c_str());
            setFatalError();
        } else {
            // avoid too much retry and no mem log
            sleep(2);
        }
    } else {
        // ReportMetrics should be done in indexlib BuildDocumentMetrics now.
        ReportCounters(batch);
    }
    return status.IsOK();
}

std::pair<indexlib::Status, indexlibv2::framework::Version> BuilderV2Impl::getLatestVersion() const
{
    auto indexRoot = _tablet->GetTabletInfos()->GetIndexRoot().GetLocalRoot();
    auto indexDir = indexlib::file_system::Directory::GetPhysicalDirectory(indexRoot);

    fslib::FileList fileList;
    auto status = indexlibv2::framework::VersionLoader::ListVersion(indexDir, &fileList);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "list version in path %s failed", indexRoot.c_str());
        return {indexlib::Status::InternalError(), indexlibv2::framework::Version()};
    }
    if (!fileList.empty()) {
        const auto& versionFileName = *fileList.rbegin();
        auto [st, version] =
            indexlibv2::framework::VersionLoader::LoadVersion(indexDir->GetIDirectory(), versionFileName);
        if (!st.IsOK()) {
            BS_LOG(ERROR, "load version %s in path %s failed", versionFileName.c_str(), indexRoot.c_str());
            return {indexlib::Status::InternalError(), indexlibv2::framework::Version()};
        }
        BS_LOG(INFO, "load version %s in path %s", versionFileName.c_str(), indexRoot.c_str());
        return {indexlib::Status::OK(), *version};
    }
    BS_LOG(INFO, "no version in path %s, will open invalid version", indexRoot.c_str());
    return {indexlib::Status::InternalError(), indexlibv2::framework::Version()};
}

bool BuilderV2Impl::merge()
{
    // tablet execute inc merge for bs local job
    if (hasFatalError()) {
        return false;
    }

    auto [st, sourceVersion] = getLatestVersion();
    if (!st.IsOK()) {
        setFatalError();
        BS_LOG(ERROR, "merge failed, get latest version failed");
        return false;
    }
    std::string taskType = indexlibv2::table::MERGE_TASK_TYPE;
    std::string taskName = indexlibv2::table::DESIGNATE_BATCH_MODE_MERGE_TASK_NAME;

    std::map<std::string, std::string> params;
    indexlibv2::framework::ReopenOptions reopenOptions;
    auto s = _tablet->Reopen(
        reopenOptions, indexlibv2::framework::VersionCoord(sourceVersion.GetVersionId(), sourceVersion.GetFenceName()));
    if (!s.IsOK()) {
        setFatalError();
        BS_LOG(ERROR, "tablet reopen failed");
        return false;
    }

    auto [status, _] = _tablet->ExecuteTask(sourceVersion, taskType, taskName, params);
    if (!status.IsOK()) {
        setFatalError();
        BS_LOG(ERROR, "tablet execute inc merge task failed");
        return false;
    }
    auto needCommit = _tablet->NeedCommit();
    if (needCommit) {
        auto st = _tablet->Commit(indexlibv2::framework::CommitOptions().SetNeedPublish(true)).first;
        if (!st.IsOK()) {
            setFatalError();
            BS_LOG(ERROR, "tablet commit failed");
            return false;
        }
    }

    return true;
}

void BuilderV2Impl::ReportCounters(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch)
{
    auto validMessageCount = batch->GetValidDocCount();
    if (validMessageCount > 0) {
        if (_totalDocCountCounter) {
            _totalDocCountCounter->Increase(validMessageCount);
        }
        if (_builderDocCountCounter) {
            if (!_tablet) {
                return;
            }
            auto tabletInfos = _tablet->GetTabletInfos();
            if (!tabletInfos) {
                return;
            }
            auto tabletMetrics = tabletInfos->GetTabletMetrics();
            if (!tabletMetrics) {
                return;
            }
            auto buildMetrics = tabletMetrics->GetBuildDocumentMetrics();
            if (!buildMetrics) {
                return;
            }
            _builderDocCountCounter->Set(buildMetrics->GetTotalDocCount());
        }
    }
}

void BuilderV2Impl::stop(std::optional<int64_t> stopTimestamp, bool needSeal, bool immediately)
{
    if (hasFatalError()) {
        TABLET_LOG(INFO, "fatal error happened, do not endIndex");
        return;
    }
    if (!stopTimestamp) {
        TABLET_LOG(INFO, "stopTimestamp not specified, stop without seal/commit");
        return;
    }

    TABLET_LOG(INFO, "builder stop begin");
    std::lock_guard<std::mutex> lock(_buildMutex);

    TABLET_LOG(INFO, "end index with stopTimestamp = [%ld]", stopTimestamp.value());
    auto status = needSeal ? _tablet->Seal() : _tablet->Flush();
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "end index failed, %s", status.ToString().c_str());
        setFatalError();
        proto::ServiceErrorCode ec = proto::BUILDER_ERROR_UNKNOWN;
        if (status.IsIOError()) {
            ec = proto::BUILDER_ERROR_DUMP_FILEIO;
        }
        REPORT_ERROR_WITH_ADVICE(ec, status.ToString(), proto::BS_RETRY);
        return;
    } else {
        if (needSeal) {
            setSealed();
        }
    }
    // TODO(hanyao): commit with stopTimestamp (offline build)
    TABLET_LOG(INFO, "builder stop end");
}

std::pair<indexlib::Status, indexlibv2::framework::VersionMeta>
BuilderV2Impl::commit(const indexlibv2::framework::CommitOptions& options)
{
    return _tablet->Commit(options);
}

int64_t BuilderV2Impl::getIncVersionTimestamp() const
{
    if (_tablet) {
        return _tablet->GetTabletInfos()->GetLoadedPublishVersion().GetTimestamp();
    }
    return INVALID_TIMESTAMP;
}

indexlibv2::framework::Locator BuilderV2Impl::getLastLocator() const
{
    if (_tablet) {
        return _tablet->GetTabletInfos()->GetLatestLocator();
    }
    return indexlibv2::framework::Locator();
}

indexlibv2::framework::Locator BuilderV2Impl::getLatestVersionLocator() const
{
    if (_tablet) {
        return _tablet->GetTabletInfos()->GetLoadedPublishVersion().GetLocator();
    }
    return indexlibv2::framework::Locator();
}
indexlibv2::framework::Locator BuilderV2Impl::getLastFlushedLocator() const
{
    if (_tablet) {
        return _tablet->GetTabletInfos()->GetLastCommittedLocator();
    }
    return indexlibv2::framework::Locator();
}

bool BuilderV2Impl::hasFatalError() const { return _fatalError.load(); }
void BuilderV2Impl::setFatalError() { _fatalError.store(true); }
std::shared_ptr<indexlib::util::CounterMap> BuilderV2Impl::GetCounterMap() const
{
    if (!_tablet) {
        return nullptr;
    }
    return _tablet->GetTabletInfos()->GetCounterMap();
}
void BuilderV2Impl::setNeedReconstruct() { _needReconstruct.store(true); }
bool BuilderV2Impl::needReconstruct() const { return _needReconstruct.load(); }
void BuilderV2Impl::setSealed() { _isSealed.store(true); }
bool BuilderV2Impl::isSealed() const { return _isSealed.load(); }

indexlib::Status BuilderV2Impl::doBuild(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch)
{
    return _tablet->Build(batch);
}

void BuilderV2Impl::switchToConsistentMode()
{
    indexlibv2::framework::OpenOptions::BuildMode buildMode = (_builderConfig.consistentModeBuildThreadCount > 1)
                                                                  ? indexlibv2::framework::OpenOptions::CONSISTENT_BATCH
                                                                  : indexlibv2::framework::OpenOptions::STREAM;
    indexlibv2::framework::OpenOptions openOptions(buildMode, _builderConfig.consistentModeBuildThreadCount,
                                                   _builderConfig.inconsistentModeBuildThreadCount);
    openOptions.SetUpdateControlFlowOnly(true);
    indexlibv2::framework::ReopenOptions reopenOptions(openOptions);
    indexlib::Status st = _tablet->Reopen(reopenOptions, indexlib::CONTROL_FLOW_VERSIONID);
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "switch to consistent mode failed, %s", st.ToString().c_str());
        setFatalError();
        proto::ServiceErrorCode ec = proto::BUILDER_ERROR_UNKNOWN;
        REPORT_ERROR_WITH_ADVICE(ec, st.ToString(), proto::BS_RETRY);
        return;
    }
}

#undef TABLET_NAME

} // namespace build_service::builder
