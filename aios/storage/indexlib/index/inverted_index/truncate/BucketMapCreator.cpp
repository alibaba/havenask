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
#include "indexlib/index/inverted_index/truncate/BucketMapCreator.h"

#include <algorithm>

#include "autil/ThreadPool.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/inverted_index/truncate/SortWorkItem.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BucketMapCreator);

std::pair<Status, BucketMaps> BucketMapCreator::CreateBucketMaps(
    const std::shared_ptr<indexlibv2::config::TruncateOptionConfig>& truncateOptionConfig,
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& tabletSchema,
    const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper, TruncateAttributeReaderCreator* attrReaderCreator,
    int64_t segmentMergePlanIdx)
{
    const indexlibv2::config::TruncateProfileVec& truncateProfiles = truncateOptionConfig->GetTruncateProfiles();
    assert(truncateProfiles.size() > 0);
    indexlibv2::config::TruncateProfileVec needSortTruncateProfiles;
    for (size_t i = 0; i < truncateProfiles.size(); ++i) {
        const std::shared_ptr<indexlibv2::config::TruncateProfile>& profile = truncateProfiles[i];
        const std::string& strategyName = profile->truncateStrategyName;
        std::shared_ptr<indexlibv2::config::TruncateStrategy> truncateStrategy =
            truncateOptionConfig->GetTruncateStrategy(strategyName);
        if (NeedCreateBucketMap(profile, truncateStrategy, tabletSchema)) {
            needSortTruncateProfiles.push_back(profile);
        }
    }

    BucketMaps bucketMaps;
    if (needSortTruncateProfiles.empty()) {
        AUTIL_LOG(INFO, "no need create bucket maps for sort truncate profiles is empty");
        return {Status::OK(), bucketMaps};
    }

    auto threadPool = CreateBucketMapThreadPool(needSortTruncateProfiles.size());
    threadPool->start("indexBucketMap");
    int64_t before = autil::TimeUtility::currentTimeInSeconds();
    for (const auto& needSortTruncateProfile : needSortTruncateProfiles) {
        const std::string& strategyName = needSortTruncateProfile->truncateStrategyName;
        auto truncStrategy = truncateOptionConfig->GetTruncateStrategy(strategyName);
        AUTIL_LOG(INFO, "create bucket map for truncate profile %s",
                  needSortTruncateProfile->truncateProfileName.c_str());
        const std::string bucketMapName =
            GetBucketMapName(segmentMergePlanIdx, needSortTruncateProfile->truncateProfileName);
        auto bucketMap = std::make_shared<BucketMap>(bucketMapName, index::BucketMap::GetBucketMapType());
        if (!bucketMap->Init(docMapper->GetNewDocCount())) {
            AUTIL_LOG(ERROR, "bucket map init error for truncate profile %s NewDocCount(%u)",
                      needSortTruncateProfile->truncateProfileName.c_str(), docMapper->GetNewDocCount());
            continue;
        }
        bucketMaps.insert(std::make_pair(needSortTruncateProfile->truncateProfileName, bucketMap));
        SortWorkItem* workItem = new SortWorkItem((*needSortTruncateProfile), docMapper->GetNewDocCount(),
                                                  attrReaderCreator, bucketMap, tabletSchema);
        if (autil::ThreadPool::ERROR_NONE != threadPool->pushWorkItem(workItem)) {
            AUTIL_LOG(ERROR, "push work item to thread pool failed.");
            workItem->destroy();
            return {Status::InternalError("push work item to thread pool failed"), bucketMaps};
        }
    }
    try {
        threadPool->waitFinish();
        threadPool->stop();
    } catch (...) {
        AUTIL_LOG(ERROR, "push work item failed, unknown exception");
        return {Status::InternalError("push work item exception"), bucketMaps};
    }

    int64_t after = autil::TimeUtility::currentTimeInSeconds();
    AUTIL_LOG(INFO, "Init bucket map success, segment merge planId[%ld] total bucket count[%lu] timeused[%ld] seconds",
              segmentMergePlanIdx, bucketMaps.size(), (after - before));
    return {Status::OK(), bucketMaps};
}

std::string BucketMapCreator::GetBucketMapName(size_t segmentMergePlanIdx, const std::string& truncateProfileName)
{
    return std::string("BucketMap_") + autil::StringUtil::toString(segmentMergePlanIdx) + "_" + truncateProfileName;
}

bool BucketMapCreator::NeedCreateBucketMap(
    const std::shared_ptr<indexlibv2::config::TruncateProfile>& truncateProfile,
    const std::shared_ptr<indexlibv2::config::TruncateStrategy>& truncateStrategy,
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& tabletSchema)
{
    if (!truncateProfile->HasSort()) {
        return false;
    }

    if (!truncateStrategy->HasLimit()) {
        return false;
    }

    const auto& sortParams = truncateProfile->sortParams;
    for (size_t i = 0; i < sortParams.size(); ++i) {
        const std::string sortField = sortParams[i].GetSortField();
        auto attributeConfig = tabletSchema->GetIndexConfig(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR, sortField);
        if (attributeConfig == nullptr) {
            AUTIL_LOG(INFO, "field [%s] not exist in attribute config , do not create bucket map, truncate profile[%s]",
                      sortField.c_str(), truncateProfile->truncateProfileName.c_str());
            return false;
        }
    }
    return true;
}

std::shared_ptr<autil::ThreadPool> BucketMapCreator::CreateBucketMapThreadPool(uint32_t truncateProfileSize)
{
    auto threadNum = std::min<uint32_t>(truncateProfileSize, 20);
    AUTIL_LOG(INFO, "thread num for MultiThreadBucketMapScheduler: [%u].", threadNum);
    return std::make_shared<autil::ThreadPool>(threadNum, autil::ThreadPoolBase::DEFAULT_QUEUESIZE,
                                               /*stopIfException*/ true);
}

} // namespace indexlib::index
