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
#include "indexlib/index_base/index_meta/progress_synchronizer.h"

#include "indexlib/document/index_locator.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"

using namespace std;
using namespace indexlib::document;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, ProgressSynchronizer);

ProgressSynchronizer::ProgressSynchronizer() : mTimestamp(INVALID_TIMESTAMP), mFormatVersion(0) {}

ProgressSynchronizer::~ProgressSynchronizer() {}

static bool IndexLocatorCmp(const IndexLocator& lft, const IndexLocator& rht)
{
    if (lft.getSrc() == rht.getSrc()) {
        return rht.getOffset() < lft.getOffset();
    }
    return lft.getSrc() < rht.getSrc();
}

void ProgressSynchronizer::Init(const vector<Version>& versions)
{
    if (versions.size() == 1) {
        mTimestamp = versions[0].GetTimestamp();
        mLocator = versions[0].GetLocator();
        mFormatVersion = versions[0].GetFormatVersion();
        return;
    }
    for (const auto& version : versions) {
        mFormatVersion = max(mFormatVersion, version.GetFormatVersion());
    }
    vector<IndexLocator> indexLocatorVec;
    for (const auto& version : versions) {
        if (version.GetFormatVersion() == mFormatVersion) {
            mTimestamp = max(mTimestamp, version.GetTimestamp());
        }
        const Locator& locator = version.GetLocator();
        if (!locator.IsValid()) {
            continue;
        }
        IndexLocator indexLocator;
        indexLocator.fromString(locator.GetLocator());
        if (indexLocator.getOffset() == -1) {
            continue;
        }
        indexLocatorVec.push_back(indexLocator);
    }

    sort(indexLocatorVec.begin(), indexLocatorVec.end(), IndexLocatorCmp);
    if (!indexLocatorVec.empty()) {
        mLocator.SetLocator(indexLocatorVec.rbegin()->toString());
        IE_LOG(INFO,
               "progress in version sync to timestamp [%ld], "
               "locator [%lu:%ld]",
               mTimestamp, indexLocatorVec.rbegin()->getSrc(), indexLocatorVec.rbegin()->getOffset());
    }
}

void ProgressSynchronizer::Init(const vector<SegmentInfo>& segInfos)
{
    if (segInfos.size() == 1) {
        mTimestamp = segInfos[0].timestamp;
        mLocator = Locator(segInfos[0].GetLocator().Serialize());
        return;
    }

    vector<IndexLocator> indexLocatorVec;
    for (auto segInfo : segInfos) {
        mTimestamp = max(mTimestamp, segInfo.timestamp);
        auto locator = segInfo.GetLocator();
        if (!locator.IsValid()) {
            continue;
        }
        IndexLocator indexLocator;
        indexLocator.fromString(locator.Serialize());
        if (indexLocator.getOffset() == -1) {
            continue;
        }
        indexLocatorVec.push_back(indexLocator);
    }
    sort(indexLocatorVec.begin(), indexLocatorVec.end(), IndexLocatorCmp);
    if (!indexLocatorVec.empty()) {
        mLocator.SetLocator(indexLocatorVec.rbegin()->toString());
        IE_LOG(INFO,
               "progress in segment info sync to timestamp [%ld], "
               "locator [%lu:%ld]",
               mTimestamp, indexLocatorVec.rbegin()->getSrc(), indexLocatorVec.rbegin()->getOffset());
    }
}
}} // namespace indexlib::index_base
