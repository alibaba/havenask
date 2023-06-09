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
#include "indexlib/index_base/segment/offline_segment_directory.h"

#include "indexlib/index_base/offline_recover_strategy.h"

using namespace std;

using namespace indexlib::config;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, OfflineSegmentDirectory);

OfflineSegmentDirectory::OfflineSegmentDirectory(bool enableRecover, RecoverStrategyType recoverType)
    : mEnableRecover(enableRecover)
    , mRecoverType(recoverType)
{
}

OfflineSegmentDirectory::OfflineSegmentDirectory(const OfflineSegmentDirectory& other)
    : SegmentDirectory(other)
    , mEnableRecover(other.mEnableRecover)
    , mRecoverType(other.mRecoverType)
{
}

OfflineSegmentDirectory::~OfflineSegmentDirectory() {}

void OfflineSegmentDirectory::DoInit(const file_system::DirectoryPtr& directory)
{
    if (!mIsSubSegDir && mEnableRecover) {
        mVersion = OfflineRecoverStrategy::Recover(mVersion, directory, mRecoverType);
    }
}
}} // namespace indexlib::index_base
