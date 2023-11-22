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
#include "indexlib/config/impl/offline_config_impl.h"

#include "autil/StringUtil.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, OfflineConfigImpl);

OfflineConfigImpl::OfflineConfigImpl() : mNeedRebuildAdaptiveBitmap(false), mNeedRebuildTruncateIndex(false) {}

OfflineConfigImpl::OfflineConfigImpl(const OfflineConfigImpl& other)
    : mNeedRebuildAdaptiveBitmap(other.mNeedRebuildAdaptiveBitmap)
    , mNeedRebuildTruncateIndex(other.mNeedRebuildTruncateIndex)
    , mLoadConfigList(other.mLoadConfigList)
    , mModuleInfos(other.mModuleInfos)
{
}

OfflineConfigImpl::~OfflineConfigImpl() {}

void OfflineConfigImpl::Jsonize(Jsonizable::JsonWrapper& json) { json.Jsonize("modules", mModuleInfos, ModuleInfos()); }

void OfflineConfigImpl::Check() const {}

void OfflineConfigImpl::operator=(const OfflineConfigImpl& other)
{
    mNeedRebuildTruncateIndex = other.mNeedRebuildTruncateIndex;
    mNeedRebuildAdaptiveBitmap = other.mNeedRebuildAdaptiveBitmap;
    mLoadConfigList = other.mLoadConfigList;
    mModuleInfos = other.mModuleInfos;
}

void OfflineConfigImpl::SetLoadConfigList(const LoadConfigList& loadConfigList) { mLoadConfigList = loadConfigList; }
}} // namespace indexlib::config
