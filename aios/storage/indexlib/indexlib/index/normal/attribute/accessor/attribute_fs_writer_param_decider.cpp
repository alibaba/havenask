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
#include "indexlib/index/normal/attribute/accessor/attribute_fs_writer_param_decider.h"

#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeFSWriterParamDecider);

WriterOption AttributeFSWriterParamDecider::MakeParam(const string& fileName)
{
    WriterOption option;
    option.asyncDump = false;
    option.copyOnDump = false;
    if (mOptions.IsOffline()) {
        return option;
    }

    const OnlineConfig& onlineConfig = mOptions.GetOnlineConfig();
    if (!onlineConfig.onDiskFlushRealtimeIndex) {
        return option;
    }

    AttributeConfig::ConfigType attrConfType = mAttrConfig->GetConfigType();
    if (attrConfType != AttributeConfig::ct_normal && attrConfType != AttributeConfig::ct_virtual) {
        return option;
    }

    if (!mAttrConfig->IsAttributeUpdatable()) {
        return option;
    }

    if (mAttrConfig->IsMultiValue() || mAttrConfig->GetFieldType() == ft_string) {
        option.copyOnDump = (fileName == ATTRIBUTE_OFFSET_FILE_NAME);
        return option;
    }
    option.copyOnDump = (fileName == ATTRIBUTE_DATA_FILE_NAME);
    return option;
}
}} // namespace indexlib::index
