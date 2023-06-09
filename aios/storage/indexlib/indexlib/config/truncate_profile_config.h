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
#ifndef __INDEXLIB_TRUNCATE_PROFILE_CONFIG_H
#define __INDEXLIB_TRUNCATE_PROFILE_CONFIG_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/index/inverted_index/config/PayloadConfig.h"
#include "indexlib/index/inverted_index/config/TruncateProfileConfig.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class TruncateProfileConfig : public indexlibv2::config::TruncateProfileConfig
{
public:
    TruncateProfileConfig();
    ~TruncateProfileConfig() = default;
    // for test
    TruncateProfileConfig(const std::string& profileName, const std::string& sortDesp);

public:
    void AssertEqual(const TruncateProfileConfig& other) const;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateProfileConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_TRUNCATE_PROFILE_CONFIG_H
