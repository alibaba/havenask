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
#include "indexlib/config/truncate_profile_config.h"

#include "indexlib/util/Status2Exception.h"
using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, TruncateProfileConfig);

TruncateProfileConfig::TruncateProfileConfig()
{
    static_assert(sizeof(*this) == sizeof(indexlibv2::config::TruncateProfileConfig),
                  "this class should have no members.");
}

TruncateProfileConfig::TruncateProfileConfig(const std::string& profileName, const std::string& sortDesp)
{
    TEST_SetTruncateProfileName(profileName);
    TEST_SetSortParams(sortDesp);
}

void TruncateProfileConfig::AssertEqual(const TruncateProfileConfig& other) const
{
    auto status = CheckEqual(other);
    THROW_IF_STATUS_ERROR(status);
}

}} // namespace indexlib::config
