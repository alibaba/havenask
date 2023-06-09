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
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"

#include "indexlib/config/package_index_config.h"

using namespace indexlib::config;

namespace indexlib::index {

bool LegacyIndexFormatOption::OwnSectionAttribute(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfigPtr)
{
    PackageIndexConfigPtr packageIndexConfig = std::dynamic_pointer_cast<config::PackageIndexConfig>(indexConfigPtr);
    if (packageIndexConfig && packageIndexConfig->HasSectionAttribute()) {
        return true;
    }
    return false;
}

LegacyIndexFormatOption LegacyIndexFormatOption::FromString(const std::string& str)
{
    LegacyJsonizableIndexFormatOption jsonizableOption;
    autil::legacy::FromJsonString(jsonizableOption, str);
    return jsonizableOption.GetIndexFormatOption();
}

LegacyJsonizableIndexFormatOption::LegacyJsonizableIndexFormatOption(LegacyIndexFormatOption option)
    : mOption(option)
    , mPostingFormatOption(option._postingFormatOption)
{
}

const LegacyIndexFormatOption& LegacyJsonizableIndexFormatOption::GetIndexFormatOption() const { return mOption; }

void LegacyJsonizableIndexFormatOption::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("has_section_attribute", mOption._hasSectionAttribute);
    json.Jsonize("has_bitmap_index", mOption._hasBitmapIndex);
    json.Jsonize("posting_format_option", mPostingFormatOption);
    if (json.GetMode() == FROM_JSON) {
        mOption._postingFormatOption = mPostingFormatOption.GetPostingFormatOption();
    }
}

} // namespace indexlib::index
