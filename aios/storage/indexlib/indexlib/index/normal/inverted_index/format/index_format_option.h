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
#pragma once

#include "indexlib/index/inverted_index/format/IndexFormatOption.h"

namespace indexlib::index {

class LegacyIndexFormatOption : public IndexFormatOption
{
public:
    static LegacyIndexFormatOption FromString(const std::string& str);

protected:
    bool OwnSectionAttribute(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfigPtr) override;

private:
    friend class LegacyJsonizableIndexFormatOption;
};

class LegacyJsonizableIndexFormatOption : public autil::legacy::Jsonizable
{
public:
    LegacyJsonizableIndexFormatOption(LegacyIndexFormatOption option = LegacyIndexFormatOption());

    const LegacyIndexFormatOption& GetIndexFormatOption() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    LegacyIndexFormatOption mOption;
    JsonizablePostingFormatOption mPostingFormatOption;
};

} // namespace indexlib::index
