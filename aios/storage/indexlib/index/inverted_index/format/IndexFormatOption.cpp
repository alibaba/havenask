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
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, IndexFormatOption);

void IndexFormatOption::Init(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfigPtr)
{
    assert(indexConfigPtr);
    optionflag_t optionFlag = indexConfigPtr->GetOptionFlag();
    // TODO:
    if (indexConfigPtr->GetInvertedIndexType() == it_expack) {
        optionFlag |= of_fieldmap;
    }

    _isNumberIndex = InvertedIndexUtil::IsNumberIndex(indexConfigPtr);
    // TODO: indexlibv2::config::InvertedIndexConfig and InvertedIndexType should match, need add some testcase
    // TODO: no position in number and string index, assert(!(optionFlag & of_position_list))
    // TODO: why section attr depends on position list?

    _postingFormatOption.Init(indexConfigPtr);

    if (OwnSectionAttribute(indexConfigPtr)) {
        _hasSectionAttribute = true;
    }

    if (indexConfigPtr->GetHighFreqVocabulary()) {
        _hasBitmapIndex = true;
    }
}

bool IndexFormatOption::OwnSectionAttribute(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfigPtr)
{
    auto packageIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(indexConfigPtr);
    if (packageIndexConfig && packageIndexConfig->HasSectionAttribute()) {
        return true;
    }
    return false;
}

std::string IndexFormatOption::ToString(const IndexFormatOption& option)
{
    JsonizableIndexFormatOption jsonizableOption(option);
    return autil::legacy::ToJsonString(jsonizableOption);
}

IndexFormatOption IndexFormatOption::FromString(const std::string& str)
{
    JsonizableIndexFormatOption jsonizableOption;
    autil::legacy::FromJsonString(jsonizableOption, str);
    return jsonizableOption.GetIndexFormatOption();
}

bool IndexFormatOption::Load(const std::shared_ptr<file_system::Directory>& directory)
{
    if (!directory->IsExist(INDEX_FORMAT_OPTION_FILE_NAME)) {
        return false;
    }

    std::shared_ptr<file_system::FileReader> fileReader =
        directory->CreateFileReader(INDEX_FORMAT_OPTION_FILE_NAME, file_system::FSOT_MEM);

    int64_t fileLen = fileReader->GetLogicLength();
    std::string content;
    content.resize(fileLen);
    char* data = const_cast<char*>(content.c_str());
    fileReader->Read(data, fileLen, 0).GetOrThrow();
    *this = FromString(content);
    return true;
}

void IndexFormatOption::Store(const std::shared_ptr<file_system::Directory>& directory)
{
    std::string content = ToString(*this);
    directory->Store(INDEX_FORMAT_OPTION_FILE_NAME, content, file_system::WriterOption());
}

bool IndexFormatOption::TEST_Equals(IndexFormatOption* lhs, IndexFormatOption* rhs)
{
    if (lhs->HasSectionAttribute() != rhs->HasSectionAttribute()) {
        return false;
    }
    if (lhs->HasBitmapIndex() != rhs->HasBitmapIndex()) {
        return false;
    }
    if (lhs->IsNumberIndex() != rhs->IsNumberIndex()) {
        return false;
    }
    if (lhs->_postingFormatOption == rhs->_postingFormatOption) {
        return true;
    }
    return false;
}

JsonizableIndexFormatOption::JsonizableIndexFormatOption(IndexFormatOption option)
    : _option(option)
    , _postingFormatOption(option._postingFormatOption)
{
}

const IndexFormatOption& JsonizableIndexFormatOption::GetIndexFormatOption() const { return _option; }

void JsonizableIndexFormatOption::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("has_section_attribute", _option._hasSectionAttribute);
    json.Jsonize("has_bitmap_index", _option._hasBitmapIndex);
    json.Jsonize("posting_format_option", _postingFormatOption);
    if (json.GetMode() == FROM_JSON) {
        _option._postingFormatOption = _postingFormatOption.GetPostingFormatOption();
    }
}

} // namespace indexlib::index
