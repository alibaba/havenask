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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"

namespace indexlib::index {

class DefaultTermDictionaryReader : public DictionaryReader
{
public:
    DefaultTermDictionaryReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config, df_t df)
        : _indexConfig(config)
        , _df(df)
    {
    }
    ~DefaultTermDictionaryReader() = default;

public:
    Status Open(const std::shared_ptr<file_system::Directory>&, const std::string&, bool) override;

    index::Result<bool> InnerLookup(dictkey_t key, indexlib::file_system::ReadOption option,
                                    dictvalue_t& value) noexcept override;

    std::shared_ptr<DictionaryIterator> CreateIterator() const override;

private:
    std::set<dictkey_t> _keys;
    dictvalue_t _defaultValue = INVALID_DICT_VALUE;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    df_t _df;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
