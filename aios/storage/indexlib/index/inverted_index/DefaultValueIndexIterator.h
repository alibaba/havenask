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
#include "indexlib/index/inverted_index/IndexIterator.h"
#include "indexlib/index/inverted_index/format/PostingDecoderImpl.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"

namespace indexlib::index {

class DefaultValueIndexIterator : public IndexIterator
{
public:
    DefaultValueIndexIterator(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                              const df_t df);
    ~DefaultValueIndexIterator() = default;

public:
    bool HasNext() const override;
    PostingDecoder* Next(index::DictKeyInfo& key) override;

private:
    df_t _df;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::unique_ptr<TermMeta> _termMeta;
    PostingFormatOption _postingFormatOption;
    std::unique_ptr<PostingDecoderImpl> _decoder;
    std::unique_ptr<DictionaryReader> _dicReader;
    std::shared_ptr<DictionaryIterator> _dictionaryIterator;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
