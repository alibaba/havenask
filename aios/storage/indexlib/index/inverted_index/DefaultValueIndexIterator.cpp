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
#include "indexlib/index/inverted_index/DefaultValueIndexIterator.h"

#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/dictionary/DefaultTermDictionaryReader.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, DefaultValueIndexIterator);

DefaultValueIndexIterator::DefaultValueIndexIterator(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig, const df_t df)
    : _df(df)
    , _indexConfig(indexConfig)
{
    _postingFormatOption.Init(_indexConfig);
    _dicReader.reset(new DefaultTermDictionaryReader(_indexConfig, _df));
    auto status = _dicReader->Open(nullptr, "", false);
    THROW_IF_STATUS_ERROR(status);
    _dictionaryIterator = _dicReader->CreateIterator();
    _termMeta.reset(new TermMeta);
    _decoder.reset(new PostingDecoderImpl(_postingFormatOption));
}

bool DefaultValueIndexIterator::HasNext() const { return _dictionaryIterator->HasNext(); }

PostingDecoder* DefaultValueIndexIterator::Next(index::DictKeyInfo& key)
{
    dictvalue_t value;
    _dictionaryIterator->Next(key, value);
    bool isDocList = false;
    bool dfFirst = true;
    [[maybe_unused]] bool ret = ShortListOptimizeUtil::IsDictInlineCompressMode(value, isDocList, dfFirst);
    assert(ret);
    _decoder->Init(_termMeta.get(), value, isDocList, dfFirst);
    return _decoder.get();
}

} // namespace indexlib::index
