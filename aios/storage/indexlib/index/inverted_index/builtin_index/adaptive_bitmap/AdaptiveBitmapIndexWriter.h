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
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlib::util {
class DictKeyInfo;
}

namespace indexlib::file_system {
class RelocatableFolder;
class FileWriter;
} // namespace indexlib::file_system

namespace indexlib::index {
class PostingIterator;
class PostingMerger;
class AdaptiveBitmapTrigger;
class DictionaryWriter;
class BitmapPostingWriter;

class AdaptiveBitmapIndexWriter
{
public:
    AdaptiveBitmapIndexWriter(const std::shared_ptr<AdaptiveBitmapTrigger>& trigger,
                              const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConf,
                              const std::shared_ptr<file_system::RelocatableFolder>& adaptiveDictFolder);

    virtual ~AdaptiveBitmapIndexWriter() = default;

    void Init(const std::shared_ptr<DictionaryWriter>& dictWriter,
              const std::shared_ptr<file_system::FileWriter>& postingWriter);

    bool NeedAdaptiveBitmap(const index::DictKeyInfo& dictKey, const std::shared_ptr<PostingMerger>& postingMerger);

    virtual void AddPosting(const index::DictKeyInfo& dictKey, termpayload_t termPayload,
                            const std::shared_ptr<PostingIterator>& postingIt);

    void EndPosting();

    int64_t EstimateMemoryUse(uint32_t docCount) const;

    const std::vector<index::DictKeyInfo>& GetAdaptiveDictKeys() { return _adaptiveDictKeys; }

protected:
    void CollectDocIds(const std::shared_ptr<PostingIterator>& postingIt, std::vector<docid_t>& docIds);

    void DumpPosting(const index::DictKeyInfo& dictkey, const std::shared_ptr<BitmapPostingWriter>& writer);

    void DumpAdaptiveDictFile();

protected:
    using BitmapDictionaryWriter = TieredDictionaryWriter<dictkey_t>;

    std::shared_ptr<AdaptiveBitmapTrigger> _adaptiveBitmapTrigger;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::shared_ptr<file_system::RelocatableFolder> _adaptiveDictFolder;

    std::shared_ptr<BitmapDictionaryWriter> _dictionaryWriter;
    std::shared_ptr<file_system::FileWriter> _postingWriter;

    std::vector<index::DictKeyInfo> _adaptiveDictKeys;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
