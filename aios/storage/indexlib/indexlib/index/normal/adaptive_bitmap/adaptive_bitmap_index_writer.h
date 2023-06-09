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
#ifndef __INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_H
#define __INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(file_system, ArchiveFolder);
DECLARE_REFERENCE_CLASS(index, BitmapPostingWriter);
DECLARE_REFERENCE_CLASS(index, PostingMerger);
DECLARE_REFERENCE_CLASS(index, PostingIterator);

namespace indexlib { namespace index { namespace legacy {
class AdaptiveBitmapTrigger;

class AdaptiveBitmapIndexWriter
{
public:
    AdaptiveBitmapIndexWriter(const std::shared_ptr<AdaptiveBitmapTrigger>& trigger,
                              const config::IndexConfigPtr& indexConf,
                              const file_system::ArchiveFolderPtr& adaptiveDictFolder);

    virtual ~AdaptiveBitmapIndexWriter();

public:
    void Init(const std::shared_ptr<DictionaryWriter>& dictWriter, const file_system::FileWriterPtr& postingWriter);

    bool NeedAdaptiveBitmap(const index::DictKeyInfo& dictKey, const PostingMergerPtr& postingMerger);

    virtual void AddPosting(const index::DictKeyInfo& dictKey, termpayload_t termPayload,
                            const std::shared_ptr<PostingIterator>& postingIt);

    void EndPosting();

    int64_t EstimateMemoryUse(uint32_t docCount) const;

public:
    // for test
    const std::vector<index::DictKeyInfo>& GetAdaptiveDictKeys() { return mAdaptiveDictKeys; }

protected:
    void CollectDocIds(const std::shared_ptr<PostingIterator>& postingIt, std::vector<docid_t>& docIds);

    void DumpPosting(const index::DictKeyInfo& dictkey, const std::shared_ptr<BitmapPostingWriter>& writer);

    void DumpAdaptiveDictFile();

protected:
    typedef TieredDictionaryWriter<dictkey_t> BitmapDictionaryWriter;
    typedef std::shared_ptr<BitmapDictionaryWriter> BitmapDictionaryWriterPtr;

    std::shared_ptr<AdaptiveBitmapTrigger> mAdaptiveBitmapTrigger;
    config::IndexConfigPtr mIndexConfig;
    file_system::ArchiveFolderPtr mAdaptiveDictFolder;

    BitmapDictionaryWriterPtr mDictionaryWriter;
    file_system::FileWriterPtr mPostingWriter;

    std::vector<index::DictKeyInfo> mAdaptiveDictKeys;

private:
    friend class AdaptiveBitmapIndexWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveBitmapIndexWriter);
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_H
