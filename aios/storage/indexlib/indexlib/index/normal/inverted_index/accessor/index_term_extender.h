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
#ifndef __INDEXLIB_INDEX_TERM_EXTENDER_H
#define __INDEXLIB_INDEX_TERM_EXTENDER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/inverted_index/IndexDataWriter.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib { namespace index { namespace legacy {
class MultiAdaptiveBitmapIndexWriter;
class TruncateIndexWriter;
class IndexTermExtender
{
public:
    enum TermOperation { TO_REMAIN, TO_DISCARD };

public:
    IndexTermExtender(const config::IndexConfigPtr& indexConfig,
                      const std::shared_ptr<legacy::TruncateIndexWriter>& truncateIndexWriter,
                      const std::shared_ptr<legacy::MultiAdaptiveBitmapIndexWriter>& adaptiveBitmapWriter);

    virtual ~IndexTermExtender();

public:
    void Init(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
              IndexOutputSegmentResources& indexOutputSegmentResources);

    TermOperation ExtendTerm(index::DictKeyInfo key, const PostingMergerPtr& postingMerger,
                             SegmentTermInfo::TermIndexMode mode);

    void Destroy();

private:
    bool MakeTruncateTermPosting(index::DictKeyInfo key, const PostingMergerPtr& postingMerger,
                                 SegmentTermInfo::TermIndexMode mode);

    bool MakeAdaptiveBitmapTermPosting(index::DictKeyInfo key, const PostingMergerPtr& postingMerger);

private:
    // virtual for UT
    virtual std::shared_ptr<PostingIterator> CreateCommonPostingIterator(const util::ByteSliceListPtr& sliceList,
                                                                         uint8_t compressMode,
                                                                         SegmentTermInfo::TermIndexMode mode);

private:
    config::IndexConfigPtr mIndexConfig;
    TruncateIndexWriterPtr mTruncateIndexWriter;
    std::shared_ptr<legacy::MultiAdaptiveBitmapIndexWriter> mAdaptiveBitmapIndexWriter;
    LegacyIndexFormatOption mIndexFormatOption;
    friend class IndexTermExtenderTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexTermExtender);
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_INDEX_TERM_EXTENDER_H
