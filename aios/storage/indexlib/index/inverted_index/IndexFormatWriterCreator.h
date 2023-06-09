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

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/Types.h"

namespace indexlibv2::framework {
class SegmentStatistics;
}

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlib::util {
class SimplePool;
class BuildResourceMetrics;

} // namespace indexlib::util

namespace indexlib::index {
class BitmapIndexWriter;
class DictionaryWriter;
struct PostingWriterResource;
class PostingWriter;
class IndexFormatOption;

class IndexFormatWriterCreator
{
private:
    IndexFormatWriterCreator();
    ~IndexFormatWriterCreator();

public:
    static PostingWriter* CreatePostingWriter(InvertedIndexType indexType,
                                              PostingWriterResource* postingWriterResource);

    static DictionaryWriter*
    CreateDictionaryWriter(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlibv2::framework::SegmentStatistics>& segmentStatistics,
                           autil::mem_pool::PoolBase* pool);

    static BitmapIndexWriter* CreateBitmapIndexWriter(const std::shared_ptr<IndexFormatOption>& indexFormatOption,
                                                      bool isRealTime, autil::mem_pool::Pool* byteSlicePool,
                                                      util::SimplePool* simplePool);
    // static std::unique_ptr<index::SectionAttributeWriter>
    // CreateSectionAttributeWriter(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
    //                              const std::shared_ptr<index::IndexFormatOption>& indexFormatOption,
    //                              util::BuildResourceMetrics* buildResourceMetrics);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
