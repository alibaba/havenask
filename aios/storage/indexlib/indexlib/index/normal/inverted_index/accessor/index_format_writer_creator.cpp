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
#include "indexlib/index/normal/inverted_index/accessor/index_format_writer_creator.h"

#include "indexlib/config/index_config.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexWriter.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::common;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, IndexFormatWriterCreator);

PostingWriter* IndexFormatWriterCreator::CreatePostingWriter(InvertedIndexType indexType,
                                                             PostingWriterResource* postingWriterResource)
{
    assert(postingWriterResource);
    PostingWriter* postingWriter = nullptr;
    switch (indexType) {
    case it_string:
    case it_spatial:
    case it_datetime:
    case it_range:
    case it_text:
    case it_pack:
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
    case it_expack:
        postingWriter = POOL_NEW_CLASS(postingWriterResource->byteSlicePool, PostingWriterImpl, postingWriterResource);
        break;
    default:
        assert(false);
    }
    return postingWriter;
}

DictionaryWriter* IndexFormatWriterCreator::CreateDictionaryWriter(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig, const std::string& temperatureLayer,
    autil::mem_pool::PoolBase* pool)
{
    return DictionaryCreator::CreateWriter(indexConfig, temperatureLayer, pool);
}

BitmapIndexWriter*
IndexFormatWriterCreator::CreateBitmapIndexWriter(const std::shared_ptr<LegacyIndexFormatOption>& indexFormatOption,
                                                  bool isRealTime, Pool* byteSlicePool, SimplePool* simplePool)
{
    if (!indexFormatOption->HasBitmapIndex()) {
        return nullptr;
    }
    return POOL_NEW_CLASS(byteSlicePool, BitmapIndexWriter, byteSlicePool, simplePool,
                          indexFormatOption->IsNumberIndex(), isRealTime);
}

std::unique_ptr<index::SectionAttributeWriter> IndexFormatWriterCreator::CreateSectionAttributeWriter(
    const config::IndexConfigPtr& indexConfig, const std::shared_ptr<LegacyIndexFormatOption>& indexFormatOption,
    BuildResourceMetrics* buildResourceMetrics)
{
    if (indexFormatOption->HasSectionAttribute()) {
        PackageIndexConfigPtr packageIndexConfig = std::dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
        auto sectionAttributeWriter = std::make_unique<SectionAttributeWriter>(packageIndexConfig);
        sectionAttributeWriter->Init(/*indexConfig=*/nullptr, buildResourceMetrics);
        return sectionAttributeWriter;
    }
    return nullptr;
}
}}} // namespace indexlib::index::legacy
