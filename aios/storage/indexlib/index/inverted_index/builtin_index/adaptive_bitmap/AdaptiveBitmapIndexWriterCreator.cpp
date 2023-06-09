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
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/AdaptiveBitmapIndexWriterCreator.h"

#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/MultiAdaptiveBitmapIndexWriter.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, AdaptiveBitmapIndexWriterCreator);

AdaptiveBitmapIndexWriterCreator::AdaptiveBitmapIndexWriterCreator(
    const std::shared_ptr<file_system::RelocatableFolder>& adaptiveDictFolder,
    const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper,
    const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments)
    : _adaptiveDictFolder(adaptiveDictFolder)
    , _triggerCreator(docMapper)
    , _targetSegments(targetSegments)
{
}

std::shared_ptr<AdaptiveBitmapTrigger> AdaptiveBitmapIndexWriterCreator::CreateAdaptiveBitmapTrigger(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConf)
{
    return _triggerCreator.Create(indexConf);
}

std::shared_ptr<MultiAdaptiveBitmapIndexWriter>
AdaptiveBitmapIndexWriterCreator::Create(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConf,
                                         const file_system::IOConfig& ioConfig)
{
    if (indexConf->GetShardingType() == indexlibv2::config::InvertedIndexConfig::IST_NEED_SHARDING) {
        return nullptr;
    }

    std::shared_ptr<AdaptiveBitmapTrigger> trigger = CreateAdaptiveBitmapTrigger(indexConf);
    if (!trigger) {
        return nullptr;
    }

    if (it_unknown == indexConf->GetInvertedIndexType()) {
        return nullptr;
    }

    return std::make_shared<MultiAdaptiveBitmapIndexWriter>(trigger, indexConf, _adaptiveDictFolder, _targetSegments);
}

} // namespace indexlib::index
