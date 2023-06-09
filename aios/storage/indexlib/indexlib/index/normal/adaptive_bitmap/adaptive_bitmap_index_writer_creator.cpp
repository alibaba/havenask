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
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer_creator.h"

#include "indexlib/config/index_config.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/adaptive_bitmap/multi_adaptive_bitmap_index_writer.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::config;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, AdaptiveBitmapIndexWriterCreator);

AdaptiveBitmapIndexWriterCreator::AdaptiveBitmapIndexWriterCreator(
    const file_system::ArchiveFolderPtr& adaptiveDictFolder, const ReclaimMapPtr& reclaimMap,
    const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
    : mTriggerCreator(reclaimMap)
{
    mAdaptiveDictFolder = adaptiveDictFolder;
    mOutputSegmentMergeInfos = outputSegmentMergeInfos;
}

AdaptiveBitmapIndexWriterCreator::~AdaptiveBitmapIndexWriterCreator() {}

std::shared_ptr<AdaptiveBitmapTrigger>
AdaptiveBitmapIndexWriterCreator::CreateAdaptiveBitmapTrigger(const IndexConfigPtr& indexConf)
{
    return mTriggerCreator.Create(indexConf);
}

MultiAdaptiveBitmapIndexWriterPtr AdaptiveBitmapIndexWriterCreator::Create(const config::IndexConfigPtr& indexConf,
                                                                           const config::MergeIOConfig& ioConfig)
{
    if (indexConf->GetShardingType() == IndexConfig::IST_NEED_SHARDING) {
        return MultiAdaptiveBitmapIndexWriterPtr();
    }

    std::shared_ptr<AdaptiveBitmapTrigger> trigger = CreateAdaptiveBitmapTrigger(indexConf);
    if (!trigger) {
        return MultiAdaptiveBitmapIndexWriterPtr();
    }

    MultiAdaptiveBitmapIndexWriterPtr writer;
    if (it_unknown == indexConf->GetInvertedIndexType()) {
        return MultiAdaptiveBitmapIndexWriterPtr();
    }

    // if (!FslibWrapper::IsExist(mAdaptiveDictDir).GetOrThrow())
    // {
    //     PrepareAdaptiveDictDir();
    // }
    writer.reset(new MultiAdaptiveBitmapIndexWriter(trigger, indexConf, mAdaptiveDictFolder, mOutputSegmentMergeInfos));
    return writer;
}
}}} // namespace indexlib::index::legacy
