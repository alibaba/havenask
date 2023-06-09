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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_AITHETA_RT_SEGMENT_RETRIEVER_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_AITHETA_RT_SEGMENT_RETRIEVER_H

#include <aitheta/index_framework.h>
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/inverted_index/customized_index/in_mem_segment_retriever.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/indexlib.h"

#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_config.h"
#include "indexlib_plugin/plugins/aitheta/rt_segment_reader.h"

namespace indexlib {
namespace aitheta_plugin {

class AithetaRtSegmentRetriever : public indexlib::index::InMemSegmentRetriever {
 public:
    AithetaRtSegmentRetriever(const util::KeyValueMap& parameters, const RtSegmentPtr& segment)
        : indexlib::index::InMemSegmentRetriever(parameters) {
        mSgtReader.reset(new RtSegmentReader(parameters, segment));
    }
    ~AithetaRtSegmentRetriever() {}

 public:
    RtSegmentReaderPtr GetSegmentReader() const { return mSgtReader; }
    MatchInfo Search(const std::string& query, autil::mem_pool::Pool* sessionPool) override { return MatchInfo(); }

 private:
    RtSegmentReaderPtr mSgtReader;

 private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AithetaRtSegmentRetriever);
}
}
#endif  // __INDEXLIB_PLUGIN_PLUGINS_AITHETA_AITHETA_RT_SEGMENT_RETRIEVER_H
