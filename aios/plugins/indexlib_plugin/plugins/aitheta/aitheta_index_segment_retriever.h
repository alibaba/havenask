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
#ifndef __INDEXLIB_AITHETA_INDEX_SEGMENT_RETRIEVER_H
#define __INDEXLIB_AITHETA_INDEX_SEGMENT_RETRIEVER_H

#include <memory>
#include <unordered_map>
#include <vector>
#include <aitheta/index_framework.h>
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib_plugin/plugins/aitheta/index_segment_reader.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/segment.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_config.h"

namespace indexlib {
namespace aitheta_plugin {

class AithetaIndexSegmentRetriever : public indexlib::index::IndexSegmentRetriever {
 public:
    AithetaIndexSegmentRetriever(const util::KeyValueMap &parameters) : IndexSegmentRetriever(parameters) {}
    ~AithetaIndexSegmentRetriever() = default;

 public:
    size_t EstimateLoadMemoryUse(const indexlib::file_system::DirectoryPtr &indexDir) const override;
    bool DoOpen(const indexlib::file_system::DirectoryPtr &indexDir) override;
    MatchInfo Search(const std::string &query, autil::mem_pool::Pool *sessionPool) override;

 public:
    IndexSegmentReaderPtr GetSegmentReader() const { return mSgtReader; }

 private:
    IndexSegmentReaderPtr mSgtReader;

 private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AithetaIndexSegmentRetriever);
}
}
#endif  // __INDEXLIB_AITHETA_INDEX_SEGMENT_RETRIEVER_H
