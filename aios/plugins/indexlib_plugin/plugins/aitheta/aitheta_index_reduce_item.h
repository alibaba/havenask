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
#ifndef __INDEXLIB_AITHETA_INDEX_REDUCE_ITEM_H
#define __INDEXLIB_AITHETA_INDEX_REDUCE_ITEM_H

#include <deque>
#include <map>
#include <memory>
#include <tuple>

#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/indexlib.h"

#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/segment.h"

namespace indexlib {
namespace aitheta_plugin {

class AithetaIndexReduceItem : public indexlib::index::IndexReduceItem {
 public:
    AithetaIndexReduceItem(){};
    ~AithetaIndexReduceItem(){};

 public:
    bool LoadIndex(const file_system::DirectoryPtr &indexDir) override;
    bool UpdateDocId(const DocIdMap &docIdMap) override;
    SegmentPtr GetSegment() const { return mSegment; }

 private:
    SegmentPtr mSegment;

 private:
    IE_LOG_DECLARE();
    friend class AithetaIndexReducer;
    friend class AithetaIndexerTest;
};

DEFINE_SHARED_PTR(AithetaIndexReduceItem);

}
}

#endif  //__INDEXLIB_AITHETA_INDEX_REDUCE_ITEM_H
