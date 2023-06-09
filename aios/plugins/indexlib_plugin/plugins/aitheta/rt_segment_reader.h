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
#ifndef __INDEXLIB_AITHETA_RT_SEGMENT_READER_H
#define __INDEXLIB_AITHETA_RT_SEGMENT_READER_H

#include "indexlib_plugin/plugins/aitheta/index_segment_reader.h"
#include "indexlib_plugin/plugins/aitheta/rt_segment.h"

namespace indexlib {
namespace aitheta_plugin {

class RtSegmentReader : public IndexSegmentReader {
 public:
    RtSegmentReader(const util::KeyValueMap &keyVal, const RtSegmentPtr &segment)
        : IndexSegmentReader(keyVal, segment) {}
    virtual ~RtSegmentReader() = default;

 public:
    bool Init(const ContextHolderPtr &holder, docid_t sgtId,
              const indexlib::index::DeletionMapReaderPtr &delMapRr) override;

 public:
    void SetOfflineMaxIndexAttrs(const std::unordered_map<catid_t, IndexAttr> &catid2IndexAttr) {
        mCatid2OfflineMaxIndexAttr = catid2IndexAttr;
    }

 private:
    bool InitRtIndex(catid_t catid, const IndexAttr &indexAttr, const ContextHolderPtr &holder);

 private:
    std::unordered_map<catid_t, IndexAttr> mCatid2OfflineMaxIndexAttr;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RtSegmentReader);

}
}

#endif  //__INDEXLIB_AITHETA_IN_MEM_SEGMENT_READER_H
