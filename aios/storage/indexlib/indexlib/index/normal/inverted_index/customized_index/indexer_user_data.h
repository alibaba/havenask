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
#ifndef __INDEXLIB_INDEXER_USER_DATA_H
#define __INDEXLIB_INDEXER_USER_DATA_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"

namespace indexlib {

struct IndexerSegmentData {
public:
    file_system::DirectoryPtr indexDir;
    index_base::SegmentInfo segInfo;
    segmentid_t segId;
    docid_t baseDocId;
    bool isBuildingSegment;
};
///////////////////////////////////////////////////////////

class IndexerUserData
{
public:
    IndexerUserData();
    virtual ~IndexerUserData();

public:
    virtual bool init(const std::vector<IndexerSegmentData>& segDatas);

    virtual size_t estimateInitMemUse(const std::vector<IndexerSegmentData>& segDatas) const;

    void setData(const std::string& key, const std::string& value);
    bool getData(const std::string& key, std::string& value) const;

private:
    std::map<std::string, std::string> mDataMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexerUserData);

} // namespace indexlib

#endif //__INDEXLIB_INDEXER_USER_DATA_H
