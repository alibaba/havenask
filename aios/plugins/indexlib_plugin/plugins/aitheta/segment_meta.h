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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_SEGMENT_META_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_SEGMENT_META_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"

namespace indexlib {
namespace aitheta_plugin {

typedef std::map<int64_t, size_t> OfflineIndexAttrMap;

class SegmentMeta : public autil::legacy::Jsonizable {
 public:
    SegmentMeta(const SegmentType &t = SegmentType::kUnknown, bool onlineBuilt = false, int32_t dimension = -1)
        : mType(t), mIsBuiltOnline(onlineBuilt), mDimension(dimension), mDocNum(0ul), mCatNum(0ul) {}
    ~SegmentMeta() {}

 public:
    SegmentType GetType() const { return mType; }
    void SetType(const SegmentType &t) { mType = t; }
    int32_t GetDimension() const { return mDimension; }
    void SetDimension(uint32_t d) { mDimension = d; }
    size_t GetDocNum() const { return mDocNum; }
    void SetDocNum(uint32_t d) { mDocNum = d; }
    size_t GetCatNum() const { return mCatNum; }
    void SetCatNum(uint32_t d) { mCatNum = d; }
    bool IsBuiltOnline() const { return mIsBuiltOnline; }
    void IsBuiltOnline(bool b) { mIsBuiltOnline = b; }

 public:
    void Jsonize(JsonWrapper &json) override;
    bool Load(const indexlib::file_system::DirectoryPtr &dir);
    bool Dump(const indexlib::file_system::DirectoryPtr &dir);
    bool Merge(const SegmentMeta &others);

 private:
    SegmentType mType;
    bool mIsBuiltOnline;
    uint32_t mDimension;
    uint32_t mDocNum;
    uint32_t mCatNum;
    IE_LOG_DECLARE();
};

}
}

#endif  //  __INDEXLIB_PLUGIN_PLUGINS_AITHETA_SEGMENT_META_H
