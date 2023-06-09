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
#include "indexlib_plugin/plugins/aitheta/segment_meta.h"
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace indexlib::file_system;

namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, SegmentMeta);

const std::string UNKNOWN = "unknown";
const std::string RAW = "raw";
const std::string INDEX = "index";
const std::string PARALLEL = "parallel";
const std::string RTINDEX = "rtindex";

bool SegmentMeta::Load(const indexlib::file_system::DirectoryPtr &dir) {
    auto reader = IndexlibIoWrapper::CreateReader(dir, SEGMENT_META_FILE, FSOT_MMAP);
    if (nullptr == reader) {
        return false;
    }

    string meta((char *)reader->GetBaseAddress(), reader->GetLength());
    reader->Close().GetOrThrow();

    try {
        FromJsonString(*this, meta);
    } catch (const ExceptionBase &e) {
        IE_LOG(ERROR, "failed to jsonize meta[%s] in path[%s]", meta.c_str(), dir->DebugString().c_str());
        return false;
    }
    IE_LOG(INFO, "load segment meta in path[%s]", dir->DebugString().c_str());
    return true;
}

bool SegmentMeta::Dump(const indexlib::file_system::DirectoryPtr &dir) {
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    dir->RemoveFile(SEGMENT_META_FILE, removeOption);
    auto writer = IndexlibIoWrapper::CreateWriter(dir, SEGMENT_META_FILE);
    if (nullptr == writer) {
        return false;
    }

    string meta;
    try {
        meta = ToJsonString(*this);
    } catch (const ExceptionBase &e) {
        writer->Close().GetOrThrow();
        IE_LOG(ERROR, "failed to write meta in path[%s]", writer->DebugString().c_str());
        return false;
    }
    writer->Write(meta.data(), meta.size()).GetOrThrow();
    writer->Close().GetOrThrow();
    IE_LOG(INFO, "dump segment meta in path[%s]", dir->DebugString().c_str());
    return true;
}

void SegmentMeta::Jsonize(JsonWrapper &json) {
    json.Jsonize("doc_num", mDocNum, 0u);
    json.Jsonize("cat_num", mCatNum, 0u);
    json.Jsonize("dimension", mDimension, 0u);
    json.Jsonize("online_built", mIsBuiltOnline, false);
    json.Jsonize("type", mType, SegmentType::kUnknown);
    if (json.GetMode() == Jsonizable::Mode::TO_JSON) {
        string type;
        switch (mType) {
            case SegmentType::kRaw:
                type = RAW;
                break;
            case SegmentType::kIndex:
                type = INDEX;
                break;
            case SegmentType::kPMIndex:
                type = PARALLEL;
                break;
            case SegmentType::kRtIndex:
                type = RTINDEX;
                break;
            default:
                type = UNKNOWN;
        }
        json.Jsonize("type_str", type);
    }
}

bool SegmentMeta::Merge(const SegmentMeta &other) {
    if (mDimension != other.mDimension) {
        IE_LOG(ERROR, "dimension does not match, this[%d], other[%d]", mDimension, other.mDimension);
        return false;
    }
    if (mIsBuiltOnline != other.mIsBuiltOnline) {
        IE_LOG(ERROR, "different index type, this[%d], other[%d]", mIsBuiltOnline, other.mIsBuiltOnline);
        return false;
    }
    mDocNum += other.mDocNum;
    mCatNum += other.mCatNum;
    IE_LOG(INFO, "type: this[%s], other[%s]", Type2Str(mType).c_str(), Type2Str(other.mType).c_str());
    return true;
}

}
}
