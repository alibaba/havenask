#include "aitheta_indexer/plugins/aitheta/segment.h"

#include <autil/TimeUtility.h>
#include <autil/legacy/exception.h>
#include <autil/legacy/jsonizable.h>
#include <indexlib/misc/exception.h>
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace std;
using namespace autil::legacy;
using namespace autil;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_LOG_SETUP(aitheta_plugin, SegmentMeta);
IE_LOG_SETUP(aitheta_plugin, Segment);

const std::string UNKNOWN = "unknown";
const std::string INDEX = "index";
const std::string RAW = "raw";
const std::string PARALLEL = "parallel";
const std::string FILE_SIZE_OLD_VERSION = "size";

void SegmentMeta::Jsonize(Jsonizable::JsonWrapper &json) {
    string typeStr;
    if (SegmentType::kIndex == mType) {
        typeStr = INDEX;
    } else if (SegmentType::kRaw == mType) {
        typeStr = RAW;
    } else if (SegmentType::kMultiIndex == mType) {
        typeStr = PARALLEL;
    } else {
        typeStr = UNKNOWN;
    }

    // TODO(richard.sy): ugly code for compatibility
    string validDocNumStr = StringUtil::toString(mValidDocNum);
    string totalDocNumStr = StringUtil::toString(mTotalDocNum);
    string categoryNumStr = StringUtil::toString(mCategoryNum);
    string maxCategoryDocNumStr = StringUtil::toString(mMaxCategoryDocNum);
    string fileSizeStr = StringUtil::toString(mFileSize);
    string dimensionStr = StringUtil::toString(mDimension);

    json.Jsonize("type", typeStr, "");
    json.Jsonize("valid_doc_num", validDocNumStr, "");
    json.Jsonize("total_doc_num", totalDocNumStr, "");
    json.Jsonize("category_num", categoryNumStr, "");
    json.Jsonize("max_category_doc_num", maxCategoryDocNumStr, "");
    json.Jsonize("file_size", fileSizeStr, "");
    if (fileSizeStr.empty()) {
        json.Jsonize(FILE_SIZE_OLD_VERSION, fileSizeStr);
    }
    json.Jsonize("dimension", dimensionStr, "");
    if (json.GetMode() == Jsonizable::Mode::FROM_JSON) {
        if (typeStr == INDEX) {
            mType = SegmentType::kIndex;
        } else if (typeStr == RAW) {
            mType = SegmentType::kRaw;
        } else if (typeStr == PARALLEL) {
            mType = SegmentType::kMultiIndex;
        } else {
            mType = SegmentType::kUnknown;
        }

        StringUtil::fromString(validDocNumStr, mValidDocNum);
        StringUtil::fromString(totalDocNumStr, mTotalDocNum);
        StringUtil::fromString(categoryNumStr, mCategoryNum);
        StringUtil::fromString(maxCategoryDocNumStr, mMaxCategoryDocNum);
        StringUtil::fromString(fileSizeStr, mFileSize);
        StringUtil::fromString(dimensionStr, mDimension);
    }

    json.Jsonize("is_optimize_merge_index", mIsOptimizeMergeIndex, false);
}

bool SegmentMeta::Load(const DirectoryPtr &dir) {
    auto reader = IndexlibIoWrapper::CreateReader(dir, SEGMENT_META_FILE_NAME, FSOT_MMAP);
    if (!reader) {
        return false;
    }

    string metaStr((char *)reader->GetBaseAddress(), reader->GetLength());
    reader->Close();

    try {
        FromJsonString(*this, metaStr);
    } catch (const ExceptionBase &e) {
        IE_LOG(ERROR, "failed to jsonize meta[%s] in path[%s]", metaStr.c_str(), dir->GetPath().c_str());
        return false;
    }
    return true;
}

bool SegmentMeta::Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) {
    auto writer = IndexlibIoWrapper::CreateWriter(dir, SEGMENT_META_FILE_NAME);
    if (nullptr == writer) {
        return false;
    }
    string metaStr;
    try {
        metaStr = ToJsonString(*this);
    } catch (const ExceptionBase &e) {
        writer->Close();
        IE_LOG(ERROR, "failed to write meta in path[%s]", writer->GetPath().c_str());
        return false;
    }
    writer->Write(metaStr.data(), metaStr.size());
    writer->Close();
    return true;
}

void SegmentMeta::Merge(const SegmentMeta &other) {
    mValidDocNum += other.mValidDocNum;
    mTotalDocNum += other.mTotalDocNum;
    mCategoryNum += other.mCategoryNum;
    mFileSize += other.mFileSize;
    if (other.mMaxCategoryDocNum > mMaxCategoryDocNum) {
        mMaxCategoryDocNum = other.mMaxCategoryDocNum;
    }
}

IE_NAMESPACE_END(aitheta_plugin);
