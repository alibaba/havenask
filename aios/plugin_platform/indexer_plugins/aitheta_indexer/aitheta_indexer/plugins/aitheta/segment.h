#ifndef __INDEXLIB_AITHETA_SEGMENT_H
#define __INDEXLIB_AITHETA_SEGMENT_H

#include <map>
#include <string>
#include <vector>

#include <indexlib/common_define.h>
#include <indexlib/file_system/file_reader.h>
#include <indexlib/file_system/file_writer.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer_define.h>
#include <indexlib/indexlib.h>
#include "aitheta_indexer/plugins/aitheta/common_define.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

struct SegmentMeta : public autil::legacy::Jsonizable {
 public:
    SegmentMeta(SegmentType type = SegmentType::kUnknown, size_t validDocNum = 0u, size_t totalDocNum = 0u)
        : mType(type),
          mIsOptimizeMergeIndex(false),
          mValidDocNum(validDocNum),
          mTotalDocNum(totalDocNum),
          mCategoryNum(0u),
          mMaxCategoryDocNum(0u),
          mFileSize(0u) {}
    ~SegmentMeta() {}

 public:
    SegmentType getType() const { return mType; }
    void setType(SegmentType type) { mType = type; }
    bool isOptimizeMergeIndex() const { return mIsOptimizeMergeIndex; }
    void isOptimizeMergeIndex(bool isOpt) { mIsOptimizeMergeIndex = isOpt; }

    size_t getValidDocNum() const { return mValidDocNum; }
    void setValidDocNum(size_t v) { mValidDocNum = v; }
    size_t getTotalDocNum() const { return mTotalDocNum; }
    void setTotalDocNum(size_t t) { mTotalDocNum = t; }
    size_t getCategoryNum() const { return mCategoryNum; }
    void setCategoryNum(size_t n) { mCategoryNum = n; }
    size_t getMaxCategoryDocNum() const { return mMaxCategoryDocNum; }
    void setMaxCategoryDocNum(size_t n) { mMaxCategoryDocNum = n; }
    size_t getFileSize() const { return mFileSize; }
    void setFileSize(size_t s) { mFileSize = s; }
    int32_t getDimension() const { return mDimension; }
    void setDimension(int32_t dim) { mDimension = dim; }

 public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool Empty() const { return SegmentType::kUnknown == mType; }
    bool Load(const IE_NAMESPACE(file_system)::DirectoryPtr &dir);
    bool Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &dir);
    void Merge(const SegmentMeta &other);

 private:
    SegmentType mType;
    bool mIsOptimizeMergeIndex;
    size_t mValidDocNum;
    size_t mTotalDocNum;
    size_t mCategoryNum;
    size_t mMaxCategoryDocNum;
    size_t mFileSize;
    size_t mDimension;

    IE_LOG_DECLARE();
};

class Segment {
 public:
    Segment(const SegmentMeta &meta) : mMeta(meta){};
    virtual ~Segment() {}

 private:
    Segment(const Segment &) = delete;
    Segment &operator=(const Segment &) = delete;

 public:
    SegmentType GetSegmentType() const { return mMeta.getType(); }

 public:
    virtual bool PrepareReduceTaskInput(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, ReduceTaskInput &input) = 0;
    virtual bool Load(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) = 0;
    virtual bool UpdateDocId(const DocIdMap &docIdMap) = 0;
    virtual bool Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) = 0;
    virtual bool EstimateLoad4ReduceMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                                              size_t &size) const = 0;
    virtual bool EstimateLoad4RetrieveMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                                                size_t &size) const = 0;

 protected:
    SegmentMeta mMeta;
    util::KeyValueMap mKvParam;
    IE_LOG_DECLARE();
    friend class AithetaIndexerTest;
};
DEFINE_SHARED_PTR(Segment);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //  INDEXLIB_PLUGIN_PLUGINS_AITHETA_SEGMENT_H_
