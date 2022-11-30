#ifndef __INDEXLIB_AITHETA_INDEX_SEGMENT_BUILDER_H
#define __INDEXLIB_AITHETA_INDEX_SEGMENT_BUILDER_H

#include <unordered_map>

#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_config.h"
#include "aitheta_indexer/plugins/aitheta/util/reduce_task/reduce_task_creater.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class IndexSegmentBuilder {
 public:
    IndexSegmentBuilder(const std::vector<RawSegmentPtr> &rawSegVector, const CommonParam &cp,
                        const util::KeyValueMap &kv, const KnnConfig &knnConfig)
        : mRawSegVector(rawSegVector), mComParam(cp), mKvParam(kv), mKnnConfig(knnConfig) {}
    ~IndexSegmentBuilder() {}

 public:
    bool BuildAndDump(const IE_NAMESPACE(file_system)::DirectoryPtr &directory, const CustomReduceTaskPtr &task,
                      const std::vector<RawSegmentPtr> &incRawSegmentVec);

 private:
    bool Build(FlexibleFloatIndexHolderPtr &data, IE_NAMESPACE(file_system)::FileWriterPtr &writer,
               MipsParams &mipsParams);

    static const std::string CreateLocalDumpPath();

 private:
    std::vector<RawSegmentPtr> mRawSegVector;
    CommonParam mComParam;
    util::KeyValueMap mKvParam;
    KnnConfig mKnnConfig;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexSegmentBuilder);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_INDEX_SEGMENT_BUILDER_H
