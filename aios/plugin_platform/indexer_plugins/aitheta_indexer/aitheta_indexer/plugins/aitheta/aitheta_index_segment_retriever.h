#ifndef __INDEXLIB_AITHETA_INDEX_SEGMENT_RETRIEVER_H
#define __INDEXLIB_AITHETA_INDEX_SEGMENT_RETRIEVER_H

#include <sys/sysinfo.h>

#include <memory>
#include <unordered_map>
#include <vector>

#include <aitheta/index_framework.h>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/pool_allocator.h>
#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_retriever.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h>
#include <indexlib/index/normal/deletionmap/deletion_map_reader.h>
#include <indexlib/indexlib.h>
#include <indexlib/storage/file_system_wrapper.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/segment_reader.h"
#include "aitheta_indexer/plugins/aitheta/segment.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_config.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaIndexSegmentRetriever : public IE_NAMESPACE(index)::IndexSegmentRetriever {
    friend class AithetaIndexRetriever;

 public:
    AithetaIndexSegmentRetriever(const util::KeyValueMap &parameters);

 public:
    size_t EstimateLoadMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &indexDir) const override;
    bool Open(const file_system::DirectoryPtr &indexDir) override;
    MatchInfo Search(const std::string &query, autil::mem_pool::Pool *sessionPool) override { return MatchInfo(); }
    SegmentReaderPtr GetSegmentReader() const { return mSegmentReader; }

 private:
    bool InitKnnConfig(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) {
        return mKnnConfig.Load(dir);
    }

 private:
    SegmentMeta mSegmentMeta;
    SegmentReaderPtr mSegmentReader;
    KnnConfig mKnnConfig;

 private:
    aitheta::IndexPluginBroker mPluginBroker;

 private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(AithetaIndexSegmentRetriever);
IE_NAMESPACE_END(aitheta_plugin);
#endif  // __INDEXLIB_AITHETA_INDEX_SEGMENT_RETRIEVER_H
