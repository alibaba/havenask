#ifndef __INDEXLIB_AITHETA_SEGMENT_READER_H
#define __INDEXLIB_AITHETA_SEGMENT_READER_H

#include <map>
#include <string>
#include <vector>

#include <autil/ThreadPool.h>
#include <indexlib/common_define.h>
#include <indexlib/file_system/file_reader.h>
#include <indexlib/file_system/file_writer.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer_define.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/segment.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class SegmentReader;
DEFINE_SHARED_PTR(SegmentReader);

class SegmentReader {
 public:
    SegmentReader(const SegmentMeta &meta) : mSegmentMeta(meta) {}
    virtual ~SegmentReader() {}

 private:
    SegmentReader(const Segment &) = delete;
    Segment &operator=(const Segment &) = delete;

 public:
    virtual bool Open(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) = 0;

    virtual bool UpdatePkey2DocId(docid_t baseDocId, const std::vector<SegmentReaderPtr> &segmentReaderVec,
                                  const std::vector<docid_t> &segmentBaseDocIdVec) = 0;

    virtual std::vector<MatchInfo> Search(const std::vector<QueryInfo> &queryInfos, autil::mem_pool::Pool *sessionPool,
                                          int32_t topK, bool needScoreFilter = false, float score = 0.0) = 0;

 public:
    const SegmentMeta &GetSegmentMeta() const { return mSegmentMeta; }
    SegmentType GetSegmentType() const { return mSegmentMeta.getType(); }

 protected:
    SegmentMeta mSegmentMeta;
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(aitheta_plugin);

#endif  //  __INDEXLIB_AITHETA_SEGMENT_READER_H
