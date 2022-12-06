#ifndef __INDEXLIB_AITHETA_RAW_SEGMENT_READER_H
#define __INDEXLIB_AITHETA_RAW_SEGMENT_READER_H

#include <tuple>
#include <unordered_map>

#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"
#include "aitheta_indexer/plugins/aitheta/segment_reader.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class RawSegmentReader;
DEFINE_SHARED_PTR(RawSegmentReader);

class RawSegmentReader : public SegmentReader {
 public:
    RawSegmentReader(const SegmentMeta &meta) : SegmentReader(meta) {}
    ~RawSegmentReader() {}

 public:
    bool Open(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) override;

    bool UpdatePkey2DocId(docid_t baseDocId, const std::vector<SegmentReaderPtr> &segmentReaderVec,
                          const std::vector<docid_t> &segmentBaseDocIdVec) override {
        return true;
    }

    std::vector<MatchInfo> Search(const std::vector<QueryInfo> &queryInfos, autil::mem_pool::Pool *sessionPool,
                                  int32_t topK, bool needScoreFilter = false, float score = 0.0) {
        return {};
    }

    const RawSegmentPtr &getSegment() const { return mSegment; }

 private:
    RawSegmentPtr mSegment;
    std::unordered_map<int64_t, docid_t> mPkey2DocId;
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_RAW_SEGMENT_READER_H
