#ifndef __INDEXLIB_CUSTOMIZED_INDEX_SEGMENT_READER_H
#define __INDEXLIB_CUSTOMIZED_INDEX_SEGMENT_READER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"
#include "indexlib/common/term.h"
#include "indexlib/plugin/plugin_manager.h"

IE_NAMESPACE_BEGIN(index);

class CustomizedIndexSegmentReader : public IndexSegmentReader
{
public:
    CustomizedIndexSegmentReader();
    ~CustomizedIndexSegmentReader();
public:
    bool Init(const config::IndexConfigPtr& indexConfig,
              const IndexSegmentRetrieverPtr& retriever);

    const IndexSegmentRetrieverPtr& GetSegmentRetriever() const { return mRetriever; }

    bool GetSegmentPosting(dictkey_t key, docid_t baseDocId,
                           index::SegmentPosting &segPosting,
                           autil::mem_pool::Pool* sessionPool) const override;
private:
    IndexSegmentRetrieverPtr mRetriever;
    config::IndexConfigPtr mIndexConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedIndexSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_CUSTOMIZED_INDEX_SEGMENT_READER_H
