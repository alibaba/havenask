#ifndef __INDEXLIB_PRIMARY_KEY_H
#define __INDEXLIB_PRIMARY_KEY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKey
{
public:
    PrimaryKey() {}
    virtual ~PrimaryKey() {}

public:
    virtual void Init(const config::IndexConfigPtr& indexConfig) = 0;
    virtual void Init(const config::IndexConfigPtr& indexConfig, 
                      const SegmentDirectoryBasePtr& segDir) = 0;
    virtual void AddSegment(const std::string& segPath, segmentid_t segId,
                            docid_t baseDocId) = 0;
    virtual void AddKey(const std::string& key, docid_t docId) = 0;
    virtual docid_t Lookup(const std::string& key) const = 0;
    virtual void Combine(docid_t baseDocId, 
                         const ReclaimMapPtr& reclaimMap) = 0;  

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimaryKey);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_H
