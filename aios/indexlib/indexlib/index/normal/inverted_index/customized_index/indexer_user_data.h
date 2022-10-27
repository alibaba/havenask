#ifndef __INDEXLIB_INDEXER_USER_DATA_H
#define __INDEXLIB_INDEXER_USER_DATA_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/index_meta/segment_info.h"

namespace heavenask { namespace indexlib {

struct IndexerSegmentData
{
public:
    file_system::DirectoryPtr indexDir;
    index_base::SegmentInfo segInfo;
    segmentid_t segId;
    bool isBuildingSegment;
};
///////////////////////////////////////////////////////////

class IndexerUserData
{
public:
    IndexerUserData();
    virtual ~IndexerUserData();
    
public:
    virtual bool init(const std::vector<IndexerSegmentData>& segDatas);

    virtual size_t estimateInitMemUse(
            const std::vector<IndexerSegmentData>& segDatas) const;
    
    void setData(const std::string& key, const std::string& value);
    bool getData(const std::string& key, std::string& value) const;

private:
    std::map<std::string, std::string> mDataMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexerUserData);

}
}

#endif //__INDEXLIB_INDEXER_USER_DATA_H
