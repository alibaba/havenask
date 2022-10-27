#ifndef ISEARCH_SEARCH_METAINFO_H_
#define ISEARCH_SEARCH_METAINFO_H_
#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <ha3/rank/TermMetaInfo.h>
#include <vector>

BEGIN_HA3_NAMESPACE(rank);


class MetaInfo 
{
public:
    MetaInfo(uint32_t reserveSize = DEFAULT_RESERVE_SIZE) {
        _termMetaInfos.reserve(reserveSize);
    }
    ~MetaInfo() {
    }
public:
    uint32_t size() const {
        return (uint32_t)(_termMetaInfos.size());
    }

    const TermMetaInfo& operator[](uint32_t idx) const {
        return _termMetaInfos[idx];
    }
    
    void pushBack(const TermMetaInfo &termMetaInfo) {
        _termMetaInfos.push_back(termMetaInfo);
    }
private:
    std::vector<TermMetaInfo> _termMetaInfos;
    static const uint32_t DEFAULT_RESERVE_SIZE = 4;
private:
    HA3_LOG_DECLARE();    
};


END_HA3_NAMESPACE(rank);
#endif
