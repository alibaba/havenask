#ifndef ISEARCH_RANKSORTDESCRIPTION_H
#define ISEARCH_RANKSORTDESCRIPTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/SortDescription.h>

BEGIN_HA3_NAMESPACE(common);

class RankSortDescription
{
public:
    RankSortDescription();
    ~RankSortDescription();
private:
    RankSortDescription(const RankSortDescription &);
    RankSortDescription& operator=(const RankSortDescription &);
public:
    void setSortDescs(const std::vector<SortDescription*> &sortDesc);
    size_t getSortDescCount() const {
        return _sortDescs.size();
    }
    const SortDescription* getSortDescription(size_t idx) const {
        assert(idx < _sortDescs.size());
        return _sortDescs[idx];
    }

    const std::vector<SortDescription*> &getSortDescriptions() const {
        return _sortDescs;
    }

    void setPercent(const std::string &percentStr);
    float getPercent() const {
        return _percent;
    }
    std::string toString() const;
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
private:
    std::vector<SortDescription*> _sortDescs;
    float _percent;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RankSortDescription);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_RANKSORTDESCRIPTION_H
