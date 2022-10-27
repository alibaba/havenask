#ifndef __INDEXLIB_SORT_DESCRIPTION_H
#define __INDEXLIB_SORT_DESCRIPTION_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

struct SortDescription : public autil::legacy::Jsonizable
{
public:
    SortDescription() 
        : sortPattern(sp_desc)
    {}
    SortDescription(const std::string &sortFieldName_, SortPattern sortPattern_)
        : sortFieldName(sortFieldName_)
        , sortPattern(sortPattern_)
    {}

public:
    void Jsonize(JsonWrapper &json);
public:
    bool operator==(const SortDescription &other) const;
    bool operator!=(const SortDescription &other) const 
    {
        return !(*this == other);
    }
    std::string ToString() const;
public:
    std::string sortFieldName;
    SortPattern sortPattern;
};

typedef std::vector<common::SortDescription> SortDescriptions;
IE_NAMESPACE_END(common);

#endif //__INDEXLIB_PARTITION_META_H
