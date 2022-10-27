#ifndef __INDEXLIB_SORT_PARAM_H
#define __INDEXLIB_SORT_PARAM_H

#include <tr1/memory>
#include <autil/StringTokenizer.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/sort_pattern_transformer.h"

IE_NAMESPACE_BEGIN(config);

#define SORT_DESCRIPTION_SEPERATOR ";"
#define SORT_DESCRIPTION_DESCEND '-'
#define SORT_DESCRIPTION_ASCEND '+'


class SortParam : public autil::legacy::Jsonizable
{
public:
    SortParam() : mSortPattern(sp_desc) { }

    ~SortParam() { }

public:
    std::string GetSortField() const
    {
        return mSortField;
    }
    
    void SetSortField(const std::string& sortField)
    {
        mSortField = sortField;
    }

    SortPattern GetSortPattern() const
    {
        return mSortPattern;
    }

    void SetSortPattern(SortPattern sortPattern)
    {
        mSortPattern = sortPattern;
    }

    std::string GetSortPatternString() const
    {
        return SortPatternTransformer::ToString(mSortPattern);
    }

    void SetSortPatternString(const std::string& sortPatternString)
    {
        mSortPattern = SortPatternTransformer::FromString(sortPatternString);
    }

    bool IsDesc() const
    { 
        return mSortPattern != sp_asc; 
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
    {
        json.Jsonize("SortField", mSortField, mSortField);
        if (json.GetMode() == FROM_JSON)
        {
            std::string strSortPattern;
            json.Jsonize("SortType", strSortPattern, strSortPattern);
            mSortPattern = SortPatternTransformer::FromString(strSortPattern);
        }
        else
        {
            std::string strSortPattern = 
                SortPatternTransformer::ToString(mSortPattern);
            json.Jsonize("SortType", strSortPattern, strSortPattern);
        }
    }

    void fromSortDescription(const std::string sortDescription);

    std::string toSortDescription() const;

    void AssertEqual(const SortParam& other) const;

private:
    std::string mSortField;
    SortPattern mSortPattern;

private:
    IE_LOG_DECLARE();
};

typedef std::vector<SortParam> SortParams;

inline const std::string SortParamsToString(const SortParams& sortParams)
{
    if (sortParams.empty())
    {
        return "";
    }
    
    std::stringstream ss;
    size_t i = 0;
    for (i = 0; i < sortParams.size() - 1; ++i)
    {
        ss << sortParams[i].toSortDescription() << SORT_DESCRIPTION_SEPERATOR;
    }
    ss << sortParams[i].toSortDescription();
    return ss.str();
}

inline SortParams StringToSortParams(const std::string& sortDesp)
{
    SortParams params;
    autil::StringTokenizer st(sortDesp, SORT_DESCRIPTION_SEPERATOR,
                              autil::StringTokenizer::TOKEN_TRIM |
                              autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); it++)
    {
        SortParam sortParam;
        sortParam.fromSortDescription(*it);
        params.push_back(sortParam);
    }
    return params;
}


DEFINE_SHARED_PTR(SortParam);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SORT_PARAM_H
