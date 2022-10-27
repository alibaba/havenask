#include "indexlib/misc/exception.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/sort_param.h"

using namespace std;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SortParam);

void SortParam::fromSortDescription(const std::string sortDescription)
{
    if (sortDescription.size() < 1) {
        IE_CONFIG_ASSERT_PARAMETER_VALID(false,
                string("invalid sort description: ") + sortDescription);
    }

    if (sortDescription[0] == SORT_DESCRIPTION_DESCEND)
    {
        mSortField = sortDescription.substr(1);
        mSortPattern = sp_desc;
    }
    else if(sortDescription[0] == SORT_DESCRIPTION_ASCEND)
    {
        mSortField = sortDescription.substr(1);
        mSortPattern = sp_asc;
    }
    else
    {
        mSortField = sortDescription;
        mSortPattern = sp_desc;
    }
}

std::string SortParam::toSortDescription() const
{
    stringstream ss;
    ss << (IsDesc()?SORT_DESCRIPTION_DESCEND:SORT_DESCRIPTION_ASCEND);
    ss << mSortField;
    return ss.str();
}

void SortParam::AssertEqual(const SortParam& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mSortField, other.mSortField,
                           "mSortField doesn't match");
    IE_CONFIG_ASSERT_EQUAL(mSortPattern, other.mSortPattern,
                           "mSortPattern doesn't match");
}

IE_NAMESPACE_END(config);

