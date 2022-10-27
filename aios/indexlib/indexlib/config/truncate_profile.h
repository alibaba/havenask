#ifndef __INDEXLIB_TRUNCATE_PROFILE_H
#define __INDEXLIB_TRUNCATE_PROFILE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/config_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/sort_param.h"
#include "indexlib/config/truncate_profile_config.h"  

IE_NAMESPACE_BEGIN(config);

class TruncateProfile : public autil::legacy::Jsonizable
{
public:
    TruncateProfile();
    TruncateProfile(TruncateProfileConfig& truncateProfileConfig);
    ~TruncateProfile();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    void Check() const;

    void AppendIndexName(const std::string& indexName);

    bool operator==(const TruncateProfile& other) const;

    bool HasSort() const { return !mSortParams.empty(); };

    size_t GetSortDimenNum() const { return mSortParams.size(); }

public:
    typedef std::vector<std::string> StringVec;

    std::string mTruncateProfileName;
    SortParams mSortParams;
    std::string mTruncateStrategyName;
    StringVec mIndexNames;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateProfile);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_PROFILE_H
