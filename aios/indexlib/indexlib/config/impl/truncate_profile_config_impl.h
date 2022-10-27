#ifndef __INDEXLIB_TRUNCATE_PROFILE_CONFIG_IMPL_H
#define __INDEXLIB_TRUNCATE_PROFILE_CONFIG_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/misc/exception.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/sort_param.h"
#include "indexlib/config/truncate_profile_config.h"

IE_NAMESPACE_BEGIN(config);

class TruncateProfileConfigImpl : public autil::legacy::Jsonizable
{
public:
    TruncateProfileConfigImpl();
    
    // for test
    TruncateProfileConfigImpl(
	const std::string& profileName, const std::string& sortDesp);
    
    ~TruncateProfileConfigImpl();

public:
    const std::string& GetTruncateProfileName() const
    { return mTruncateProfileName; }

    const SortParams& GetTruncateSortParams() const
    { return mSortParams; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void AssertEqual(const TruncateProfileConfigImpl& other) const;

    void Check() const;

private:
    std::string mTruncateProfileName;
    std::string mOriSortDescriptions;
    SortParams mSortParams;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateProfileConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_PROFILE_CONFIG_IMPL_H
