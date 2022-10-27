#ifndef __INDEXLIB_TRUNCATE_PROFILE_CONFIG_H
#define __INDEXLIB_TRUNCATE_PROFILE_CONFIG_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/sort_param.h"

IE_NAMESPACE_BEGIN(config);
class TruncateProfileConfigImpl;
DEFINE_SHARED_PTR(TruncateProfileConfigImpl);

class TruncateProfileConfig : public autil::legacy::Jsonizable
{
public:
    TruncateProfileConfig();
    // for test
    TruncateProfileConfig(
	const std::string& profileName, const std::string& sortDesp);
    ~TruncateProfileConfig();

public:
    const std::string& GetTruncateProfileName() const;

    const SortParams& GetTruncateSortParams() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void AssertEqual(const TruncateProfileConfig& other) const;

    void Check() const;

private:
    TruncateProfileConfigImplPtr mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateProfileConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_PROFILE_CONFIG_H
