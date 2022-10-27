#ifndef __INDEXLIB_TRUNCATE_PROFILE_SCHEMA_IMPL_H
#define __INDEXLIB_TRUNCATE_PROFILE_SCHEMA_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/config/truncate_profile_config.h"

IE_NAMESPACE_BEGIN(config);

class TruncateProfileSchemaImpl : public autil::legacy::Jsonizable
{
public:
    typedef std::map<std::string, TruncateProfileConfigPtr> TruncateProfileConfigMap;
    typedef TruncateProfileConfigMap::const_iterator Iterator;

public:
    TruncateProfileSchemaImpl();
    ~TruncateProfileSchemaImpl();

public:
    TruncateProfileConfigPtr GetTruncateProfileConfig(
            const std::string& truncateProfileName) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const TruncateProfileSchemaImpl& other) const;

    inline Iterator Begin() const { return mTruncateProfileConfigs.begin(); }
    inline Iterator End() const { return mTruncateProfileConfigs.end(); }
    size_t Size() const { return mTruncateProfileConfigs.size(); }

    const TruncateProfileConfigMap& GetTruncateProfileConfigMap()
    {
        return mTruncateProfileConfigs;
    }

    // for test
    void AddTruncateProfileConfig(
	const TruncateProfileConfigPtr& truncateProfileConfig);
    
private:
    TruncateProfileConfigMap mTruncateProfileConfigs;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(TruncateProfileSchemaImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_PROFILE_SCHEMA_IMPL_H
