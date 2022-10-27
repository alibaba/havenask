#ifndef __INDEXLIB_TRUNCATE_PROFILE_SCHEMA_H
#define __INDEXLIB_TRUNCATE_PROFILE_SCHEMA_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/truncate_profile_config.h"

IE_NAMESPACE_BEGIN(config);
class TruncateProfileSchemaImpl;
DEFINE_SHARED_PTR(TruncateProfileSchemaImpl);

class TruncateProfileSchema : public autil::legacy::Jsonizable
{
public:
    typedef std::map<std::string, TruncateProfileConfigPtr> TruncateProfileConfigMap;
    typedef TruncateProfileConfigMap::const_iterator Iterator;

public:
    TruncateProfileSchema();
    ~TruncateProfileSchema() {}

public:
    TruncateProfileConfigPtr GetTruncateProfileConfig(
        const std::string& truncateProfileName) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const TruncateProfileSchema& other) const;

    Iterator Begin() const;
    Iterator End() const;
    size_t Size() const;

    const TruncateProfileConfigMap& GetTruncateProfileConfigMap();

    // for test
    void AddTruncateProfileConfig(
	const TruncateProfileConfigPtr& truncateProfileConfig);
    
private:
    TruncateProfileSchemaImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(TruncateProfileSchema);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_PROFILE_SCHEMA_H
