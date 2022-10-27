#ifndef ISEARCH_RESOURCETYPESET_H
#define ISEARCH_RESOURCETYPESET_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(config);

typedef std::string ResourceType;

class ResourceTypeSet
{
public:
    ResourceTypeSet();
    ResourceTypeSet(const std::string &str) {
        fromString(str);
    }

    ~ResourceTypeSet();


    void fromString(const std::string &str);
    
    std::string toString() const;

    bool has(const ResourceType &resourceType) const {
        if (resourceType.empty() && empty()) {
            return true;
        }
        return _types.find(resourceType) != _types.end();
    }

    const std::set<ResourceType>& getTypes() const {
        return _types;
    }
    
    bool empty() const;
    
    ResourceTypeSet& operator = (const std::string &str) {
        fromString(str);
        return *this;
    }

private:
    std::set<ResourceType> _types;

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ResourceTypeSet);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_RESOURCETYPESET_H
