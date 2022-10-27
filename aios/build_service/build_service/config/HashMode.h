#ifndef ISEARCH_BS_HASHMODE_H
#define ISEARCH_BS_HASHMODE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class HashMode : public autil::legacy::Jsonizable
{
public:
    HashMode();
    virtual ~HashMode();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    virtual bool validate() const;
public:
    std::string _hashFunction;
    mutable std::vector<std::string> _hashFields;
    std::map<std::string, std::string> _hashParams;
    std::string _hashField;
private:
    BS_LOG_DECLARE();
};

class RegionHashMode : public HashMode
{
public:
    RegionHashMode();
    ~RegionHashMode();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const override;
public:
    std::string _regionName;
public:
    static const std::string REGION_NAME;
private:
    BS_LOG_DECLARE();
};

typedef std::vector<RegionHashMode> RegionHashModes;

}
}

#endif //ISEARCH_BS_HASHMODE_H
