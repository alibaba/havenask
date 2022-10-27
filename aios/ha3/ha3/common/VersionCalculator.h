#ifndef ISEARCH_VERSIONCALCULATOR_H
#define ISEARCH_VERSIONCALCULATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(common);

class VersionCalculator
{
public:
    VersionCalculator();
    ~VersionCalculator();
private:
    VersionCalculator(const VersionCalculator &);
    VersionCalculator& operator=(const VersionCalculator &);
public:
    static uint32_t calcVersion(const std::string &workerConfigVersion,
                                const std::string &configDataVersion,
                                const std::string &remoteConfigPath,
                                const std::string &fullVersionStr);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(VersionCalculator);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_VERSIONCALCULATOR_H
