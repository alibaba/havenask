#ifndef ISEARCH_QRSSEARCHCONFIG_H
#define ISEARCH_QRSSEARCHCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsChainManager.h>

BEGIN_HA3_NAMESPACE(service);

class QrsSearchConfig
{
public:
    QrsSearchConfig();
    ~QrsSearchConfig();
private:
    QrsSearchConfig(const QrsSearchConfig &);
    QrsSearchConfig& operator=(const QrsSearchConfig &);
public:
    std::string _configDir;
    qrs::QrsChainManagerPtr _qrsChainMgrPtr;
    HaCompressType _resultCompressType;
    std::set<std::string> _metricsSrcWhiteList;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsSearchConfig);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSSEARCHCONFIG_H
