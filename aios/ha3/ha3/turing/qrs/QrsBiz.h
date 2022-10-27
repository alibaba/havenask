#ifndef ISEARCH_TURING_QRSBIZ_H
#define ISEARCH_TURING_QRSBIZ_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/search/Biz.h>
#include <suez/search/BizMeta.h>
#include <ha3/monitor/QrsBizMetrics.h>
#include <ha3/config/ConfigAdapter.h>
#include <ha3/config/AnomalyProcessConfig.h>
#include <ha3/service/QrsSearchConfig.h>
#include <ha3/turing/qrs/QrsRunGraphContext.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(turing);

class QrsBiz: public suez::turing::Biz
{
public:
    QrsBiz();
    ~QrsBiz();
private:
    QrsBiz(const QrsBiz &);
    QrsBiz& operator=(const QrsBiz &);

public:
    service::QrsSearchConfigPtr getQrsSearchConfig();
    config::ConfigAdapterPtr getConfigAdapter();
    kmonitor::MetricsReporter *getPluginMetricsReporter();
    QrsRunGraphContextArgs getQrsRunGraphContextArgs();
protected:
    tensorflow::SessionResourcePtr createSessionResource(uint32_t count) override;
    tensorflow::QueryResourcePtr createQueryResource() override;
    bool updateFlowConfig(const std::string &zoneBizName) override;
    std::string getBizInfoFile() override;
    bool getDefaultBizJson(std::string &defaultBizJson) override;
    tensorflow::Status preloadBiz() override;

private:
    bool createQrsSearchConfig();
    HaCompressType getCompressType(const config::QrsConfig &qrsConfig);
    bool updateHa3ClusterFlowConfig();
    bool updateTuringClusterFlowConfig();
public:
    static std::string getBenchmarkBizName(const std::string &bizName);
    static void fillCompatibleFieldInfo(config::AnomalyProcessConfig &flowConf);
private:
    service::QrsSearchConfigPtr _qrsSearchConfig;
    config::ConfigAdapterPtr _configAdapter;
    kmonitor::MetricsReporterPtr _pluginMetricsReporter;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<QrsBiz> QrsBizPtr;

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_TURING_QRSBIZ_H
