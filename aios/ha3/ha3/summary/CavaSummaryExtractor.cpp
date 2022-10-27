#include <ha3/summary/CavaSummaryExtractor.h>
#include <ha3/cava/SummaryDoc.h>
#include <ha3/cava/SummaryProvider.h>
#include <cava/common/ErrorDef.h>
#include <suez/turing/metrics/Monitor.h>

using namespace std;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(summary);
HA3_LOG_SETUP(summary, CavaSummaryExtractor);
static const string kSummaryPluginQps = "cava.summaryPluginQps";
static const string kSummaryExceptionQps = "cava.summaryExceptionQps";


CavaSummaryExtractor::CavaSummaryExtractor(const KeyValueMap &parameters,
        const CavaPluginManagerPtr &cavaPluginManagerPtr,
        kmonitor::MetricsReporter *metricsReporter)
    : SummaryExtractor()
    , _cavaPluginManager(cavaPluginManagerPtr)
    , _metricsReporter(metricsReporter)
    , _defaultClassName("plugins.DefaultSummaryProcessor")
    , _summaryNameKey("summary_class_name")
    , _tracer(NULL)
    , _summaryObj(NULL)
    , _cavaCtx(NULL)
    , _summaryModuleInfo(NULL)
{
    auto it = parameters.find("default_class_name");
    if (it != parameters.end()) {
        _defaultClassName = it ->second;
    }
    it = parameters.find("summary_name_key");
    if (it != parameters.end()) {
        _summaryNameKey = it ->second;
    }
}

CavaSummaryExtractor::~CavaSummaryExtractor()
{
    DELETE_AND_SET_NULL(_cavaCtx);
}

bool CavaSummaryExtractor::beginRequest(SummaryExtractorProvider *provider) {
    REPORT_USER_MUTABLE_QPS(_metricsReporter, kSummaryPluginQps);
    _tracer = provider->getRequestTracer();
    const common::Request *request = provider->getRequest();
    common::ConfigClause *configClause = request->getConfigClause();
    ha3::SummaryProvider *summaryProvider = provider->getCavaSummaryProvider();
    _cavaCtx = new CavaCtx(provider->getCavaAllocator());
    std::string className = configClause->getKVPairValue(_summaryNameKey);
    if (className.empty()) {
        className = _defaultClassName;
    }

    _cavaModuleInfo = _cavaPluginManager->getCavaModuleInfo(className);
    _summaryModuleInfo = dynamic_cast<CavaSummaryModuleInfo *>(_cavaModuleInfo.get());
    if (!_summaryModuleInfo) {
        REQUEST_TRACE(ERROR, "no summary processor in this query: %s", className.c_str());
        return false;
    }

    REQUEST_TRACE(INFO, "use summary processor [%s]", className.c_str());
    _summaryObj = _summaryModuleInfo->createFunc(_cavaCtx);
    if (unlikely(_cavaCtx->exception != cava::ExceptionCode::EXC_NONE)) {
        REQUEST_TRACE(ERROR, "create summary processor: %s failed, exception:\n%s",
                      _summaryModuleInfo->className.c_str(),
                      _cavaCtx->getStackInfo().c_str());
        REPORT_USER_MUTABLE_QPS(_metricsReporter, kSummaryExceptionQps);
        _cavaCtx->reset();
        return false;
    }
    if (NULL == _summaryObj) {
        REQUEST_TRACE(ERROR, "create summary processor: %s failed",
                      _summaryModuleInfo->className.c_str());
        return false;
    }
    if (!_summaryModuleInfo->initFunc) {
        REQUEST_TRACE(ERROR, "no init found");
        return false;
    }
    bool ret = _summaryModuleInfo->initFunc(_summaryObj, _cavaCtx, summaryProvider);
    if (unlikely(_cavaCtx->exception != cava::ExceptionCode::EXC_NONE)) {
        REQUEST_TRACE(ERROR, "call [%s]'s init exception\n%s",
                      _summaryModuleInfo->className.c_str(),
                      _cavaCtx->getStackInfo().c_str());
        REPORT_USER_MUTABLE_QPS(_metricsReporter, kSummaryExceptionQps);
        _cavaCtx->reset();
        return false;
    }
    if (unlikely(!ret)) {
        REQUEST_TRACE(ERROR, "call [%s]'s init method failed",
                      _summaryModuleInfo->className.c_str());
        return false;
    }

    return true;
}

void CavaSummaryExtractor::extractSummary(common::SummaryHit &summaryHit) {
    ha3::SummaryDoc doc(&summaryHit);
    bool ret = _summaryModuleInfo->processFunc(_summaryObj, _cavaCtx, &doc);
    //目前cava脚本内不支持hitTracer
    common::Tracer *tracer = summaryHit.getHitTracer();
    if (unlikely(_cavaCtx->exception != cava::ExceptionCode::EXC_NONE)) {
        if (tracer != NULL) {
            tracer->trace("cava summary process exception!\n" + _cavaCtx->getStackInfo());
        }
        REPORT_USER_MUTABLE_QPS(_metricsReporter, kSummaryExceptionQps);
        _cavaCtx->reset();
    }

    if (tracer != NULL && !ret) {
        tracer->trace("cava summary process failed!");
    }
}

END_HA3_NAMESPACE(summary);
