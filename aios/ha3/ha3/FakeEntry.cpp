#include <suez/search/SearchManager.h>

namespace suez {

class FakeWorker : public suez::SearchManager
{
public:
    FakeWorker() {
    }
    ~FakeWorker() {
    }
private:
    FakeWorker(const FakeWorker &);
    FakeWorker& operator=(const FakeWorker &);
public:
    bool update(const suez::IndexProvider &indexProvider,
                const suez::BizMetas &bizMetas,
                const suez::ServiceInfo &serviceInfo,
                const autil::legacy::json::JsonMap &targetAppInfo,
                suez::SuezErrorCollector &errorCollector) override
    {
        return false;
    }
    autil::legacy::json::JsonMap getServiceInfo() const override {
        return autil::legacy::json::JsonMap();
    }
    void stopService() override {
    }
    void stopWorker() override {
    }
};

SearchManager *createSearchManager() {
    return new FakeWorker();
}

}
