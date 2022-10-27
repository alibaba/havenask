#ifndef ISEARCH_BS_COUNTERFILESYNCHRONIZER_H
#define ISEARCH_BS_COUNTERFILESYNCHRONIZER_H

#include "build_service/common_define.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/util/Log.h"
#include <indexlib/util/counter/counter_map.h>
#include <functional>

namespace build_service {
namespace common {

class CounterFileSynchronizer : public CounterSynchronizer
{
public:
    CounterFileSynchronizer();
    ~CounterFileSynchronizer();
private:
    CounterFileSynchronizer(const CounterFileSynchronizer &);
    CounterFileSynchronizer& operator=(const CounterFileSynchronizer &);

public:
    static IE_NAMESPACE(util)::CounterMapPtr loadCounterMap(
            const std::string &counterFilePath, bool &fileExist);
    
public:
    bool init(const IE_NAMESPACE(util)::CounterMapPtr& counterMap,
              const std::string& counterFilePath);
public:
    bool sync() const override;

private:
    static bool doWithRetry(std::function<bool()> predicate,
                            int retryLimit, int intervalInSecond)
    {
        for (int i = 0; i < retryLimit; ++i) {
            if (predicate()) {
                return true;
            } else {
                BS_LOG(WARN, "predicate return false, retry for %d times", i+1);
            }
            sleep(intervalInSecond);
        }
        return false;
    }
    
private:
    std::string _counterFilePath;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterFileSynchronizer);

}
}

#endif //ISEARCH_BS_COUNTERFILESYNCHRONIZER_H
