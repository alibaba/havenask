#ifndef ISEARCH_BS_COUNTERREDISSYNCHRONIZER_H
#define ISEARCH_BS_COUNTERREDISSYNCHRONIZER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/util/RedisClient.h"
#include "build_service/common/CounterSynchronizer.h"
#include <indexlib/util/counter/counter_map.h>

namespace build_service {
namespace common {

class CounterRedisSynchronizer : public CounterSynchronizer
{
public:
    CounterRedisSynchronizer();
    ~CounterRedisSynchronizer();
private:
    CounterRedisSynchronizer(const CounterRedisSynchronizer &);
    CounterRedisSynchronizer& operator=(const CounterRedisSynchronizer &);
public:
    static IE_NAMESPACE(util)::CounterMapPtr loadCounterMap(
            const util::RedisInitParam &redisParam,
            const std::string &key,
            const std::string &field,
            bool &valueExist);
public:
    bool init(const IE_NAMESPACE(util)::CounterMapPtr& counterMap,
              const util::RedisInitParam &redisParam,
              const std::string &key,
              const std::string &field,
              int32_t ttlInSecond = -1);
public:
    bool sync() const override;

private:
    util::RedisInitParam _redisInitParam;
    std::string _key;
    std::string _field;
    int32_t _ttlInSecond;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterRedisSynchronizer);

}
}

#endif //ISEARCH_BS_COUNTERREDISSYNCHRONIZER_H
