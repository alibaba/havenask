#ifndef ISEARCH_BS_COUNTERSYNCHRONIZERCREATOR_H
#define ISEARCH_BS_COUNTERSYNCHRONIZERCREATOR_H

#include "build_service/common_define.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/util/Log.h"
#include "build_service/config/CounterConfig.h"
#include <indexlib/util/counter/counter_map.h>

namespace build_service {
namespace common {

class CounterSynchronizerCreator
{
public:
    CounterSynchronizerCreator();
    ~CounterSynchronizerCreator();
private:
    CounterSynchronizerCreator(const CounterSynchronizerCreator &);
    CounterSynchronizerCreator& operator=(const CounterSynchronizerCreator &);
public:
    static bool validateCounterConfig(const config::CounterConfigPtr &counterConfig);
    
    static IE_NAMESPACE(util)::CounterMapPtr loadCounterMap(
            const config::CounterConfigPtr &counterConfig, bool &loadFromExisted);
    
    static CounterSynchronizer* create(const IE_NAMESPACE(util)::CounterMapPtr &counterMap,
            const config::CounterConfigPtr &counterConfig);
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterSynchronizerCreator);

}
}

#endif //ISEARCH_BS_COUNTERSYNCHRONIZERCREATOR_H
