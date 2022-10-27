#ifndef ISEARCH_BS_COUNTERSYNCHRONIZER_H
#define ISEARCH_BS_COUNTERSYNCHRONIZER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/BasicDefs.pb.h"
#include <autil/LoopThread.h>
#include <indexlib/util/counter/counter_map.h>

namespace build_service {
namespace common {

class CounterSynchronizer
{
public:
    CounterSynchronizer() {};
    virtual ~CounterSynchronizer() {};

private:
    CounterSynchronizer(const CounterSynchronizer &);
    CounterSynchronizer& operator=(const CounterSynchronizer &);

public:
    static std::string getCounterZkRoot(const std::string &appZkRoot,
            const proto::BuildId &buildId);
    static bool getCounterZkFilePath(const std::string &appZkRoot,
            const proto::PartitionId &pid, std::string &counterFilePath);
    static std::string getCounterRedisKey(const std::string &serviceName,
            const proto::BuildId &buildId);
    
    static bool getCounterRedisHashField(const proto::PartitionId &pid, std::string &fieldName); 
    
public:
    bool init(const IE_NAMESPACE(util)::CounterMapPtr& counterMap) {
        if (!counterMap) {
            BS_LOG(ERROR, "init failed due to counterMap is NULL");
            return false;
        }
        _counterMap = counterMap;
        return true;
    }
    
    IE_NAMESPACE(util)::CounterMapPtr getCounterMap() const
    {
        return _counterMap;
    }

    bool beginSync(int64_t syncInterval = DEFAULT_SYNC_INTERVAL);
    void stopSync();
    virtual bool sync() const = 0;
    
protected:
    IE_NAMESPACE(util)::CounterMapPtr _counterMap;
    autil::LoopThreadPtr _syncThread;
    static const uint64_t DEFAULT_SYNC_INTERVAL = 30u;    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterSynchronizer);

}
}

#endif //ISEARCH_BS_COUNTERSYNCHRONIZER_H
