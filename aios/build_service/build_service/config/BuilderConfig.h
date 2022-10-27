#ifndef ISEARCH_BS_BUILDERCONFIG_H
#define ISEARCH_BS_BUILDERCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/index_base/index_meta/partition_meta.h>
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class BuilderConfig : public autil::legacy::Jsonizable
{
public:
    BuilderConfig();
    ~BuilderConfig();
public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    bool validate() const;
    int64_t calculateSortQueueMemory(size_t buildTotalMemMB) const
    {
        if (sortQueueMem < 0) {
            return (int64_t)(buildTotalMemMB * SORT_QUEUE_MEM_FACTOR);
        }
        return sortQueueMem / 2;        
    }
public:
    bool operator==(const BuilderConfig &other) const;
public:
    int64_t sortQueueMem;  // in MB
    int64_t asyncQueueMem; // in MB
    bool sortBuild;
    bool asyncBuild;
    bool documentFilter;
    uint32_t asyncQueueSize;
    uint32_t sortQueueSize; // for test
    int sleepPerdoc; // for test
    int buildQpsLimit;
    size_t batchBuildSize;
    IE_NAMESPACE(index_base)::SortDescriptions sortDescriptions;

public:
    static const uint32_t DEFAULT_SORT_QUEUE_SIZE;
    static const uint32_t DEFAULT_ASYNC_QUEUE_SIZE;
    static const int64_t INVALID_SORT_QUEUE_MEM;
    static const int64_t INVALID_ASYNC_QUEUE_MEM;
    static constexpr double SORT_QUEUE_MEM_FACTOR = 0.3;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_BUILDERCONFIG_H
