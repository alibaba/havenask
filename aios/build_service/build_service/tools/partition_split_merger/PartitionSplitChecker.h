#ifndef ISEARCH_BS_PARTITIONSPLITCHECKER_H
#define ISEARCH_BS_PARTITIONSPLITCHECKER_H

#include "build_service/util/Log.h"
#include <indexlib/config/index_partition_options.h>
#include <indexlib/config/index_partition_schema.h>

namespace build_service {
namespace tools {

class PartitionSplitChecker
{
public:
    struct Param {
        std::string configPath;
        std::string clusterName;
    };

public:
    PartitionSplitChecker();
    ~PartitionSplitChecker();

private:
    PartitionSplitChecker(const PartitionSplitChecker &);
    PartitionSplitChecker& operator=(const PartitionSplitChecker &);

public:
    bool init(const Param &param);
    bool check() const;

private:
    bool initIndexPartitionSchema(const Param &param);

private:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _indexPartitionSchemaPtr;
private:
    friend class PartitionSplitCheckerTest;
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_PARTITIONSPLITCHECKER_H
