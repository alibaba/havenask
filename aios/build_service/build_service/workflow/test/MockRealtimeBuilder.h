#ifndef ISEARCH_BS_MOCKREALTIMEBUILDER_H
#define ISEARCH_BS_MOCKREALTIMEBUILDER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/RealtimeBuilder.h"

namespace build_service {
namespace workflow {

class MockRealtimeBuilder : public RealtimeBuilder
{
public:
    MockRealtimeBuilder(const std::string &configPath,
                        const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart,
                        const RealtimeBuilderResource& builderResource)
        : RealtimeBuilder(configPath, indexPart, builderResource)
    {}

    ~MockRealtimeBuilder() {}

protected:
    MOCK_CONST_METHOD0(getIndexRoot, std::string());
    MOCK_METHOD2(downloadConfig, bool(const std::string&, KeyValueMap&));
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_MOCKREALTIMEBUILDER_H
