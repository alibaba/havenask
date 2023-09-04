#pragma once

#include "suez/table/SuezPartitionFactory.h"
#include "suez/table/test/MockSuezPartition.h"

namespace suez {

class FakeSuezPartitionFactory : public SuezPartitionFactory {
public:
    SuezPartitionPtr create(SuezPartitionType type,
                            const TableResource &tableResource,
                            const CurrentPartitionMetaPtr &partitionMeta) override {
        switch (type) {
        case SPT_INDEXLIB:
            return SuezPartitionPtr(new NiceMockSuezIndexPartition(tableResource, partitionMeta));
        case SPT_TABLET:
            return SuezPartitionPtr(
                new NiceMockSuezPartition(partitionMeta, tableResource.pid, tableResource.metricsReporter));
        case SPT_RAWFILE:
            return SuezPartitionPtr(new NiceMockSuezRawFilePartition(tableResource, partitionMeta));
        default:
            assert(false);
            return SuezPartitionPtr();
        }
    }
};

} // namespace suez
