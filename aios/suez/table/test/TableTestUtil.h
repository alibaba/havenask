#pragma once

#include "suez/table/Table.h"

namespace suez {

class TableTestUtil {
public:
    static TablePtr make(const PartitionId &pid) {
        auto tableResource = TableResource(pid);
        TablePtr table(new Table(tableResource));
        return table;
    }
    static TablePtr make(const PartitionId &pid, const CurrentPartitionMeta &current) {
        auto tableResource = TableResource(pid);
        TablePtr table(new Table(tableResource));
        table->_partitionMeta.reset(new CurrentPartitionMeta(current));
        return table;
    }
};

} // namespace suez
