package com.taobao.search.iquan.core.rel.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;

public class IquanDefaultRelMetadataProvider {
    public static RelMetadataProvider INSTANCE = buildRelMetadataProvider();

    private static RelMetadataProvider buildRelMetadataProvider() {
        return ChainedRelMetadataProvider.of(
                ImmutableList.of(
//                        FlinkDefaultRelMetadataProvider.INSTANCE(),
                        DefaultRelMetadataProvider.INSTANCE,
                        IquanRelMdScanNode.SOURCE,
                        IquanRelMDSubDAGPartitionPruning.SOURCE
                )
        );
    }
}
