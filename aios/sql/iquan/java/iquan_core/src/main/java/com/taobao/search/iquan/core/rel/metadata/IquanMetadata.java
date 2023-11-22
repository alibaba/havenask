package com.taobao.search.iquan.core.rel.metadata;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public abstract class IquanMetadata {

    /**
     * Metadata about the scan operators
     */
    public interface ScanNode extends Metadata {
        Method METHOD = Types.lookupMethod(ScanNode.class, "getScanNodes");
        MetadataDef<ScanNode> DEF = MetadataDef.of(
                ScanNode.class,
                ScanNode.Handler.class,
                METHOD
        );

        List<TableScan> getScanNodes();

        interface Handler extends MetadataHandler<ScanNode> {
            List<TableScan> getScanNodes(RelNode r, RelMetadataQuery mq);
        }
    }

    /**
     * Metadata about the sub dag partition pruning
     */
    public interface SubDAGPartitionPruning extends Metadata {
        Method METHOD = Types.lookupMethod(SubDAGPartitionPruning.class, "getPartitionPruning");
        MetadataDef<SubDAGPartitionPruning> DEF = MetadataDef.of(
                SubDAGPartitionPruning.class,
                SubDAGPartitionPruning.Handler.class,
                METHOD
        );

        Object getPartitionPruning();

        interface Handler extends MetadataHandler<SubDAGPartitionPruning> {
            Object getPartitionPruning(RelNode r, RelMetadataQuery mq);
        }
    }
}
