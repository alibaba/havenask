package com.taobao.search.iquan.core.rel.metadata;

import com.taobao.search.iquan.core.api.schema.Table;
import com.taobao.search.iquan.core.rel.ops.physical.*;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Default implementations of the
 * {@link IquanMetadata.ScanNode}
 * metadata provider for Physical RelNode.
 */
public class IquanRelMdScanNode implements MetadataHandler<IquanMetadata.ScanNode> {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(new IquanRelMdScanNode(), IquanMetadata.ScanNode.METHOD);

    public IquanRelMdScanNode() {
    }

    @Override
    public MetadataDef<IquanMetadata.ScanNode> getDef() {
        return IquanMetadata.ScanNode.DEF;
    }

    public List<TableScan> getScanNode(TableScan r, RelMetadataQuery mq) {
        return null;
    }

    public List<TableScan> getScanNode(IquanTableScanBase r, RelMetadataQuery mq) {
        return Collections.singletonList(r);
    }

    public List<TableScan> getScanNode(IquanValuesOp r, RelMetadataQuery mq) {
        return null;
    }

    public List<TableScan> getScanNode(HepRelVertex r, RelMetadataQuery mq) {
        RelNode input = r.getCurrentRel();
        return getScanNodeInternal(input, mq);
    }

    public List<TableScan> getScanNode(IquanUnionOp r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        return getScanNodeInternal(inputs, mq);
    }

    public List<TableScan> getScanNode(IquanMergeOp r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        return getScanNodeInternal(inputs, mq);
    }

    public List<TableScan> getScanNode(IquanJoinOp r, RelMetadataQuery mq) {
        return getScanNodeInternal(r.getInputs(), mq);
    }

    public List<TableScan> getScanNode(IquanTableFunctionScanOp r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        return getScanNodeInternal(inputs, mq);
    }

    public List<TableScan> getScanNode(IquanCorrelateOp r, RelMetadataQuery mq) {
        return getScanNodeInternal(r.getLeft(), mq);
    }

    public List<TableScan> getScanNode(ExecLookupJoinOp r, RelMetadataQuery mq) {
        return getScanNodeInternal(r.getJoinOp(), mq);
    }

    public List<TableScan> getScanNode(RelNode r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        return getScanNodeInternal(inputs, mq);
    }

    private List<TableScan> getScanNodeInternal(RelNode input, RelMetadataQuery mq) {
        IquanMetadata.ScanNode metadata = input.metadata(IquanMetadata.ScanNode.class, mq);
        return metadata.getScanNode();
    }

    private List<TableScan> getScanNodeInternal(List<RelNode> inputs, RelMetadataQuery mq) {
        IquanMetadata.ScanNode metadata;
        List<TableScan> resultList = new ArrayList<>(inputs.size());
        for (RelNode input : inputs) {
            metadata = input.metadata(IquanMetadata.ScanNode.class, mq);
            List<TableScan> scanList = metadata.getScanNode();
            filterTableScan(resultList, scanList);
        }
        return resultList;
    }

    private void filterTableScan(List<TableScan> resultList, List<TableScan> scanList) {
        for (TableScan tableScan : scanList) {
            Table table = IquanRelOptUtils.getIquanTable(tableScan);
            if (table != null) {
                resultList.add(tableScan);
            }
        }
    }
}
