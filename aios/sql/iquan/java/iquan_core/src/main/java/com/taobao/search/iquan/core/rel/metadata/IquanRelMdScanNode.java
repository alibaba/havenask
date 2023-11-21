package com.taobao.search.iquan.core.rel.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.rel.ops.physical.ExecLookupJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCorrelateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanMergeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUnionOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanValuesOp;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

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

    public List<TableScan> getScanNodes(TableScan r, RelMetadataQuery mq) {
        return new ArrayList<>();
    }

    public List<TableScan> getScanNodes(IquanTableScanBase r, RelMetadataQuery mq) {
        return Collections.singletonList(r);
    }

    public List<TableScan> getScanNodes(IquanValuesOp r, RelMetadataQuery mq) {
        return new ArrayList<>();
    }

    public List<TableScan> getScanNodes(HepRelVertex r, RelMetadataQuery mq) {
        RelNode input = r.getCurrentRel();
        return getScanNodeInternal(input, mq);
    }

    public List<TableScan> getScanNodes(IquanUnionOp r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        return getScanNodeInternal(inputs, mq);
    }

    public List<TableScan> getScanNodes(IquanMergeOp r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        return getScanNodeInternal(inputs, mq);
    }

    public List<TableScan> getScanNodes(IquanJoinOp r, RelMetadataQuery mq) {
        return getScanNodeInternal(r.getInputs(), mq);
    }

    public List<TableScan> getScanNodes(IquanTableFunctionScanOp r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        return getScanNodeInternal(inputs, mq);
    }

    public List<TableScan> getScanNodes(IquanCorrelateOp r, RelMetadataQuery mq) {
        return getScanNodeInternal(r.getLeft(), mq);
    }

    public List<TableScan> getScanNodes(ExecLookupJoinOp r, RelMetadataQuery mq) {
        List<RelNode> inputs = new ArrayList<>(2);
        inputs.add(r.getBuildOp());
        inputs.add(r.getInput());
        return getScanNodeInternal(inputs, mq);
    }

    public List<TableScan> getScanNodes(RelNode r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        return getScanNodeInternal(inputs, mq);
    }

    private List<TableScan> getScanNodeInternal(RelNode input, RelMetadataQuery mq) {
        if (input instanceof IquanExchangeOp) {
            return new ArrayList<>();
        }
        IquanMetadata.ScanNode metadata = input.metadata(IquanMetadata.ScanNode.class, mq);
        return metadata.getScanNodes();
    }

    private List<TableScan> getScanNodeInternal(List<RelNode> inputs, RelMetadataQuery mq) {
        IquanMetadata.ScanNode metadata;
        List<TableScan> resultList = new ArrayList<>(inputs.size());
        for (RelNode input : inputs) {
            if (input instanceof IquanExchangeOp) {
                continue;
            }
            metadata = input.metadata(IquanMetadata.ScanNode.class, mq);
            List<TableScan> scanList = metadata.getScanNodes();
            filterTableScan(resultList, scanList);
        }
        return resultList;
    }

    private void filterTableScan(List<TableScan> resultList, List<TableScan> scanList) {
        for (TableScan tableScan : scanList) {
            IquanTable iquanTable = IquanRelOptUtils.getIquanTable(tableScan);
            if (iquanTable != null) {
                resultList.add(tableScan);
            }
        }
    }
}
