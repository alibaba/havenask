package com.taobao.search.iquan.core.rel.rules.logical;

import com.taobao.search.iquan.core.api.schema.TvfInputTable;
import com.taobao.search.iquan.core.api.schema.TvfOutputTable;
import com.taobao.search.iquan.core.api.schema.TvfSignature;
import com.taobao.search.iquan.core.catalog.function.internal.TableValueFunction;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.*;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.BitSets;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.IntStream;

@Value.Enclosing
public class IquanProjectTvfTransposeRule
        extends RelRule<IquanProjectTvfTransposeRule.Config>
        implements TransformationRule {

    private static final Logger logger = LoggerFactory.getLogger(IquanProjectTvfTransposeRule.class);

    protected IquanProjectTvfTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final TableFunctionScan tvfScan = call.rel(1);
        final List<RelNode> tvfInputs = tvfScan.getInputs();
        if (tvfInputs.size() != 1) {
            return;
        }
        try {
            final ProjectPushTvf pusher = new ProjectPushTvf(project, tvfScan, tvfInputs.get(0), call.builder());
            if (!pusher.canPush()) {
                return;
            }
            final RelNode newTopProject = pusher.convert();
            if (newTopProject != null) {
                call.transformTo(newTopProject);
            }
        } catch (Exception e) {
            logger.warn("exec IquanProjectTvfTransposeRule failed:", e);
        }
    }

    public static class ProjectPushTvf {

        public static final String ENABLE_PUSH_PROJECT_KEY = "enable_project_push";
        private final Project topProject;
        private final TableFunctionScan tvfScan;
        private final RelNode tvfInput;
        private final RelBuilder relBuilder;
        private final RexCall tvfCall;
        private final TableValueFunction tvfOp;
        private final TvfSignature tvfSig;
        private final RelRecordType tvfInputType;

        public ProjectPushTvf(
                Project topProject,
                TableFunctionScan tvfScan,
                RelNode tvfInput,
                RelBuilder relBuilder)
        {
            this.topProject = topProject;
            this.tvfScan = tvfScan;
            this.tvfInput = tvfInput;
            this.relBuilder = relBuilder;
            this.tvfCall = (RexCall) tvfScan.getCall();
            this.tvfOp = (TableValueFunction) tvfCall.getOperator();
            this.tvfSig = tvfOp.getTvfFunction().getSignatureList().get(0);
            this.tvfInputType = (RelRecordType) tvfInput.getRowType();
        }

        public boolean canPush() {
            boolean enableProjectPush = (boolean) tvfOp.getProperties()
                    .getOrDefault(ENABLE_PUSH_PROJECT_KEY, Boolean.FALSE);
            if (!enableProjectPush) {
                return false;
            }
            for (TvfInputTable table : tvfSig.getInputTables()) {
                if (!table.getInputRelFields().isEmpty()) {
                    return false;
                }
                if (!table.isAutoInfer()) {
                    return false;
                }
            }
            for (TvfOutputTable table : tvfSig.getOutputTables()) {
                if (!table.isAutoInfer()) {
                    return false;
                }
            }
            return true;
        }

        public RelNode convert() {
            final BitSet inputRefs = locateInputReference();
            /**
             * 输入表的所有字段都被引用到，不需要做字段裁剪
             * */
            boolean refAll = IntStream.range(0, tvfInputType.getFieldCount())
                    .allMatch(inputRefs::get);
            if (refAll) {
                return null;
            }
            return pushProject(inputRefs);
        }

        /**
         * 定位对tvfInput依赖的字段，依赖如下
         * 1. 排除tvf新增字段后，topProject依赖的所有字段
         * 2. tvf配置的check_fields
         * 3. tvf表达式中DESCRIPTOR描述的字段
         * 返回字段引用的BitSet
         * */
        public BitSet locateInputReference() {
            final List<String> inputFieldNames = tvfInputType.getFieldNames();
            final int nInputFields = inputFieldNames.size();
            final BitSet inputRefs = new BitSet(nInputFields);

            /**
             * 对于依赖1，因为tvf输入表和输出表都是auto_infer，输出表是在输入表基础上扩展了new_fields，
             * 所以topProject依赖字段的下标如果小于tvf输入表的字段数，那这个下标就是输入表对应字段的下标
             * */
            final RexVisitor<Void> inferVisitor = new RexVisitorImpl<Void>(true) {
                @Override
                public Void visitInputRef(RexInputRef inputRef) {
                    int index = inputRef.getIndex();
                    if (index < nInputFields) {
                        inputRefs.set(index);
                    }
                    return null;
                }
            };
            inferVisitor.visitEach(topProject.getProjects());

            /**
             * 对于依赖2，需要计算tvf的check_fields在输入表上的下标
             * */
            final Set<String> reserveFields = new HashSet<>();
            for (TvfInputTable table : tvfSig.getInputTables()) {
                table.getCheckRelFields().forEach(f -> reserveFields.add(f.getName()));
            }
            if (!reserveFields.isEmpty()) {
                for (int i = 0; i < nInputFields; ++i) {
                    if (reserveFields.contains(inputFieldNames.get(i))) {
                        inputRefs.set(i);
                    }
                }
            }

            /**
             * 对于依赖3，因为validate阶段已经将tvf的字段依赖表示为输入表的下标，
             * 所以只需要遍历tvf表达式的所有操作数，对字段引用类型的操作数记录下标
             * */
            final RexVisitor<Void> operandVisitor = new RexVisitorImpl<Void>(true) {
                public Void visitInputRef(RexInputRef inputRef) {
                    inputRefs.set(inputRef.getIndex());
                    return null;
                }
            };
            tvfCall.accept(operandVisitor);

            return inputRefs;
        }

        /**
         * 下推project，返回下推后的关系根节点
         * */
        public RelNode pushProject(BitSet inputRefs) {
            /**
             * 新增一个Project节点，以tvfInput做为输入，只project被引用的字段。
             * 记录字段裁剪前后字段下标的映射关系
             * */
            final RexBuilder rexBuilder = tvfScan.getCluster().getRexBuilder();
            final Map<Integer, Integer> old2newIndex = new HashMap<>();
            final List<RexNode> pushProjectNodes = new ArrayList<>();
            final List<String> pushProjectFieldNames = new ArrayList<>();
            int newIndex = 0;
            for (int i : BitSets.toIter(inputRefs)) {
                old2newIndex.put(i, newIndex);
                ++newIndex;
                pushProjectNodes.add(rexBuilder.makeInputRef(tvfInput, i));
                pushProjectFieldNames.add(tvfInputType.getFieldNames().get(i));
            }
            final Project pushProject = (Project) relBuilder.push(tvfInput)
                    .project(pushProjectNodes, pushProjectFieldNames, true)
                    .build();
            /**
             * 将pushProject做为tvf的输入，重定向字段引用和表引用，构建新的tvf节点
             * */
            final RexShuttle tvfShuttle = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    int newIndex = old2newIndex.getOrDefault(inputRef.getIndex(), Integer.MAX_VALUE);
                    return rexBuilder.makeInputRef(pushProject, newIndex);
                }

                @Override
                public RexNode visitRangeRef(RexRangeRef rangeRef) {
                    return rexBuilder.makeRangeReference(pushProject);
                }
            };
            final List<RexNode> newTvfOperands = tvfShuttle.visitList(tvfCall.getOperands());
            final RelNode newTvfScan = relBuilder.push(pushProject)
                    .functionScan(tvfOp, 1, newTvfOperands)
                    .build();
            /**
             * 重定向topProject的字段引用，构建新的Project节点
             * 如果老下标小于nInputFields，说明字段是输入表上的，可以用old2newIndex转成新下标
             * 如果老下标大于等于nInputFields，说明是tvf新增的字段，老下标减nInputFields就是第几个新增字段，再加输入表的字段数就是新下标
             * */
            final int nInputFields = tvfInputType.getFieldCount();
            final int nNewInputFields = pushProjectNodes.size();
            final RexShuttle projectShuttle = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    int index = inputRef.getIndex();
                    int newIndex;
                    if (index < nInputFields) {
                        newIndex = old2newIndex.getOrDefault(inputRef.getIndex(), Integer.MAX_VALUE);
                    } else {
                        newIndex = index - nInputFields + nNewInputFields;
                    }
                    return rexBuilder.makeInputRef(newTvfScan, newIndex);
                }
            };
            final List<RexNode> newTopProjectNodes = projectShuttle.visitList(topProject.getProjects());
            return relBuilder.push(newTvfScan)
                    .project(newTopProjectNodes, topProject.getRowType().getFieldNames(), true)
                    .build();
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableIquanProjectTvfTransposeRule.Config.builder().operandSupplier(b0 ->
                b0.operand(LogicalProject.class).oneInput(b1 ->
                        b1.operand(LogicalTableFunctionScan.class).anyInputs()))
                .build();

        @Override
        default IquanProjectTvfTransposeRule toRule() {
            return new IquanProjectTvfTransposeRule(this);
        }
    }
}
