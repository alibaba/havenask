package com.taobao.search.iquan.core.rel.rules.physical;

import java.util.*;
import java.util.stream.Collectors;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Table;
import com.taobao.search.iquan.core.api.schema.TableType;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.hint.IquanHintCategory;
import com.taobao.search.iquan.core.rel.hint.IquanHintOptUtils;
import com.taobao.search.iquan.core.rel.ops.physical.*;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.rel.rules.physical.join_utils.RowCountPredicator;
import com.taobao.search.iquan.core.rel.rules.physical.join_utils.DecisionRuleTable;
import com.taobao.search.iquan.core.rel.rules.physical.join_utils.Pattern;
import com.taobao.search.iquan.core.rel.rules.physical.join_utils.PhysicalJoinFactory;
import com.taobao.search.iquan.core.utils.IquanJoinUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.javatuples.Pair;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecEquiJoinBaseRule extends RelOptRule {
    private static final Logger logger = LoggerFactory.getLogger(ExecEquiJoinBaseRule.class);

    protected ExecEquiJoinBaseRule(RelBuilderFactory relBuilderFactory) {
        super(operand(IquanJoinOp.class,
                any()), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IquanJoinOp join = call.rel(0);

        if (!IquanJoinUtils.isEquiJoin(join)) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanJoinOp join = call.rel(0);

        IquanConfigManager conf = IquanRelOptUtils.getConfigFromRel(join);
        boolean forceHashJoin = conf.getBoolean(SqlConfigOptions.IQUAN_OPTIMIZER_FORCE_HASH_JOIN);

        RelNode leftInput = IquanRelOptUtils.toRel(join.getLeft());
        RelNode rightInput = IquanRelOptUtils.toRel(join.getRight());
        Table leftTable = IquanRelOptUtils.getIquanTable(leftInput);
        Table rightTable = IquanRelOptUtils.getIquanTable(rightInput);

        /**
         * calc attributes of Pattern
         */
        //Distribution leftDistribution = leftTable == null ? null : leftTable.getDistribution();
        //Distribution rightDistribution = rightTable == null ? null : rightTable.getDistribution();
        boolean leftUseIndex = false;
        boolean rightUseIndex = false;
        boolean leftBroadcast = checkBottomBroadcast(leftInput);
        boolean rightBroadcast = checkBottomBroadcast(rightInput);
        if (!forceHashJoin) {
            leftUseIndex = isUseIndex(join, leftInput, leftTable, new ArrayList<>(join.getLeftKeyNameMap().values()), true, rightBroadcast);
            rightUseIndex = isUseIndex(join, rightInput, rightTable, new ArrayList<>(join.getRightKeyNameMap().values()), false, leftBroadcast);
        }

        boolean leftScannable = IquanJoinUtils.isScannable(leftTable, leftInput);
        boolean rightScannable = IquanJoinUtils.isScannable(rightTable, rightInput);

        RowCountPredicator predicator = new RowCountPredicator();
        long leftRowCount = predicator.rowCount(leftInput);
        long rightRowCount = predicator.rowCount(rightInput);
        boolean leftIsBigger = leftRowCount > rightRowCount;

        Pair<RelHint, Boolean> validHintInfo = IquanHintOptUtils.resolveHints(
                leftInput, rightInput, IquanHintCategory.CAT_JOIN);
        if (leftInput instanceof IquanTableScanBase
                && rightInput instanceof IquanTableScanBase
                && join.getJoinType() == JoinRelType.INNER) {
            RelNode rel = MainAuxOpt.tryMainAuxOpt(join, validHintInfo);
            if (rel != null) {
                logger.debug("match MainAuxJoinOpt");
                call.transformTo(rel);
                return;
            }
            logger.debug("miss match MainAuxJoinOpt");
        }

        /**
         * build pattern with and without hint
         */
        Pattern pattern = new Pattern(leftUseIndex, rightUseIndex, leftScannable, rightScannable, leftIsBigger);
        Pattern finalPattern = pattern;
        if (!forceHashJoin) {
            Pattern newPatternWithHint = rewritePatternWithHint(validHintInfo, pattern);
            if (newPatternWithHint != null) {
                finalPattern = newPatternWithHint;
            }
        }

        /**
         * search result and construct physical join op
         */
        PhysicalJoinFactory physicalJoinFactory = DecisionRuleTable.search(finalPattern);
        if (physicalJoinFactory == null) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_JOIN_INVALID,
                    "pattern: [" + finalPattern + "] does not match any result");
        }

        /**
         * transform
         */
        call.transformTo(physicalJoinFactory.create(join, leftTable, rightTable));
    }

    private boolean checkBottomBroadcast(RelNode input) {
        RelOptTable relOptTable = RelDistributionUtil.getBottomTable(input);
        if (relOptTable == null) {
            return false;
        }
        Table table = IquanRelOptUtils.getIquanTable(relOptTable);
        if (table == null) {
            return false;
        }
        return table.getDistribution().getPartitionCnt() == 1;
    }

    private Pattern rewritePatternWithHint(Pair<RelHint, Boolean> validHintInfo, Pattern pattern) {
        RelHint validHint = validHintInfo.getValue0();
        if (validHint == null) {
            return null;
        }

        Pattern newPattern = (Pattern) pattern.clone();
        boolean leftIsBuild = validHintInfo.getValue1();
        String validHintName = validHint.hintName;
        if (validHintName.equals(ConstantDefine.LOOKUP_JOIN_HINT)) {
            if (leftIsBuild && pattern.isLeftHasIndex()) {
                newPattern.setRightHasIndex(false);
            } else if (!leftIsBuild && pattern.isRightHasIndex()) {
                newPattern.setLeftHasIndex(false);
            }
            return newPattern;
        } else if (validHintName.equals(ConstantDefine.HASH_JOIN_HINT)) {
            newPattern.setLeftHasIndex(false);
            newPattern.setRightHasIndex(false);
            return newPattern;
        }
        return null;
    }

    protected boolean isUseIndex(IquanJoinOp join, RelNode input, Table table,
                                 List<String> joinKeys, boolean isLeft, boolean otherBroadcast) {
        if (table == null || table.getTableType() == TableType.TT_ORC) {
            // TODO: use table meta feature instead of table type
            return false;
        }
        //check need shuffle
        if (!otherBroadcast) {
            Distribution distribution = table.getDistribution();
            if (distribution.getPartitionCnt() > 1) {
                List<Integer> hashFieldsPos = IquanJoinUtils.calcHashFieldsPos(distribution, joinKeys);
                if (hashFieldsPos == null) {
                    List<String> pkList = table.getPrimaryMap().keySet().stream()
                            .map(PlanWriteUtils::unFormatFieldName)
                            .collect(Collectors.toList());
                    if (!joinKeys.stream().anyMatch(pkList::contains)) {
                        return false;
                    }

                }
            }
        }
        return true;
    }
}

class MainAuxOpt extends Object{
    private static final Logger logger = LoggerFactory.getLogger(MainAuxOpt.class);
    private final IquanJoinOp join;
    private final IquanTableScanBase leftInput;
    private final IquanTableScanBase rightInput;
    private final Table leftTable;
    private final Table rightTable;
    private final JoinInfo joinInfo;
    private boolean isLeftMain;
    private final RexBuilder builder;
    private final List<RexNode> leftProjects;
    private final List<RexNode> rightProjects;
    private final RexNode leftCond;
    private final RexNode rightCond;
    private final RexNode joinCond;
    private int mainAuxCondIndex = -1;

    MainAuxOpt(IquanJoinOp join) {
        this.join = join;
        this.leftInput = (IquanTableScanBase) IquanRelOptUtils.toRel(join.getLeft());
        this.rightInput = (IquanTableScanBase) IquanRelOptUtils.toRel(join.getRight());
        this.leftTable = IquanRelOptUtils.getIquanTable(leftInput);
        this.rightTable = IquanRelOptUtils.getIquanTable(rightInput);
        this.joinInfo = join.analyzeCondition();
        this.isLeftMain = true;
        this.builder = join.getCluster().getRexBuilder();
        this.leftProjects = genProjects(leftInput);
        this.rightProjects = genProjects(rightInput);
        this.leftCond = genCondition(leftInput.getNearestRexProgram());
        this.rightCond = genCondition(rightInput.getNearestRexProgram());
        this.joinCond = join.getCondition();
    }

    private List<RexNode> genProjects(IquanTableScanBase input) {
        RexProgram program = input.getNearestRexProgram();
        if (program == null) {
            List<RexNode> projects = new ArrayList<>();
            List<RelDataTypeField> fields = input.getRowType().getFieldList();
            for (int i = 0; i < fields.size(); ++i) {
                projects.add(builder.makeInputRef(fields.get(i).getType(),i));
            }
            return projects;
        }
        return program.getProjectList()
                .stream()
                .map(program::expandLocalRef)
                .collect(Collectors.toList());
    }

    private RexNode genCondition(RexProgram program) {
        if (program == null || program.getCondition() == null) {
            return null;
        }
        return program.expandLocalRef(program.getCondition());
    }

    private RelOptTable checkMainAuxJoin(
            ImmutableIntList mainKeys,
            ImmutableIntList auxKeys,
            List<RexNode> mainProjects,
            List<RexNode> auxProjects,
            Table mainTable,
            Table auxTable,
            IquanTableScanBase mainInput)
    {
        for (com.taobao.search.iquan.core.api.schema.JoinInfo tJoinInfo : mainTable.getJoinInfos()) {
            if (!tJoinInfo.getTableName().equals(auxTable.getTableName())) {
                continue;
            }
            String fieldName = tJoinInfo.getJoinField();
            for (int i = 0; i < mainKeys.size(); ++i) {
                RexNode mainNode = mainProjects.get(mainKeys.get(0));
                if (!(mainNode instanceof RexInputRef)) {
                    continue;
                }
                RexInputRef mainRef = (RexInputRef) mainNode;
                RexNode auxNode = auxProjects.get(auxKeys.get(0));
                if (!(auxNode instanceof RexInputRef)) {
                    continue;
                }
                RexInputRef auxRef = (RexInputRef) auxNode;
                if (!fieldName.equals(mainTable.getFields().get(mainRef.getIndex()).getFieldName())) {
                    continue;
                }
                String rightFieldName = auxTable.getFields().get(auxRef.getIndex()).getFieldName();
                if (auxTable.getPrimaryMap().containsKey("$" + rightFieldName)) {
                    mainAuxCondIndex = i;
                    RelOptTable relOptTable = mainInput.getTable();
                    List<String> tableName = relOptTable.getQualifiedName();
                    List<String> newTableName = new ArrayList<>(tableName);
                    newTableName.set(newTableName.size() - 1, tJoinInfo.getMainAuxTableName());
                    RelOptSchema relOptSchema = relOptTable.getRelOptSchema();
                    return relOptSchema.getTableForMember(newTableName);
                }
            }
        }
        return null;
    }

    private RexNode makeMainAuxJoinCondition() {
        JoinInfo newJoinInfo = JoinInfo.of(
                removeMainAuxJoinKey(joinInfo.leftKeys),
                removeMainAuxJoinKey(joinInfo.rightKeys));
        if (newJoinInfo.leftKeys.isEmpty()) {
            return null;
        }
        else {
            return newJoinInfo.getEquiCondition(leftInput, rightInput, builder);
        }
    }

    private ImmutableIntList removeMainAuxJoinKey(ImmutableIntList keys) {
        List<Integer> newKeys = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); ++i) {
            if (i == mainAuxCondIndex) {
                continue;
            }
            newKeys.add(keys.get(i));
        }
        return ImmutableIntList.copyOf(newKeys);
    }

    private RelOptTable genMergedTable() {
        RelOptTable mergedTable = checkMainAuxJoinRule(
                joinInfo.leftKeys,
                joinInfo.rightKeys,
                leftTable,
                rightTable,
                leftProjects,
                rightProjects,
                leftInput,
                rightInput,
                rightCond);

        if (null == mergedTable) {
            mergedTable = checkMainAuxJoinRule(
                    joinInfo.rightKeys,
                    joinInfo.leftKeys,
                    rightTable,
                    leftTable,
                    rightProjects,
                    leftProjects,
                    rightInput,
                    leftInput,
                    leftCond);
            isLeftMain = false;
        }
        if (null == mergedTable) {
            logger.debug("miss match MainAuxJoinOpt: mergedTable from {} and {} not exist",
                    leftTable.getTableName(), rightTable.getTableName());
        }
        return mergedTable;
    }
    private RelOptTable checkMainAuxJoinRule(
            ImmutableIntList mainKeys,
            ImmutableIntList auxKeys,
            Table mainTable,
            Table auxTable,
            List<RexNode> mainProjects,
            List<RexNode> auxProjects,
            IquanTableScanBase mainInput,
            IquanTableScanBase auxInput,
            RexNode auxCond)
    {
        RelOptTable relOptTable = checkMainAuxJoin(
                mainKeys,
                auxKeys,
                mainProjects,
                auxProjects,
                mainTable,
                auxTable,
                mainInput);
        if (null == relOptTable){
            return null;
        }
        if (!(checkAuxHasAllAttribute(auxTable, auxInput, auxCond) && checkJoinAllAttribute())) {
            return null;
        }
        return relOptTable;
    }

    private boolean checkAuxHasAllAttribute(Table table, IquanTableScanBase input, RexNode condition) {
        List<IquanRelNode> pushDownOps = input.getPushDownOps();
        if (pushDownOps.size() > 1) {
            logger.debug("miss match MainAuxJoinOpt: pushDownOps size > 1");
            return false;
        }

        try {
            RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
                @Override
                public Void visitInputRef(RexInputRef inputRef) {
                    if (!(table.getFieldMetaList().get(inputRef.getIndex()).isAttribute)) {
                        throw new Util.FoundOne(null);
                    }
                    return null;
                }
            };
            if (condition != null) {
                condition.accept(visitor);
            }
            return true;
        } catch (Util.FoundOne e) {
            logger.debug("miss match MainAuxJoinOpt: auxTable: {} has non-attribute field", table.getTableName());
            return false;
        }
    }

    private boolean checkJoinAllAttribute() {
        int leftCnt = leftProjects.size();
        try {
            RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true){
                @Override
                public Void visitInputRef(RexInputRef inputRef) {
                    int id = inputRef.getIndex();
                    RexNode project = id < leftCnt ? leftProjects.get(id) : rightProjects.get(id - leftCnt);
                    Table table = id < leftCnt ? leftTable : rightTable;

                    RexVisitor<Void> innerVisitor = new RexVisitorImpl<Void>(true) {
                        @Override
                        public Void visitInputRef(RexInputRef inputRef1) {
                            int innerId = inputRef1.getIndex();
                            if (!table.getFieldMetaList().get(innerId).isAttribute) {
                                throw new Util.FoundOne(null);
                            }
                            return null;
                        }
                    };
                    project.accept(innerVisitor);
                    return null;
                }
            };
            if (joinCond != null) {
                joinCond.accept(visitor);
            }
            return true;
        } catch (Util.FoundOne e) {
            logger.debug("miss match MainAuxJoinOpt: join condition has non-attribute field");
            return false;
        }
    }

    public RelNode tryGenNewScanOp() {
        RelOptTable mergedTable = genMergedTable();
        if (mergedTable == null) {
            return null;
        }

        IquanTableScanBase mainInput = isLeftMain ? leftInput : rightInput;
        IquanTableScanBase newScan = new IquanTableScanOp(
                mainInput.getCluster(), mainInput.getTraitSet(), mainInput.getHints(), mergedTable);
        final int leftCnt = leftTable.getFields().size();
        final int rightCnt = rightTable.getFields().size();
        final int leftInc = isLeftMain ? 0 : rightCnt - rightTable.getSubTablesCnt();
        final int rightInc = isLeftMain ? leftCnt - leftTable.getSubTablesCnt() : 0;
        final int leftLimit = leftCnt - leftTable.getSubTablesCnt();
        final int rightLimit = rightCnt - rightTable.getSubTablesCnt();

        List<RexNode> newProjects = new ArrayList<>();
        addNewProject(leftProjects, newProjects, newScan, leftInc, leftLimit);
        addNewProject(rightProjects, newProjects, newScan, rightInc, rightLimit);

        List<RexNode> conditions = new ArrayList<>();
        RexNode rewriteCond = makeMainAuxJoinCondition();
        if (rewriteCond != null) {
            RexNode newJoinCond = rewriteCond.accept(new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    return newProjects.get(inputRef.getIndex());
                }
            });
            conditions.add(newJoinCond);
        }
        addNewCondition(conditions, leftCond, newScan, leftInc);
        addNewCondition(conditions, rightCond, newScan, rightInc);
        RexNode calcCond = mergeConditions(conditions);
        RexProgram newProgram = RexProgram.create(newScan.getRowType(), newProjects, calcCond, join.getRowType(), builder);
        IquanCalcOp newCalc = new IquanCalcOp(join.getCluster(), join.getTraitSet(), join.getHints(), newScan, newProgram);
        return newCalc;
    }

    private RexNode mergeConditions(List<RexNode> conditions) {
        switch (conditions.size()) {
            case 0:
                return null;
            case 1:
                return conditions.get(0);
            default:
                return builder.makeCall(SqlStdOperatorTable.AND, conditions);
        }
    }

    private void addNewProject(List<RexNode> oriProjects, List<RexNode> newProjects,
                               IquanTableScanBase newScan, int inc, int limit) {
        if (oriProjects == null) {
            return;
        }
        for (RexNode node : oriProjects) {
            newProjects.add(node.accept(new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    int id = inputRef.getIndex();
                    assert id < limit : "[ERROR] in mainAux Join subTable fields are not support in sql";
                    return builder.makeInputRef(newScan, inputRef.getIndex() + inc);
                }
            }));
        }
    }
    private void addNewCondition(List<RexNode> conditions, RexNode condition, IquanTableScanBase scan, int inc) {
        if (condition == null) {
            return;
        }
        RexNode cond = condition.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                return builder.makeInputRef(scan, inputRef.getIndex() + inc);}});

        conditions.add(cond);
    }

    public static RelNode tryMainAuxOpt(IquanJoinOp join, Pair<RelHint, Boolean> validHintInfo) {
        RelHint validHint = validHintInfo.getValue0();
        if (validHint != null) {
            if (ConstantDefine.LOOKUP_JOIN_HINT.equals(validHint.hintName)) {
                logger.debug("force lookup join, disable main aux opt");
                return null;
            }
            if (ConstantDefine.HASH_JOIN_HINT.equals(validHint.hintName)) {
                logger.debug("force hash join, disable main aux opt");
                return null;
            }
        }
        MainAuxOpt opt = new MainAuxOpt(join);
        return opt.tryGenNewScanOp();
    }
}
