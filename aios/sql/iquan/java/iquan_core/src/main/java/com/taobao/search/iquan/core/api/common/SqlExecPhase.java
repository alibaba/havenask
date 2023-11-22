package com.taobao.search.iquan.core.api.common;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;

public enum SqlExecPhase {
    START(0, "start"),
    SQL_PARSE(1, "sql.parse"),
    SQL_CATALOG_REGISTER(2, "sql.catalog.register"),
    SQL_VALIDATE(3, "sql.validate"),
    REL_TRANSFORM(4, "rel.transform"),
    REL_OPTIMIZE(5, "rel.optimize"),
    REL_POST_OPTIMIZE(6, "rel.post.optimize"),
    JNI_POST_OPTIMIZE(7, "jni.post.optimize"),
    END(8, "end");

    final private int step;
    final private String name;

    SqlExecPhase(int step, String name) {
        this.step = step;
        this.name = name;
    }

    //
    public static SqlExecPhase from(String phase) {
        phase = phase.toLowerCase();
        if (phase.equals(START.getName())) {
            return START;
        } else if (phase.equals(SQL_PARSE.getName())) {
            return SQL_PARSE;
        } else if (phase.equals(SQL_CATALOG_REGISTER.getName())) {
            return SQL_CATALOG_REGISTER;
        } else if (phase.equals(SQL_VALIDATE.getName())) {
            return SQL_VALIDATE;
        } else if (phase.equals(REL_TRANSFORM.getName())) {
            return REL_TRANSFORM;
        } else if (phase.equals(REL_OPTIMIZE.getName())) {
            return REL_OPTIMIZE;
        } else if (phase.equals(REL_POST_OPTIMIZE.getName())) {
            return REL_POST_OPTIMIZE;
        } else if (phase.equals(END.getName())) {
            return END;
        } else if (phase.equals(JNI_POST_OPTIMIZE.getName())) {
            return JNI_POST_OPTIMIZE;
        } else if (phase.equals("none")) {
            return END;
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_INVALID_EXEC_PHASE,
                    String.format("phase is %s.", phase));
        }
    }

    public static boolean needExec(SqlExecPhase currentExecPhase, SqlExecPhase expectExecPhase) {
        if (currentExecPhase.getStep() >= expectExecPhase.getStep()) {
            return true;
        } else {
            return false;
        }
    }

    public int getStep() {
        return step;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getName();
    }

    //
    public static class PlanPrepareLevel {
        private final static Set<SqlExecPhase> validPhases = new HashSet<>(
                ImmutableList.of(
                        SqlExecPhase.SQL_PARSE,
                        SqlExecPhase.REL_TRANSFORM,
                        SqlExecPhase.REL_POST_OPTIMIZE,
                        SqlExecPhase.JNI_POST_OPTIMIZE,
                        SqlExecPhase.END
                )
        );
        private final SqlExecPhase phase;

        public PlanPrepareLevel(SqlExecPhase phase) {
            this.phase = phase;

            if (!isValid()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_INVALID_INPUT_PARAMS, "prepare level is not valid.");
            }
        }

        public static Set<SqlExecPhase> getValidPhases() {
            return validPhases;
        }

        private boolean isValid() {
            if (validPhases.contains(phase)) {
                return true;
            }
            return false;
        }

        public SqlExecPhase getPhase() {
            return phase;
        }
    }

    //
    public static class PlanExecPhaseResult {
        private final static Set<SqlExecPhase> validPhases = new HashSet<>(
                ImmutableList.of(
                        SqlExecPhase.SQL_PARSE,
                        SqlExecPhase.SQL_VALIDATE,
                        SqlExecPhase.REL_TRANSFORM,
                        SqlExecPhase.REL_OPTIMIZE,
                        SqlExecPhase.REL_POST_OPTIMIZE,
                        SqlExecPhase.JNI_POST_OPTIMIZE,
                        SqlExecPhase.END
                )
        );
        private final SqlExecPhase phase;

        public PlanExecPhaseResult(SqlExecPhase phase) {
            this.phase = phase;

            if (!isValid()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_INVALID_INPUT_PARAMS, "exec phase result is not valid.");
            }
        }

        public static Set<SqlExecPhase> getValidPhases() {
            return validPhases;
        }

        private boolean isValid() {
            if (validPhases.contains(phase)) {
                return true;
            }
            return false;
        }

        public SqlExecPhase getPhase() {
            return phase;
        }
    }
}
