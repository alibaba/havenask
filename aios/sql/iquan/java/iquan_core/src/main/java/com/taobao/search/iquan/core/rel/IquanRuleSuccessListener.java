package com.taobao.search.iquan.core.rel;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.utils.IquanRelOptUtils;

public class IquanRuleSuccessListener extends IquanRuleListener {
    private final List<RuleProductionEvent> successEvents = new ArrayList<>();
    private final List<String> successLog = new ArrayList<>();
    private boolean head = true;


    public List<RuleProductionEvent> getSuccessEvents() {
        return successEvents;
    }

    @Override
    public String display() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < successLog.size(); ++i) {
            RuleProductionEvent event = successEvents.get(i);
            builder.append(event.isBefore() ? "Before" : "After").append(" apply Rule [");
            builder.append(event.getRuleCall().rule).append("]:\n");
            builder.append(successLog.get(i)).append("\n");
        }
        return builder.toString();
    }

    @Override
    public void relEquivalenceFound(RelEquivalenceEvent event) {

    }

    @Override
    public void ruleAttempted(RuleAttemptedEvent event) {

    }

    @Override
    public void ruleProductionSucceeded(RuleProductionEvent event) {
        if (!event.isBefore() || head) {
            head = false;
            successEvents.add(event);
            successLog.add(IquanRelOptUtils.relToString(event.getRuleCall().getPlanner().getRoot()));
        }
    }

    @Override
    public void relDiscarded(RelDiscardedEvent event) {

    }

    @Override
    public void relChosen(RelChosenEvent event) {

    }
}
