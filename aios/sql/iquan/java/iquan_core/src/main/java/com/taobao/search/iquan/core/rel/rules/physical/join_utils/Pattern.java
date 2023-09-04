package com.taobao.search.iquan.core.rel.rules.physical.join_utils;

import java.util.Objects;

public class Pattern {
    private boolean leftHasIndex;
    private boolean rightHasIndex;
    private boolean leftIsScannable;
    private boolean rightIsScannable;
    private boolean leftBigger;

    public Pattern(boolean leftHasIndex, boolean rightHasIndex,
                   boolean leftIsScannable, boolean rightIsScannable,
                   boolean leftBigger) {
        this.leftHasIndex = leftHasIndex;
        this.rightHasIndex = rightHasIndex;
        this.leftIsScannable = leftIsScannable;
        this.rightIsScannable = rightIsScannable;
        this.leftBigger = leftBigger;
    }

    public Pattern(JsonPattern jsonPattern) {
        this.leftHasIndex = jsonPattern.leftHasIndex;
        this.rightHasIndex = jsonPattern.rightHasIndex;
        this.leftIsScannable = jsonPattern.leftIsScannable;
        this.rightIsScannable = jsonPattern.rightIsScannable;
        this.leftBigger = jsonPattern.leftBigger;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Pattern)) {
            return false;
        }
        Pattern other = (Pattern) o;
        return leftHasIndex == other.leftHasIndex
                && rightHasIndex == other.rightHasIndex
                && leftIsScannable == other.leftIsScannable
                && rightIsScannable == other.rightIsScannable
                && leftBigger == other.leftBigger;
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftHasIndex, rightHasIndex, leftIsScannable, rightIsScannable, leftBigger);
    }

    @Override
    public Object clone() {
        return new Pattern(leftHasIndex, rightHasIndex, leftIsScannable, rightIsScannable, leftBigger);
    }

    @Override
    public String toString() {
        return "leftHasIndex: " + leftHasIndex
                + ", rightHasIndex: " + rightHasIndex
                + ", leftIsScannable: " + leftIsScannable
                + ", rightIsScannable: " + rightIsScannable
                + ", leftBigger: " + leftBigger
                ;
    }

    public boolean isLeftHasIndex() {
        return leftHasIndex;
    }

    public void setLeftHasIndex(boolean leftHasIndex) {
        this.leftHasIndex = leftHasIndex;
    }

    public boolean isRightHasIndex() {
        return rightHasIndex;
    }

    public void setRightHasIndex(boolean rightHasIndex) {
        this.rightHasIndex = rightHasIndex;
    }

    public boolean leftIsScannable() {
        return leftIsScannable;
    }

    public void setLeftIsScannable(boolean leftIsScannable) {
        this.leftIsScannable = leftIsScannable;
    }

    public boolean rightIsScannable() {
        return rightIsScannable;
    }

    public void setRightIsScannable(boolean rightIsScannable) {
        this.rightIsScannable = rightIsScannable;
    }

    public boolean isLeftBigger() {
        return leftBigger;
    }

    public void setLeftBigger(boolean leftBigger) {
        this.leftBigger = leftBigger;
    }
}
