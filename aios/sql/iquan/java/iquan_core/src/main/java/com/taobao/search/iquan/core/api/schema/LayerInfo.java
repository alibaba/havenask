package com.taobao.search.iquan.core.api.schema;

import com.taobao.search.iquan.core.common.Range;

import java.util.ArrayList;
import java.util.List;


public class LayerInfo {
    private List<Range> intRangeValue = new ArrayList<>();
    private List<String> stringDiscreteValue = new ArrayList<>();
    private List<Long> intDiscreteValue = new ArrayList<>();

    public List<Range> getIntRangeValue() {
        return intRangeValue;
    }

    public void setIntRangeValue(List<Range> intRangeValue) {
        this.intRangeValue = intRangeValue;
    }

    public List<String> getStringDiscreteValue() {
        return stringDiscreteValue;
    }

    public void setStringDiscreteValue(List<String> stringDiscreteValue) {
        this.stringDiscreteValue = stringDiscreteValue;
    }

    public List<Long> getIntDiscreteValue() {
        return intDiscreteValue;
    }

    public void setIntDiscreteValue(List<Long> intDiscreteValue) {
        this.intDiscreteValue = intDiscreteValue;
    }
}
