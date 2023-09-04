package com.taobao.search.iquan.core.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.Math.max;

public class Range implements Comparable<Range>{
    private Long left;
    private Long right;

    public Range(Long first, Long right) {
        this.left = first;
        this.right = right;
    }

    public Range(Range other) {
        this.left = other.left;
        this.right = other.right;
    }

    public Long getLeft() {
        return left;
    }

    public void setLeft(Long left) {
        this.left = left;
    }

    public Long getRight() {
        return right;
    }

    public void setRight(Long right) {
        this.right = right;
    }

    public List<Long> toList() {
        List<Long> ret = new ArrayList<>();
        ret.add(left);
        ret.add(right);
        return ret;
    }


    @Override
    public int compareTo(Range o) {
        return left.equals(o.left) ? right.compareTo(o.right) : left.compareTo(o.left);
    }

    private static List<Range> trim(List<Range> ranges) {
        if (ranges == null || ranges.size() < 2) {
            return ranges;
        }
        List<Range> newRanges = new ArrayList<>();
        newRanges.add(new Range(ranges.get(0)));
        for(int i = 1; i < ranges.size(); ++i) {
            Range cur = ranges.get(i);
            Range last = newRanges.get(newRanges.size() - 1);
            if (last.right < cur.left) {
                newRanges.add(cur);
            } else {
                last.right = max(cur.right, last.right);
            }
        }
        return newRanges;
    }

    public static List<Range> getUnion(List<Range> lhs, List<Range> rhs) {
        List<Range> res = new ArrayList<>();
        res.addAll(lhs);
        res.addAll(rhs);
        Collections.sort(res);
        return trim(res);
    }

    public static List<Range> getIntersection(List<Range> lhs, List<Range> rhs) {
        List<Range> all = new ArrayList<>();
        if (lhs == null || rhs == null || lhs.isEmpty() || rhs.isEmpty()) {
            return all;
        }

        all.addAll(lhs);
        all.addAll(rhs);
        Collections.sort(all);
        List<Range> res = new ArrayList<>();
        Long x = all.get(0).left;
        Long y = all.get(0).right;
        for (int i = 1; i < all.size(); ++i) {
            Range curRange = all.get(i);
            if (y < curRange.left) {
                x = curRange.left;
                y = curRange.right;
            } else {
                if (y < curRange.right) {
                    res.add(new Range(curRange.left, y));
                    x = y;
                    y = curRange.right;
                } else {
                    res.add(new Range(curRange.left, curRange.right));
                    x = curRange.right;
                }
            }
        }
        return res;
    }
}
