# -*- coding: utf-8 -*-

class RangeUtil(object):
    def __init__(self):
        pass

    @staticmethod
    def splitRange(partitionCount, rangeFrom=0, rangeTo=65535):
        assert rangeTo >= rangeFrom, "rangeFrom[%d] < rangeTo[%d]" % (rangeFrom, rangeTo)
        if partitionCount == 0:
            return []
        rangeCount = rangeTo - rangeFrom + 1
        c = rangeCount / partitionCount
        m = rangeCount % partitionCount
        ranges = []
        f = rangeFrom
        i = 0
        while (i < partitionCount and f <= rangeTo):
            t = f + c - 1
            if i < m:
                t += 1
            ranges.append((f, t))
            f = t + 1
            i = i + 1
        return ranges

