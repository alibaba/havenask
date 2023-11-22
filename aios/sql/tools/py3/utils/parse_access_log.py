#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys
import json
from urllib.parse import unquote

DEFAULT_PARAM_VALUE = 'Other'


def parse_dynamic_params(kvpair):
    if 'urlencode_data:true' in kvpair:
        left = kvpair.find('dynamic_params:') + len('dynamic_params:')
        right = kvpair.find(';', left)
        value = unquote(kvpair[left:right])
    else:
        left = kvpair.find('dynamic_params:[[') + len('dynamic_params:')
        right = kvpair.find(']];', left) + len(']]')
        value = kvpair[left:right]
        value = value.replace('\\\\', '\\')  # ref aone #48012004
    return json.loads(value)


def parse_param_from_log(record, pattern):
    left = record.find('query=[') + len('query=[')
    right = record.find('run_info=')
    query = record[left:right].strip()[:-1]
    sql, kvpair = query.split('&&kvpair=')
    dynamic_params = parse_dynamic_params(kvpair)
    end = sql.rfind(pattern)
    if end == -1:
        return DEFAULT_PARAM_VALUE
    return dynamic_params[0][sql[:end].count('?')]


def main():
    parser = argparse.ArgumentParser(description='''cat sql_access.log | ./%(prog)s 'contain(seller_id,' ''')
    parser.add_argument('pattern')
    args = parser.parse_args()
    for line in sys.stdin:
        try:
            param = parse_param_from_log(line, args.pattern)
        except BaseException:
            param = DEFAULT_PARAM_VALUE
        sys.stdout.write(str(param))
        sys.stdout.write('\t')
        sys.stdout.write(line)


def run_case():
    record = '''[2023-05-23 11:41:14.438256] [INFO] [253003,aios/ha3/ha3/sql/framework/SqlAccessLog.cpp -- log():66] [http 33.8.22.122:38738       80       0    928     683896us        80us    2150420b16848132737354324e5b02             1    query=[query=SELECT biz_order_id FROM biz_order_seller_online WHERE  contain(is_main,?) and contain(biz_type, ?) and contain(seller_id, ?) and contain(sub_biz_type, ?) and contain(status, ?) and QUERY('gmt_modified',?)  ORDER BY seller_id desc, gmt_modified desc,biz_order_id desc LIMIT 80 OFFSET 4080 &&kvpair=timeout:3000;formatType:flatbuffers;forbitMergeSearchInfo:true;iquan.optimizer.debug.enable:false;exec.source.spec:getOrdersByQuery;trace_id:2150420b16848132737354324e5b02;dynamic_params:[["1", "200|300|100|500|10000|2000|2700", "179917267", "1|901|801|701|601|2", "0", "[1678524916,1678611316]"]];databaseName:biz_order_seller_online]          run_info=[runSqlTimeInfo { sqlPlanStartTime: 1684813273754577 sqlPlanTime: 4055 sqlPlan2GraphTime: 227 sqlRunGraphTime: 683771 sqlRunForkGraphBeginTime: 1684813273758977 sqlRunForkGraphEndTime: 1684813274438070 sqlRunForkGraphTime: 679093 sqlExecQueueUs: 1241 sqlExecComputeUs: 13747 } scanInfos { kernelName: "ScanKernel" nodeName: "0" tableName: "biz_order_seller_online" hashKey: 10000 parallelNum: 1 totalOutputCount: 33193 totalScanCount: 52985 totalUseTime: 961397 totalSeekTime: 677115 totalEvaluateTime: 229858 totalOutputTime: 961 totalComputeTimes: 6 totalInitTime: 53412 totalSeekCount: 33193 mergeCount: 5 durationTime: 962066 queryPoolSize: 6895224 } calcInfos { kernelName: "CalcKernel" nodeName: "4" hashKey: 10004 totalUseTime: 26 totalOutputCount: 80 totalComputeTimes: 1 } sortInfos { kernelName: "SortKernel" nodeName: "1" hashKey: 10001 totalUseTime: 6351 totalMergeTime: 23 totalTopKTime: 6321 totalCompactTime: 4 totalOutputTime: 31290 totalComputeTimes: 5 mergeCount: 4 } sortInfos { kernelName: "SortKernel" nodeName: "3" hashKey: 10003 totalUseTime: 5569 totalMergeTime: 4 totalTopKTime: 5563 totalCompactTime: 1 totalOutputTime: 487 totalComputeTimes: 1 }]          rpc_info=[(qrs.default_sql(-1),biz_order_seller_online.default_sql):{part:75 durUs:57555 SS:EOF RS:EOF SPEC:33.69.252.34:21604 SC:1 RC:1 SBT:1684813273759688 SET:1684813273759709 RBT:1684813273817243 RET:1684813273817243 Retry:0} {part:71 durUs:68304 SS:EOF RS:EOF SPEC:33.69.206.229:21604 SC:1 RC:1 SBT:1684813273759486 SET:1684813273759513 RBT:1684813273827790 RET:1684813273827790 Retry:0} {part:72 durUs:85738 SS:EOF RS:EOF SPEC:33.54.151.111:21604 SC:1 RC:1 SBT:1684813273759535 SET:1684813273759550 RBT:1684813273845273 RET:1684813273845273 Retry:0} {part:74 durUs:94802 SS:EOF RS:EOF SPEC:33.55.148.56:21604 SC:1 RC:1 SBT:1684813273759638 SET:1684813273759655 RBT:1684813273854440 RET:1684813273854440 Retry:0} {part:73 durUs:95557 SS:EOF RS:EOF SPEC:33.54.143.39:21604 SC:1 RC:1 SBT:1684813273759586 SET:1684813273759602 RBT:1684813273855143 RET:1684813273855143 Retry:0} {part:70 durUs:670187 SS:EOF RS :EOF SPEC:33.54.224.27:21604 SC:1 RC:1 SBT:1684813273759423 SET:1684813273759446 RBT:1684813274429610 RET:1684813274429610 Retry:0};]]'''
    assert (parse_param_from_log(record, 'contain(seller_id, ?)') == '179917267')
    assert (parse_param_from_log(record, 'seller_id,') == '179917267')

    record = '''[2023-11-01 10:00:51.781665] [INFO] [123878,aios/ha3/ha3/sql/framework/SqlAccessLog.cpp -- log():66] [http 33.60.140.167:40002      1       0    288     35060us         53us    2150489b16988040516708466e12a8             1    query=[query=SELECT count(*) FROM (select biz_order_id from biz_order_seller_online where contain(biz_type, ?) and contain(pay_status, ?) and BITAND(options,?)<>? and contain(logistics_status, ?) and contain(seller_id, ?) and BITAND(options,?) = 0 and contain(status, ?) and contain(from_group, ?) and QUERY('auction_title', ?)  order by seller_id desc,gmt_create desc limit 150) &&kvpair=timeout:3000;formatType:flatbuffers;forbitMergeSearchInfo:true;iquan.optimizer.debug.enable:false;exec.source.spec:getBizOrderCountByQuery;trace_id:2150489b16988040516708466e12a8;dynamic_params:[["200|300|9999|100|500|700|710|1000|1001|1300|150|1200|1201|1500|1550|1560|1570|1600|1650|2000|1900|2200|2300|1110|1700|760|2300|10000|2400|2410|1102|6800|6810|2600|3000|2700|1670|3200|2900|2360|3400|3450|3460|3470|3475|2950|2970|3800|3900|6868|1580|4000|62001|8012|3300", "2", 2251799813685248, 2251799813685248, "1|8|6", "666363096", 166914695549157408, "0", "0", "\\\\"9T51K29026\\\\""]];databaseName:biz_order_seller_online]        run_info=[runSqlTimeInfo { sqlPlanStartTime: 1698804051746922 sqlPlanTime: 103 sqlPlan2GraphTime: 289 sqlRunGraphTime: 34969 sqlRunForkGraphBeginTime: 1698804051747450 sqlRunForkGraphEndTime: 1698804051781524 sqlRunForkGraphTime: 34074 sqlExecQueueUs: 1796 sqlExecComputeUs: 1824 } scanInfos { kernelName: "ScanKernel" nodeName: "0" tableName: "biz_order_seller_online" hashKey: 10000 parallelNum: 1 totalOutputCount: 2 totalScanCount: 6 totalUseTime: 141445 totalSeekTime: 129042 totalEvaluateTime: 36 totalOutputTime: 271 totalComputeTimes: 6 totalInitTime: 11214 totalSeekCount: 2 mergeCount: 5 durationTime: 141532 queryPoolSize: 74283104 } aggInfos { kernelName: "AggKernel" nodeName: "4" hashKey: 10004 totalUseTime: 14 totalOutputCount: 1 totalComputeTimes: 1 collectTime: 1 outputResultTime: 1 aggPoolSize: 72 totalInputCount: 2 } calcInfos { kernelName: "CalcKernel" nodeName: "3" hashKey: 10003 totalUseTime: 43 totalOutputCount: 2 totalComputeTimes: 1 totalInputCount: 2 } sortInfos { kernelName: "SortKernel" nodeName: "2" hashKey: 10002 totalUseTime: 7 totalMergeTime: 5 totalCompactTime: 1 totalOutputTime: 19 totalComputeTimes: 1 totalInputCount: 2 } invertedInfos { dictionaryInfo { } postingInfo { } indexType: "number" hashKey: 1891950960 mergeCount: 5 } invertedInfos { dictionaryInfo { } postingInfo { } indexType: "text" hashKey: 4076674828 mergeCount: 5 }]      rpc_info=[(qrs.default_sql(-1),biz_order_seller_online.default_sql):{part:165 durUs:18693 SS:EOF RS:EOF SPEC:33.67.148.69:21604 SC:1 RC:1 SBT:1698804051748091 SET:1698804051748109 RBT:1698804051766784 RET:1698804051766784 Retry:0} {part:164 durUs:19306 SS:EOF RS:EOF SPEC:33.69.65.231:21604 SC:1 RC:1 SBT:1698804051748017 SET:1698804051748035 RBT:1698804051767323 RET:1698804051767323 Retry:0} {part:167 durUs:23596 SS:EOF RS:EOF SPEC:33.55.82.168:21604 SC:1 RC:1 SBT:1698804051748147 SET:1698804051748163 RBT:1698804051771743 RET:1698804051771743 Retry:0} {part:162 durUs:24016 SS:EOF RS:EOF SPEC:33.55.69.206:21604 SC:1 RC:1 SBT:1698804051747907 SET:1698804051747931 RBT:1698804051771923 RET:1698804051771923 Retry:0} {part:166 durUs:27414 SS:EOF RS:EOF SPEC:33.69.106.203:21604 SC:1 RC:1 SBT:1698804051748114 SET:1698804051748141 RBT:1698804051775527 RET:1698804051775528 Retry:0} {part:163 durUs:33207 SS:EOF RS:EOF SPEC:33.69.191.31:21604 SC:1 RC:1 SBT:1698804051747967 SET:1698804051747988 RBT:1698804051781174 RET:1698804051781174 Retry:0};]]'''
    assert (parse_param_from_log(record, 'contain(seller_id, ?)') == '666363096')

    kvpair = '''databaseName:biz_order_seller_online;dynamic_params:%5B%5B1684303791%2C%20%221%22%2C%20%22200%7C300%7C100%7C500%7C10000%7C2700%22%2C%20%222%22%2C%204194304%2C%204194304%2C%2018014398509481984%2C%2018014398509481984%2C%20%228%7C1%7C6%22%2C%20%229223370015425200933%22%2C%20%220%22%2C%201676614191%2C%201684303791%2C%20%225%22%5D%5D;trace_id:21057b6916843037916586649e03dd;iquan.optimizer.debug.enable:false;forbitMergeSearchInfo:true;formatType:string;urlencode_data:true;timeout:10000'''
    assert (parse_dynamic_params(kvpair) == [[1684303791,
                                              '1',
                                              '200|300|100|500|10000|2700',
                                              '2',
                                              4194304,
                                              4194304,
                                              18014398509481984,
                                              18014398509481984,
                                              '8|1|6',
                                              '9223370015425200933',
                                              '0',
                                              1676614191,
                                              1684303791,
                                              '5']])


if __name__ == '__main__':
    run_case()
    main()
