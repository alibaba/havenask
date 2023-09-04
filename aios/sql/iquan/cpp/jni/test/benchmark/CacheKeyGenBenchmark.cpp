#include "gtest/gtest.h"
#include <benchmark/benchmark.h>

#include "iquan/common/Utils.h"
#include "iquan/jni/IquanDqlRequest.h"

using namespace std;

namespace iquan {
class CacheKeyGenBenchmark : public benchmark::Fixture {
public:
    CacheKeyGenBenchmark() {
        _request.defaultCatalogName = "default";
        _request.defaultDatabaseName = "biz_order_seller_online";
        _request.sqls.push_back(
            "SELECT "
            "biz_order_id,out_order_id,seller_nick,display_nick,buyer_nick,seller_id,buyer_id,"
            "auction_id,auction_title,auction_price,buy_amount,biz_type,sub_biz_type,fail_reason,"
            "pay_status,logistics_status,out_trade_status,snap_path,gmt_create,status,buyer_rate_"
            "status,seller_rate_status,auction_pict_url,seller_memo,buyer_memo,seller_flag,buyer_"
            "flag,buyer_message_path,refund_status,attributes,attributes_cc,gmt_modified,ip,end_"
            "time,pay_time,is_main,is_detail,point_rate,parent_id,adjust_fee,discount_fee,refund_"
            "fee,confirm_paid_fee,cod_status,trade_tag,shop_id,options,ignore_sold_quantity,from_"
            "group FROM biz_order_seller_online_summary_ WHERE contain(biz_order_id,?) and "
            "contain(seller_id,?)");
        _request.sqlParams["timeout"] = string("3000");
        _request.sqlParams["formatType"] = string("flatbuffers");
        _request.sqlParams["forbitMergeSearchInfo"] = string("true");
        _request.sqlParams["iquan.optimizer.debug.enable"] = string("false");
        _request.sqlParams["exec.source.spec"] = string("querySummaryByIndex");
        _request.sqlParams["trace_id"] = string("213e210216788444920236417ec8e8");
        _request.cacheKeyConfig();
        _emptyRequest.cacheKeyConfig();
    }

private:
    IquanDqlRequest _emptyRequest;
    IquanDqlRequest _request;
};

BENCHMARK_F(CacheKeyGenBenchmark, TestEmptyToCacheFastCompact)(benchmark::State &state) {
    for (auto _ : state) {
        string jsonStr;
        Utils::fastToJson(_emptyRequest, jsonStr, true);
    }
}

BENCHMARK_F(CacheKeyGenBenchmark, TestEmptyToCacheFastNotCompact)(benchmark::State &state) {
    for (auto _ : state) {
        string jsonStr;
        Utils::fastToJson(_emptyRequest, jsonStr, false);
    }
}

BENCHMARK_F(CacheKeyGenBenchmark, TestEmptyToCacheLegacyCompact)(benchmark::State &state) {
    for (auto _ : state) {
        string jsonStr;
        Utils::toJson(_emptyRequest, jsonStr, true);
    }
}

BENCHMARK_F(CacheKeyGenBenchmark, TestEmptyToCacheLegacyNotCompact)(benchmark::State &state) {
    for (auto _ : state) {
        string jsonStr;
        Utils::toJson(_emptyRequest, jsonStr, false);
    }
}

BENCHMARK_F(CacheKeyGenBenchmark, TestToCacheFastCompact)(benchmark::State &state) {
    for (auto _ : state) {
        string jsonStr;
        Utils::fastToJson(_request, jsonStr, true);
    }
}

BENCHMARK_F(CacheKeyGenBenchmark, TestToCacheFastNotCompact)(benchmark::State &state) {
    for (auto _ : state) {
        string jsonStr;
        Utils::fastToJson(_request, jsonStr, false);
    }
}

BENCHMARK_F(CacheKeyGenBenchmark, TestToCacheLegacyCompact)(benchmark::State &state) {
    for (auto _ : state) {
        string jsonStr;
        Utils::toJson(_request, jsonStr, true);
    }
}

BENCHMARK_F(CacheKeyGenBenchmark, TestToCacheLegacyNotCompact)(benchmark::State &state) {
    for (auto _ : state) {
        string jsonStr;
        Utils::toJson(_request, jsonStr, false);
    }
}

} // namespace iquan
