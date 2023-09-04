#include "sql/ops/sort/SortInitParam.h"

#include <engine/NaviConfigContext.h>
#include <map>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/fast_jsonizable_dec.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "navi/config/NaviConfig.h"
#include "unittest/unittest.h"

using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class SortInitParamTest : public TESTBASE {};

TEST_F(SortInitParamTest, testInitFromJson) {
    {
        autil::legacy::json::JsonMap attrs;
        autil::legacy::RapidDocument jsonAttrs;
        navi::NaviConfig::parseToDocument(autil::legacy::FastToJsonString(attrs), jsonAttrs);
        navi::NaviConfigContext ctx("", &jsonAttrs, nullptr);

        SortInitParam param;
        try {
            param.initFromJson(ctx);
            ASSERT_TRUE(false);
        } catch (const autil::legacy::ExceptionBase &e) {}
    }
    {
        autil::legacy::json::JsonMap attrs;
        attrs["directions"] = ParseJson(R"json([])json");
        ;
        attrs["limit"] = Any(1);
        attrs["offset"] = Any(0);
        autil::legacy::RapidDocument jsonAttrs;
        navi::NaviConfig::parseToDocument(autil::legacy::FastToJsonString(attrs), jsonAttrs);
        navi::NaviConfigContext ctx("", &jsonAttrs, nullptr);

        SortInitParam param;
        try {
            param.initFromJson(ctx);
            ASSERT_TRUE(false);
        } catch (const autil::legacy::ExceptionBase &e) {}
    }
    {
        autil::legacy::json::JsonMap attrs;
        attrs["order_fields"] = ParseJson(R"json(["a"])json");
        ;
        attrs["limit"] = Any(1);
        attrs["offset"] = Any(0);
        autil::legacy::RapidDocument jsonAttrs;
        navi::NaviConfig::parseToDocument(autil::legacy::FastToJsonString(attrs), jsonAttrs);
        navi::NaviConfigContext ctx("", &jsonAttrs, nullptr);

        SortInitParam param;
        try {
            ASSERT_FALSE(param.initFromJson(ctx));
            ASSERT_TRUE(false);
        } catch (const autil::legacy::ExceptionBase &e) {}
    }
    {
        autil::legacy::json::JsonMap attrs;
        attrs["order_fields"] = ParseJson(R"json(["a"])json");
        ;
        attrs["directions"] = ParseJson(R"json(["ASC"])json");
        ;
        attrs["offset"] = Any(0);
        autil::legacy::RapidDocument jsonAttrs;
        navi::NaviConfig::parseToDocument(autil::legacy::FastToJsonString(attrs), jsonAttrs);
        navi::NaviConfigContext ctx("", &jsonAttrs, nullptr);

        SortInitParam param;
        try {
            ASSERT_FALSE(param.initFromJson(ctx));
            ASSERT_TRUE(false);
        } catch (const autil::legacy::ExceptionBase &e) {}
    }
    {
        autil::legacy::json::JsonMap attrs;
        attrs["order_fields"] = ParseJson(R"json(["a"])json");
        ;
        attrs["directions"] = ParseJson(R"json(["ASC"])json");
        ;
        attrs["limit"] = Any(1);
        autil::legacy::RapidDocument jsonAttrs;
        navi::NaviConfig::parseToDocument(autil::legacy::FastToJsonString(attrs), jsonAttrs);
        navi::NaviConfigContext ctx("", &jsonAttrs, nullptr);

        SortInitParam param;
        try {
            ASSERT_TRUE(param.initFromJson(ctx));
            ASSERT_TRUE(false);
        } catch (const autil::legacy::ExceptionBase &e) {}
    }
    {
        autil::legacy::json::JsonMap attrs;
        attrs["order_fields"] = ParseJson(R"json(["a"])json");
        attrs["directions"] = ParseJson(R"json([])json");
        attrs["limit"] = Any(1);
        attrs["offset"] = Any(0);
        autil::legacy::RapidDocument jsonAttrs;
        navi::NaviConfig::parseToDocument(autil::legacy::FastToJsonString(attrs), jsonAttrs);
        navi::NaviConfigContext ctx("", &jsonAttrs, nullptr);

        SortInitParam param;
        try {
            ASSERT_FALSE(param.initFromJson(ctx));
        } catch (const autil::legacy::ExceptionBase &e) { ASSERT_TRUE(false); }
    }
    {
        autil::legacy::json::JsonMap attrs;
        attrs["order_fields"] = ParseJson(R"json(["a"])json");
        attrs["directions"] = ParseJson(R"json(["ASC"])json");
        attrs["limit"] = Any(1);
        attrs["offset"] = Any(0);
        autil::legacy::RapidDocument jsonAttrs;
        navi::NaviConfig::parseToDocument(autil::legacy::FastToJsonString(attrs), jsonAttrs);
        navi::NaviConfigContext ctx("", &jsonAttrs, nullptr);

        SortInitParam param;
        try {
            ASSERT_TRUE(param.initFromJson(ctx));
        } catch (const autil::legacy::ExceptionBase &e) { ASSERT_TRUE(false); }
    }
}

} // namespace sql
