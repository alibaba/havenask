/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ha3/sql/ops/turbojetCalc/IquanPlanImporter.h"

#include <numeric>

#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/condition/ExprVisitor.h"
#include "ha3/sql/ops/condition/SqlJsonUtil.h"
#include "rapidjson/rapidjson.h"
#include "table/ValueTypeSwitch.h"
#include "turbojet/storage/matchdocs/Prelude.h"

using namespace std;
using namespace turbojet::expr;

namespace isearch::sql::turbojet {

namespace {
using SyntaxOp = std::function<Result<SyntaxNodePtr>(vector<SyntaxNodePtr> &&)>;

template <Scanner::token_type Op>
Result<SyntaxNodePtr> BinaryOp(vector<SyntaxNodePtr> &&args) {
    AR_REQUIRE_TRUE(args.size() >= 2,
                    RuntimeError::make("expected [%d] args, got [%lu]", 2, args.size()));
    auto node = make_shared<BinarySyntaxNode>(Op, args[0], args[1]);
    for (int i = 2; i < args.size(); ++i) {
        node = make_shared<BinarySyntaxNode>(Op, node, args[i]);
    }
    return node;
}

template <Scanner::token_type Op>
Result<SyntaxNodePtr> UnaryOp(vector<SyntaxNodePtr> &&args) {
    AR_REQUIRE_TRUE(args.size() == 1,
                    RuntimeError::make("expected [%d] args, got [%lu]", 1, args.size()));
    return make_shared<UnarySyntaxNode>(Op, args[0]);
}

static map<string, SyntaxOp> opMap = {
    /* relation */
    {SQL_LT_OP, BinaryOp<Scanner::token::LT>},
    {SQL_LE_OP, BinaryOp<Scanner::token::LE>},
    {SQL_GT_OP, BinaryOp<Scanner::token::GT>},
    {SQL_GE_OP, BinaryOp<Scanner::token::GE>},
    /* equality */
    {SQL_EQUAL_OP, BinaryOp<Scanner::token::EQ>},
    {SQL_NOT_EQUAL_OP, BinaryOp<Scanner::token::NE>},
    {HA3_NOT_EQUAL_OP, BinaryOp<Scanner::token::NE>},
    /* logical */
    {SQL_AND_OP, BinaryOp<Scanner::token::LOGICAL_AND>},
    {SQL_OR_OP, BinaryOp<Scanner::token::LOGICAL_OR>},
    {SQL_NOT_OP, UnaryOp<Scanner::token::LOGICAL_NOT>},
    /* arithmetic */
    {"+", BinaryOp<Scanner::token::ADD>},
    {"-",
     [](vector<SyntaxNodePtr> &&args) -> Result<SyntaxNodePtr> {
         if (args.size() == 1) {
             return UnaryOp<Scanner::token::SUB>(std::move(args));
         } else {
             return BinaryOp<Scanner::token::SUB>(std::move(args));
         }
     }},
    {"*", BinaryOp<Scanner::token::MUL>},
    {"/", BinaryOp<Scanner::token::DIV>},
};
} // namespace

#define DECLARE_VISIT(name)                                                                        \
    void name(const autil::SimpleValue &value) override {                                          \
        _r = name##_(value);                                                                       \
    }                                                                                              \
    Result<SyntaxNodePtr> name##_(const autil::SimpleValue &value)

class IquanPlan2SyntaxNodeVisitor : public ExprVisitor {
public:
    Result<SyntaxNodePtr> apply(const autil::SimpleValue &value) {
        AR_REQUIRE_TRUE(!value.IsNull(), RuntimeError::make("null is not supported by ha3"));
        Result<SyntaxNodePtr> r;
        std::swap(r, _r);
        visit(value);
        std::swap(r, _r);
        AR_REQUIRE_TRUE(!isError(), RuntimeError::make(errorInfo()));
        auto node = AR_RET_IF_ERR(std::move(r));
        if (!node) {
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            value.Accept(writer);
            return RuntimeError::make("node == nullptr: [%s]", buffer.GetString());
        }
        return node;
    }

    DECLARE_VISIT(visitCastUdf) {
        return AR_RET_IF_ERR(castOpImpl(value, false));
    }

    DECLARE_VISIT(visitMultiCastUdf) {
        return AR_RET_IF_ERR(castOpImpl(value, true));
    }

    Result<SyntaxNodePtr> castOpImpl(const autil::SimpleValue &value, bool multi) {
        auto &params = value[SQL_CONDITION_PARAMETER];
        AR_REQUIRE_TRUE(params.IsArray(), RuntimeError::make("parameters is not an array"));
        AR_REQUIRE_TRUE(params.Size() == 1,
                        RuntimeError::make("expected [1] args, got [%d]", (int)params.Size()));
        return AR_RET_IF_ERR(apply(params[0]));
    }

    DECLARE_VISIT(visitNormalUdf) {
        auto udf = value[SQL_CONDITION_OPERATOR].GetString();
        auto &params = value[SQL_CONDITION_PARAMETER];
        AR_REQUIRE_TRUE(params.IsArray(), RuntimeError::make("parameters is not an array"));
        vector<SyntaxNodePtr> args;
        for (rapidjson::SizeType i = 0; i < params.Size(); i++) {
            args.emplace_back(AR_RET_IF_ERR(apply(params[i])));
        }
        return make_shared<FunctionCallSyntaxNode>(udf, std::move(args));
    }

    DECLARE_VISIT(visitInOp) {
        return AR_RET_IF_ERR(inOpImpl(value, true));
    }

    DECLARE_VISIT(visitNotInOp) {
        return AR_RET_IF_ERR(inOpImpl(value, false));
    }

    Result<SyntaxNodePtr> inOpImpl(const autil::SimpleValue &value, bool in) {
        auto &params = value[SQL_CONDITION_PARAMETER];
        AR_REQUIRE_TRUE(params.IsArray(), RuntimeError::make("parameters is not an array"));
        AR_REQUIRE_TRUE(
            params.Size() >= 1,
            RuntimeError::make("expected at least [1] args, got [%d]", (int)params.Size()));
        vector<SyntaxNodePtr> args;
        for (rapidjson::SizeType i = 0; i < params.Size(); i++) {
            args.emplace_back(AR_RET_IF_ERR(apply(params[i])));
        }
        auto node = make_shared<FunctionCallSyntaxNode>("in", std::move(args));
        if (in) {
            return node;
        }
        return make_shared<UnarySyntaxNode>(Scanner::token::LOGICAL_NOT, node);
    }

    DECLARE_VISIT(visitCaseOp) {
        auto &params = value[SQL_CONDITION_PARAMETER];
        auto val = AR_RET_IF_ERR(apply(params[params.Size() - 1]));
        for (size_t i = params.Size() - 1; i > 0; i -= 2) {
            auto ci = AR_RET_IF_ERR(apply(params[i - 2]));
            auto vi = AR_RET_IF_ERR(apply(params[i - 1]));
            val = make_shared<ConditionalSyntaxNode>(ci, vi, val);
        }
        return val;
    }

    DECLARE_VISIT(visitOtherOp) {
        auto op = value[SQL_CONDITION_OPERATOR].GetString();
        auto it = opMap.find(op);
        AR_REQUIRE_TRUE(it != opMap.end(), RuntimeError::make("unknown operator: [%s]", op));
        auto &params = value[SQL_CONDITION_PARAMETER];
        AR_REQUIRE_TRUE(params.IsArray(), RuntimeError::make("parameters is not an array"));
        vector<SyntaxNodePtr> args;
        for (rapidjson::SizeType i = 0; i < params.Size(); i++) {
            args.emplace_back(AR_RET_IF_ERR(apply(params[i])));
        }
        return AR_RET_IF_ERR(it->second(std::move(args)));
    }

    DECLARE_VISIT(visitInt64) {
        bool minus = false;
        auto v = value.GetInt64();
        if (v < 0) {
            minus = true;
            v = -v;
        }
        SyntaxNodePtr node;
        if (limits::CanRepresent<int32_t>(v)) {
            node = make_shared<LiteralSyntaxNode>(v, false, 0);
        } else if (limits::CanRepresent<uint32_t>(v)) {
            node = make_shared<LiteralSyntaxNode>(v, true, 0);
        } else {
            node = make_shared<LiteralSyntaxNode>(v, false, 1);
        }
        if (minus) {
            return make_shared<UnarySyntaxNode>(Scanner::token::SUB, node);
        }
        return node;
    }

    DECLARE_VISIT(visitDouble) {
        return make_shared<LiteralSyntaxNode>(value.GetDouble(), false);
    }

    DECLARE_VISIT(visitBool) {
        return make_shared<LiteralSyntaxNode>(value.GetBool() ? 1 : 0, false, 0);
    }

    DECLARE_VISIT(visitString) {
        return make_shared<LiteralSyntaxNode>(value.GetString());
    }

    DECLARE_VISIT(visitColumn) {
        const auto &key = SqlJsonUtil::getColumnName(value);
        auto it = _idMap.find(key);
        if (it == _idMap.end()) {
            it = _idMap.insert({key, MatchDocsProducer::Make(key)}).first;
        }
        return it->second;
    }

private:
    Result<SyntaxNodePtr> _r;
    map<string, SyntaxNodePtr> _idMap;
};

Result<SyntaxNodePtr>
IquanPlanImporter::sqlOutput(const string &type, const string &name, const SyntaxNodePtr &node) {
    SyntaxNodePtr result;
    auto func = [&](auto a) {
        using T = typename decltype(a)::value_type;
        result = MatchDocsConsumer<T>::Make(1, name, node);
        return true;
    };
    auto vt = ExprUtil::transSqlTypeToVariableType(type).first;
    auto ok = table::ValueTypeSwitch::switchType(vt, false, func, func);
    AR_REQUIRE_TRUE(ok, RuntimeError::make("unknown sql type: [%s]", type.c_str()));
    return result;
}

Result<> IquanPlanImporter::import(const CalcInitParam &params,
                                   LogicalGraphPtr &node,
                                   map<string, expr::SyntaxNodePtr> &outMap) {
    autil::mem_pool::Pool pool;
    autil::AutilPoolAllocator alloc {&pool};

    outMap.clear();
    SyntaxNodePtr cond;
    IquanPlan2SyntaxNodeVisitor visitor;

    if (!params.outputExprsJson.empty()) {
        autil::SimpleDocument json {&alloc};
        json.Parse(params.outputExprsJson.c_str());
        AR_REQUIRE_TRUE(!json.HasParseError(),
                        RuntimeError::make("error [%d] when parsing json: [%s]",
                                           (int)json.GetParseError(),
                                           params.outputExprsJson.c_str()));
        for (auto it = json.MemberBegin(); it != json.MemberEnd(); ++it) {
            const string &key = SqlJsonUtil::isColumn(it->name)
                                    ? SqlJsonUtil::getColumnName(it->name)
                                    : it->name.GetString();
            outMap[key] = AR_RET_IF_ERR(visitor.apply(it->value));
        }
    }

    if (!params.conditionJson.empty()) {
        autil::SimpleDocument json {&alloc};
        json.Parse(params.conditionJson.c_str());
        AR_REQUIRE_TRUE(!json.HasParseError(),
                        RuntimeError::make("error [%d] when parsing json: [%s]",
                                           (int)json.GetParseError(),
                                           params.conditionJson.c_str()));
        cond = AR_RET_IF_ERR(visitor.apply(json));
    }

    vector<SyntaxNodePtr> outs;
    AR_REQUIRE_TRUE(!params.outputFieldsType.empty(),
                    RuntimeError::make("plan without outputFieldsType is not supported yet"));
    for (int i = 0; i < params.outputFields.size(); ++i) {
        auto &name = params.outputFields[i];
        auto &type = params.outputFieldsType[i];
        auto it = outMap.find(name);
        if (it != outMap.end()) {
            outs.emplace_back(AR_RET_IF_ERR(sqlOutput(type, name, it->second)));
        }
    }

    node = LogicalGraph::Make(std::move(outs), cond);
    return {};
}

} // namespace isearch::sql::turbojet
