#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/search/JitNormalAggregator.h>
#include <ha3/search/AggregatorCreator.h>
#include <ha3/func_expression/FunctionProvider.h>
#include <suez/turing/expression/syntax/SyntaxExprValidator.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/queryparser/ClauseParserContext.h>
#include <suez/turing/common/QueryResource.h>
#include <suez/turing/common/CavaJitWrapper.h>
#include <ha3/common/XMLResultFormatter.h>

using namespace autil;
using namespace std;
using namespace testing;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(queryparser);
BEGIN_HA3_NAMESPACE(search);

class TEST_CLASS_NAME : public TESTBASE {
public:
    void setUp();
    void tearDown();
    AttributeInfos* createFakeAttributeInfos();
    void testCodegen(const std::string &aggDescStr,
                           const std::string &resCode);
    std::vector<matchdoc::MatchDoc> genMatchDocs();
    std::string formatAggresult(Aggregator *agg, std::vector<matchdoc::MatchDoc> &docs) {
        agg->batchAggregate(docs);
        agg->endLayer(1.0);
        auto aggRes = agg->collectAggregateResult();
        ResultPtr result(new Result());
        result->fillAggregateResults(aggRes);
        auto aggResultCount = result->getAggregateResultCount();
        for (uint32_t i = 0; i < aggResultCount; ++i) {
            AggregateResultPtr aggResult = result->getAggregateResult(i);
            if (aggResult) {
                aggResult->constructGroupValueMap();
            }
        }
        XMLResultFormatter formatter;
        std::stringstream ss;
        formatter.format(result, ss);
        return ss.str();
    }
private:
    suez::turing::AttributeExpressionCreator *_attributeExpressionCreator;
    suez::turing::FunctionManagerPtr _funcManagerPtr;
    suez::turing::FunctionInterfaceCreator *_funcCreator;
    func_expression::FunctionProvider *_functionProvider;
    AttributeInfos *_attrInfos;
    SyntaxExprValidator *_syntaxExprValidator;
    autil::mem_pool::Pool *_pool;
    tensorflow::QueryResource *_queryResource;
    CavaJitWrapper *_cavaJitWrapper;
    common::Ha3MatchDocAllocator *_allocator;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, TEST_CLASS_NAME);

void TEST_CLASS_NAME::setUp() {
    ::cava::CavaJit::globalInit();
    _pool = new autil::mem_pool::Pool(1024);
    _allocator = new common::Ha3MatchDocAllocator(_pool, false);
    FakeIndex fakeIndex;
    fakeIndex.attributes = "attr_int8 : int8_t : -8, 2, 2, -128, 127;"
                           "attr_int16 : int16_t : -16, 2, -32768, 32767, 2;"
                           "attr_int32 : int32_t : -32, 2, -32, -2147483648, 2147483647;"
                           "attr_int64 : int64_t : -64, 2, -64, -9223372036854775808, 9223372036854775807;"
                           "attr_uint8 : uint8_t : 8, 2, 8, 255, 2;"
                           "attr_uint16 : uint16_t : 16, 2, 16, 0, 65535;"
                           "attr_uint32 : uint32_t : 32, 2, 32, 0, 4294967295;"
                           "attr_uint64 : uint64_t : 64, 2, 64, 0, 18446744073709551615;"
                           "attr_float : float : 1.0, 2.0, 1.0, "
                           "-340282346638528859811704183484516925440.000000, "
                           "340282346638528859811704183484516925440.000000;"
                           "attr_double : double : 2.0, 2.0, 1.0, "
                           "-179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000, "
                           "179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000;"
                           "attr_string : string : string1, sting2, string1, string2, string3;"
                           "attr_mint8 : multi_int8_t : -8,2#3,4##2#3;"
                           "attr_mint16 : multi_int16_t : -16,2#3,4#1#2#3;"
                           "attr_mint32 : multi_int32_t : -32,2#3,4###;"
                           "attr_mint64 : multi_int64_t : -64,2#3,4###;"
                           "attr_muint8 : multi_uint8_t : 8,2#3,4###;"
                           "attr_muint16 : multi_uint16_t : 16,2#3,4###;"
                           "attr_muint32 : multi_uint32_t : 32,2#3,4###;"
                           "attr_muint64 : multi_uint64_t : 64,2#3,4###;"
                           "attr_mfloat : multi_float : 1,2#3,4###;"
                           "attr_mdouble : multi_double : 2,2#3,4###;"
                           "attr_mstring : multi_string : string1,string2#string3,string4###";
    IndexPartitionReaderWrapperPtr idxPartiReaderWrapperPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);

    _funcCreator = new FunctionInterfaceCreator();
    FuncConfig fc;
    _funcCreator->init(fc, {});
    _functionProvider = new func_expression::FunctionProvider(func_expression::FunctionResource());
    _attributeExpressionCreator = new FakeAttributeExpressionCreator(_pool,
            idxPartiReaderWrapperPtr, _funcCreator, _functionProvider, NULL, NULL, NULL);

    _attrInfos = createFakeAttributeInfos();
    std::vector<VirtualAttribute*> virtualAttributes;
    _syntaxExprValidator = new SyntaxExprValidator(_attrInfos,
            virtualAttributes, true);

    _queryResource = new tensorflow::QueryResource();
    suez::turing::CavaConfig cavaConfigInfo;
    cavaConfigInfo._cavaConf = "../../testdata/cava_summary/ha3_cava_config.json";
    _cavaJitWrapper = new suez::turing::CavaJitWrapper("", cavaConfigInfo, NULL);
    ASSERT_TRUE(_cavaJitWrapper->init());
    _queryResource->setCavaJitWrapper(_cavaJitWrapper);
    _queryResource->setCavaAllocator(SuezCavaAllocatorPtr(new SuezCavaAllocator(_pool, 256 * 1024 * 1024)));
}

void TEST_CLASS_NAME::tearDown() {
    _queryResource->setCavaAllocator(SuezCavaAllocatorPtr());
    DELETE_AND_SET_NULL(_queryResource);
    DELETE_AND_SET_NULL(_funcCreator);
    DELETE_AND_SET_NULL(_functionProvider);
    DELETE_AND_SET_NULL(_attributeExpressionCreator);
    DELETE_AND_SET_NULL(_syntaxExprValidator);
    DELETE_AND_SET_NULL(_attrInfos);
    DELETE_AND_SET_NULL(_allocator);
    DELETE_AND_SET_NULL(_pool);
    DELETE_AND_SET_NULL(_cavaJitWrapper);
}

std::vector<matchdoc::MatchDoc> TEST_CLASS_NAME::genMatchDocs() {
    std::vector<matchdoc::MatchDoc> ret;
    for (int i = 0; i < 5; i++) {
        ret.push_back(_allocator->allocate(i));
    }
    return ret;
}

AttributeInfos* TEST_CLASS_NAME::createFakeAttributeInfos() {
    AttributeInfos *attrInfos = new AttributeInfos;

#define ADD_ATTR_TYPE_HELPER(name, ft_type, isMulti, isSub)     \
    do {                                                  \
        AttributeInfo *attrInfo = new AttributeInfo();    \
        attrInfo->setAttrName(name);                      \
        attrInfo->setMultiValueFlag(isMulti);             \
        attrInfo->setSubDocAttributeFlag(isSub);          \
        FieldInfo fieldInfo(name, ft_type);               \
        attrInfo->setFieldInfo(fieldInfo);                \
        attrInfos->addAttributeInfo(attrInfo);            \
    } while(0);                                           \

    ADD_ATTR_TYPE_HELPER("attr_int8", ft_int8, false, false);
    ADD_ATTR_TYPE_HELPER("attr_uint8", ft_uint8, false, false);
    ADD_ATTR_TYPE_HELPER("attr_int16", ft_int16, false, false);
    ADD_ATTR_TYPE_HELPER("attr_uint16", ft_uint16, false, false);
    ADD_ATTR_TYPE_HELPER("attr_int32", ft_integer, false, false);
    ADD_ATTR_TYPE_HELPER("attr_uint32", ft_uint32, false, false);
    ADD_ATTR_TYPE_HELPER("attr_int64", ft_long, false, false);
    ADD_ATTR_TYPE_HELPER("attr_uint64", ft_uint64, false, false);
    ADD_ATTR_TYPE_HELPER("attr_float", ft_float, false, false);
    ADD_ATTR_TYPE_HELPER("attr_double", ft_double, false, false);
    ADD_ATTR_TYPE_HELPER("attr_string", ft_string, false, false);

    ADD_ATTR_TYPE_HELPER("attr_mint8", ft_int8, true, false);
    ADD_ATTR_TYPE_HELPER("attr_muint8", ft_uint8, true, false);
    ADD_ATTR_TYPE_HELPER("attr_mint16", ft_int16, true, false);
    ADD_ATTR_TYPE_HELPER("attr_muint16", ft_uint16, true, false);
    ADD_ATTR_TYPE_HELPER("attr_mint32", ft_integer, true, false);
    ADD_ATTR_TYPE_HELPER("attr_muint32", ft_uint32, true, false);
    ADD_ATTR_TYPE_HELPER("attr_mint64", ft_long, true, false);
    ADD_ATTR_TYPE_HELPER("attr_muint64", ft_uint64, true, false);
    ADD_ATTR_TYPE_HELPER("attr_mfloat", ft_float, true, false);
    ADD_ATTR_TYPE_HELPER("attr_mdouble", ft_double, true, false);
    ADD_ATTR_TYPE_HELPER("attr_mstring", ft_string, true, false);

#undef ADD_ATTR_TYPE_HELPER

    return attrInfos;
}

void TEST_CLASS_NAME::testCodegen(const std::string &aggDescStr,
        const std::string &resCode)
{
    AggregatorCreator aggCreator(_attributeExpressionCreator, _pool, _queryResource);
    aggCreator._aggSamplerConfigInfo._enableDenseMode = ENABLE_DENSE_MODE;

    AggregateClause aggClause;
    ClauseParserContext ctx;
    ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
    AggregateDescription *aggDescription = ctx.stealAggDescription();
    // validate groupKeyExpr
    auto groupKeyExpr = aggDescription->getGroupKeyExpr();
    _syntaxExprValidator->validate(groupKeyExpr);
    auto errorCode = _syntaxExprValidator->getErrorCode();
    ASSERT_TRUE(errorCode == ERROR_NONE);
    // validate func
    auto &aggFunDesVec = aggDescription->getAggFunDescriptions();
    for (size_t i = 0; i < aggFunDesVec.size(); i++) {
        if (aggFunDesVec[i]->getFunctionName() == "count") {
            continue;
        }
        auto syntaxExpr = aggFunDesVec[i]->getSyntaxExpr();
        _syntaxExprValidator->validate(syntaxExpr);
    }

    aggClause.addAggDescription(aggDescription);

    Aggregator *agg = aggCreator.createAggregator(&aggClause);
    std::string code;
#define CASE(vt_type)                                                   \
    case vt_type: {                                                     \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;     \
        if (groupKeyExpr->isMultiValue()) {				\
	    DefGroupType(T, GroupType);					\
            typedef NormalAggregator<T, autil::MultiValueType<T>, GroupType> MAggT; \
	    auto aggTyped = dynamic_cast<MAggT *>(agg);			\
	    ASSERT_NE(nullptr, aggTyped);				\
            aggTyped->constructCavaCode(code);				\
        } else {                                                        \
	    DefGroupType(T, GroupType);					\
            typedef NormalAggregator<T, T, GroupType> AggT;		\
	    auto aggTyped = dynamic_cast<AggT *>(agg);			\
	    ASSERT_NE(nullptr, aggTyped);				\
            aggTyped->constructCavaCode(code);				\
        }                                                               \
        break;                                                          \
    }

    switch (groupKeyExpr->getExprResultType()) {
        CASE(vt_int8)
        CASE(vt_int16)
        CASE(vt_int32)
        CASE(vt_int64)
        CASE(vt_uint8)
        CASE(vt_uint16)
        CASE(vt_uint32)
        CASE(vt_uint64)
        CASE(vt_float)
        CASE(vt_double)
        CASE(vt_string)
    default:
        break;
    }
#undef CASE
    ASSERT_EQ(resCode, code);
    size_t hashKey = _cavaJitWrapper->calcHashKey({code});
    double ts = autil::TimeUtility::currentTime() / 1000.;
    auto cavaJitModule = _cavaJitWrapper->compile("test", {"test"}, {code}, hashKey);
    double te = autil::TimeUtility::currentTime() / 1000.;
    AUTIL_LOG(WARN, "compile use %f", te - ts);
    ASSERT_TRUE(cavaJitModule != NULL);
    auto cavaAggModuleInfo = agg->codegen();
    ASSERT_TRUE(cavaAggModuleInfo != NULL);

#define CASE(vt_type)                                                   \
    case vt_type: {                                                     \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;     \
        if (groupKeyExpr->isMultiValue()) {                             \
	    DefGroupType(T, GroupType);					\
            typedef NormalAggregator<T, autil::MultiValueType<T>, GroupType> MAggT; \
            auto aggTyped = dynamic_cast<MAggT *>(agg);                 \
	    ASSERT_NE(nullptr, aggTyped);				\
            typedef JitNormalAggregator<T, autil::MultiValueType<T>, GroupType> JitMAggT; \
            jitAgg = new JitMAggT(aggTyped, _pool,                      \
                    cavaAggModuleInfo, _queryResource->getCavaAllocator()); \
        } else {                                                        \
	    DefGroupType(T, GroupType);					\
            typedef NormalAggregator<T, T, GroupType> AggT;		\
            auto aggTyped = dynamic_cast<AggT *>(agg);                  \
	    ASSERT_NE(nullptr, aggTyped);				\
            typedef JitNormalAggregator<T, T, GroupType> JitAggT;	\
            jitAgg = new JitAggT(aggTyped, _pool,                       \
                    cavaAggModuleInfo, _queryResource->getCavaAllocator()); \
        }                                                               \
        break;                                                          \
    }

    auto docs = genMatchDocs();
    Aggregator *jitAgg = NULL;
    switch (groupKeyExpr->getExprResultType()) {
        CASE(vt_int8)
        CASE(vt_int16)
        CASE(vt_int32)
        CASE(vt_int64)
        CASE(vt_uint8)
        CASE(vt_uint16)
        CASE(vt_uint32)
        CASE(vt_uint64)
        CASE(vt_float)
        CASE(vt_double)
        CASE(vt_string)
    default:
        break;
    }
#undef CASE

    if (jitAgg) {
        auto aggResStr = formatAggresult(agg, docs);
        auto jitAggResStr = formatAggresult(jitAgg, docs);
        DELETE_AND_SET_NULL(jitAgg);
        ASSERT_EQ(aggResStr, jitAggResStr);
    } else {
        DELETE_AND_SET_NULL(agg);
    }
}


TEST_F(TEST_CLASS_NAME, testCountSingleValueAsGroupKey) {
    // test count
    const std::string countTemplate = R"(package ha3;
import unsafe.*;

class AggItem {
    long count0;
    {GroupKeyType} groupKey;
};

class JitAggregator {

    public AggItemAllocator allocator;
    public AttributeExpression groupKeyExpr;
    public uint maxSortCount;
    public {AggItemMapType} itemMap;
    public Reference groupKeyRef;
    public Reference countRef0;

    public uint count() {
        return itemMap.size();
    }

    static public JitAggregator create(Aggregator aggregator) {
        JitAggregator jitAggregator = new JitAggregator();
        jitAggregator.groupKeyRef = aggregator.getGroupKeyRef();
        jitAggregator.groupKeyExpr = aggregator.getGroupKeyExpr();
        jitAggregator.maxSortCount = aggregator.getMaxSortCount();
        jitAggregator.allocator = AggItemAllocator.create((uint)16);
        jitAggregator.itemMap = {AggItemMapType}.create();
        jitAggregator.countRef0 = aggregator.getAggFunctionRef((uint)0, (uint)0);
        return jitAggregator;
    }

    public void batch(MatchDocs docs, uint size) {
        for (uint i = 0; i < size; ++i) {
            MatchDoc doc = docs.get(i);
            {GroupKeyType} key = groupKeyExpr.{GroupKeyGetFunc}(doc);
            AggItem item = (AggItem)itemMap.get(key);
            if (item == null) {
                if (itemMap.size() >= maxSortCount) {
                    continue;
                }
                item = (AggItem)allocator.alloc();
                item.groupKey = key;
                item.count0 = 0;
                itemMap.add(key, (Any)item);
            }
            item.count0 += 1;
        }
    }

    public void result(MatchDocs docs, uint size) {
        uint i = 0;
        for (Any any = itemMap.begin(); any != null && i < size; any = itemMap.next()) {
            AggItem item = (AggItem)any;
            MatchDoc doc = docs.get(i);
            countRef0.setInt64(doc, item.count0);
            groupKeyRef.{GroupKeyRefSetFunc}(doc, item.groupKey);
            ++i;
        }
    }
}
)";
    // single value
    {
        std::string resCode = countTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "byte");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "ByteAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getInt8");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setInt8");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_int8,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "short");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "ShortAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getInt16");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setInt16");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_int16,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "int");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "IntAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getInt32");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setInt32");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_int32,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "long");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "LongAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getInt64");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setInt64");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_int64,agg_fun:count()", resCode));
    }

    {
        std::string resCode = countTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "ubyte");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "UByteAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getUInt8");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setUInt8");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_uint8,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "ushort");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "UShortAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getUInt16");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setUInt16");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_uint16,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "uint");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "UIntAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getUInt32");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setUInt32");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_uint32,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "ulong");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "ULongAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getUInt64");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setUInt64");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_uint64,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "float");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "FloatAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getFloat");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setFloat");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_float, agg_fun:count()", resCode));
    }
    {
        std::string resCode = countTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "double");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "DoubleAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getDouble");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setDouble");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_double, agg_fun:count()", resCode));
    }
    {
        std::string resCode = countTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "MChar");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "MCharAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMChar");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setSTLString");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_string, agg_fun:count()", resCode));
    }
}

TEST_F(TEST_CLASS_NAME, testCountMultipleValueAsGroupKey) {
    const std::string countMultipleValueTemplate = R"(package ha3;
import unsafe.*;

class AggItem {
    long count0;
    {GroupKeyType} groupKey;
};

class JitAggregator {

    public AggItemAllocator allocator;
    public AttributeExpression groupKeyExpr;
    public uint maxSortCount;
    public {AggItemMapType} itemMap;
    public Reference groupKeyRef;
    public Reference countRef0;

    public uint count() {
        return itemMap.size();
    }

    static public JitAggregator create(Aggregator aggregator) {
        JitAggregator jitAggregator = new JitAggregator();
        jitAggregator.groupKeyRef = aggregator.getGroupKeyRef();
        jitAggregator.groupKeyExpr = aggregator.getGroupKeyExpr();
        jitAggregator.maxSortCount = aggregator.getMaxSortCount();
        jitAggregator.allocator = AggItemAllocator.create((uint)16);
        jitAggregator.itemMap = {AggItemMapType}.create();
        jitAggregator.countRef0 = aggregator.getAggFunctionRef((uint)0, (uint)0);
        return jitAggregator;
    }

    public void batch(MatchDocs docs, uint size) {
        {GroupExprType} groupKey = null;
        for (uint i = 0; i < size; ++i) {
            MatchDoc doc = docs.get(i);
            groupKey = groupKeyExpr.{GroupKeyGetFunc}(doc, groupKey);
            for (uint i = 0; i < groupKey.size(); i++) {
                {GroupKeyType} key = groupKey.getWithoutCheck(i);
                AggItem item = (AggItem)itemMap.get(key);
                if (item == null) {
                    if (itemMap.size() >= maxSortCount) {
                        continue;
                    }
                    item = (AggItem)allocator.alloc();
                    item.groupKey = key;
                    item.count0 = 0;
                    itemMap.add(key, (Any)item);
                }
                item.count0 += 1;
            }
        }
    }

    public void result(MatchDocs docs, uint size) {
        uint i = 0;
        for (Any any = itemMap.begin(); any != null && i < size; any = itemMap.next()) {
            AggItem item = (AggItem)any;
            MatchDoc doc = docs.get(i);
            countRef0.setInt64(doc, item.count0);
            groupKeyRef.{GroupKeyRefSetFunc}(doc, item.groupKey);
            ++i;
        }
    }
}
)";
    {
        std::string resCode = countMultipleValueTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "byte");
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", "MInt8");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "ByteAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMInt8");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setInt8");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_mint8,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countMultipleValueTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "short");
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", "MInt16");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "ShortAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMInt16");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setInt16");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_mint16,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countMultipleValueTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "int");
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", "MInt32");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "IntAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMInt32");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setInt32");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_mint32,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countMultipleValueTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "long");
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", "MInt64");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "LongAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMInt64");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setInt64");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_mint64,agg_fun:count()", resCode));
    }
    // unsigned
    {
        std::string resCode = countMultipleValueTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "ubyte");
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", "MUInt8");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "UByteAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMUInt8");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setUInt8");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_muint8,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countMultipleValueTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "ushort");
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", "MUInt16");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "UShortAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMUInt16");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setUInt16");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_muint16,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countMultipleValueTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "uint");
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", "MUInt32");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "UIntAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMUInt32");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setUInt32");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_muint32,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countMultipleValueTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "ulong");
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", "MUInt64");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "ULongAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMUInt64");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setUInt64");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_muint64,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countMultipleValueTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "double");
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", "MDouble");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "DoubleAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMDouble");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setDouble");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_mdouble,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countMultipleValueTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "float");
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", "MFloat");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "FloatAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMFloat");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setFloat");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_mfloat,agg_fun:count()", resCode));
    }
    {
        std::string resCode = countMultipleValueTemplate;
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", "MChar");
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", "MString");
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", "MCharAggItemMap");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", "getMString");
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", "setSTLString");
        ASSERT_NO_FATAL_FAILURE(testCodegen("group_key:attr_mstring,agg_fun:count()", resCode));
    }
}

TEST_F(TEST_CLASS_NAME, testSumSingleValue) {
    // test sum
    const std::string templateStr = R"(package ha3;
import unsafe.*;

class AggItem {
    {SumItemType} sum0;
    {GroupKeyType} groupKey;
};

class JitAggregator {

    public AggItemAllocator allocator;
    public AttributeExpression groupKeyExpr;
    public uint maxSortCount;
    public {AggItemMapType} itemMap;
    public Reference groupKeyRef;
    public Reference sumRef0;
    public AttributeExpression sumExpr0;

    public uint count() {
        return itemMap.size();
    }

    static public JitAggregator create(Aggregator aggregator) {
        JitAggregator jitAggregator = new JitAggregator();
        jitAggregator.groupKeyRef = aggregator.getGroupKeyRef();
        jitAggregator.groupKeyExpr = aggregator.getGroupKeyExpr();
        jitAggregator.maxSortCount = aggregator.getMaxSortCount();
        jitAggregator.allocator = AggItemAllocator.create((uint)16);
        jitAggregator.itemMap = {AggItemMapType}.create();
        jitAggregator.sumRef0 = aggregator.getAggFunctionRef((uint)0, (uint)0);
        jitAggregator.sumExpr0 = aggregator.getAggFunctionExpr((uint)0);
        return jitAggregator;
    }

    public void batch(MatchDocs docs, uint size) {
        for (uint i = 0; i < size; ++i) {
            MatchDoc doc = docs.get(i);
            {GroupKeyType} key = groupKeyExpr.{GroupKeyGetFunc}(doc);
            AggItem item = (AggItem)itemMap.get(key);
            if (item == null) {
                if (itemMap.size() >= maxSortCount) {
                    continue;
                }
                item = (AggItem)allocator.alloc();
                item.groupKey = key;
                item.sum0 = 0;
                itemMap.add(key, (Any)item);
            }
            {SumExprType} sum0 = sumExpr0.{SumExprFunc}(doc);
            item.sum0 += sum0;
        }
    }

    public void result(MatchDocs docs, uint size) {
        uint i = 0;
        for (Any any = itemMap.begin(); any != null && i < size; any = itemMap.next()) {
            AggItem item = (AggItem)any;
            MatchDoc doc = docs.get(i);
            sumRef0.{SumRefSetFunc}(doc, item.sum0);
            groupKeyRef.{GroupKeyRefSetFunc}(doc, item.groupKey);
            ++i;
        }
    }
}
)";
    // single value
#define CASE_GROUP_KEY(group_key,                                       \
                       GroupKeyType, AggItemMapType, GroupKeyGetFunc, GroupKeyRefSetFunc, \
                       field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc) \
    {                                                                   \
        std::string resCode = templateStr;                              \
        autil::StringUtil::replaceAll(resCode, "{SumItemType}", #SumItemType); \
        autil::StringUtil::replaceAll(resCode, "{SumExprType}", #SumExprType); \
        autil::StringUtil::replaceAll(resCode, "{SumRefSetFunc}", #SumRefSetFunc); \
        autil::StringUtil::replaceAll(resCode, "{SumExprFunc}", #SumExprFunc); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", #GroupKeyType); \
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", #AggItemMapType); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", #GroupKeyGetFunc); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", #GroupKeyRefSetFunc); \
        testCodegen("group_key:" #group_key ",agg_fun:sum(" #field_name ")", resCode); \
        if (HasFatalFailure()) {                                        \
            return;                                                     \
        }                                                               \
    }

#define CASE_GROUP_KEY_ALL(field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc) \
    CASE_GROUP_KEY(attr_int8, byte, ByteAggItemMap, getInt8, setInt8,   \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_int16, short, ShortAggItemMap, getInt16, setInt16, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_int32, int, IntAggItemMap, getInt32, setInt32,  \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_int64, long, LongAggItemMap, getInt64, setInt64, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_uint8, ubyte, UByteAggItemMap, getUInt8, setUInt8, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_uint16, ushort, UShortAggItemMap, getUInt16, setUInt16, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_uint32, uint, UIntAggItemMap, getUInt32, setUInt32, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_uint64, ulong, ULongAggItemMap, getUInt64, setUInt64, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_float, float, FloatAggItemMap, getFloat, setFloat, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_double, double, DoubleAggItemMap, getDouble, setDouble, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_string, MChar, MCharAggItemMap, getMChar, setSTLString, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc);

    CASE_GROUP_KEY_ALL(attr_int8, long, byte, setInt64, getInt8);
    CASE_GROUP_KEY_ALL(attr_int16, long, short, setInt64, getInt16);
    CASE_GROUP_KEY_ALL(attr_int32, long, int, setInt64, getInt32);
    CASE_GROUP_KEY_ALL(attr_int64, long, long, setInt64, getInt64);
    CASE_GROUP_KEY_ALL(attr_uint8, long, ubyte, setInt64, getUInt8);
    CASE_GROUP_KEY_ALL(attr_uint16, long, ushort, setInt64, getUInt16);
    CASE_GROUP_KEY_ALL(attr_uint32, long, uint, setInt64, getUInt32);
    CASE_GROUP_KEY_ALL(attr_uint64, ulong, ulong, setUInt64, getUInt64);
    CASE_GROUP_KEY_ALL(attr_float, float, float, setFloat, getFloat);
    CASE_GROUP_KEY_ALL(attr_double, double, double, setDouble, getDouble);

#undef CASE_GROUP_KEY_ALL
#undef CASE_GROUP_KEY
}



TEST_F(TEST_CLASS_NAME, testSumMultipleValue) {
    const std::string templateStr = R"(package ha3;
import unsafe.*;

class AggItem {
    {SumItemType} sum0;
    {GroupKeyType} groupKey;
};

class JitAggregator {

    public AggItemAllocator allocator;
    public AttributeExpression groupKeyExpr;
    public uint maxSortCount;
    public {AggItemMapType} itemMap;
    public Reference groupKeyRef;
    public Reference sumRef0;
    public AttributeExpression sumExpr0;

    public uint count() {
        return itemMap.size();
    }

    static public JitAggregator create(Aggregator aggregator) {
        JitAggregator jitAggregator = new JitAggregator();
        jitAggregator.groupKeyRef = aggregator.getGroupKeyRef();
        jitAggregator.groupKeyExpr = aggregator.getGroupKeyExpr();
        jitAggregator.maxSortCount = aggregator.getMaxSortCount();
        jitAggregator.allocator = AggItemAllocator.create((uint)16);
        jitAggregator.itemMap = {AggItemMapType}.create();
        jitAggregator.sumRef0 = aggregator.getAggFunctionRef((uint)0, (uint)0);
        jitAggregator.sumExpr0 = aggregator.getAggFunctionExpr((uint)0);
        return jitAggregator;
    }

    public void batch(MatchDocs docs, uint size) {
        {GroupExprType} groupKey = null;
        for (uint i = 0; i < size; ++i) {
            MatchDoc doc = docs.get(i);
            groupKey = groupKeyExpr.{GroupKeyGetFunc}(doc, groupKey);
            for (uint i = 0; i < groupKey.size(); i++) {
                {GroupKeyType} key = groupKey.getWithoutCheck(i);
                AggItem item = (AggItem)itemMap.get(key);
                if (item == null) {
                    if (itemMap.size() >= maxSortCount) {
                        continue;
                    }
                    item = (AggItem)allocator.alloc();
                    item.groupKey = key;
                    item.sum0 = 0;
                    itemMap.add(key, (Any)item);
                }
                {SumExprType} sum0 = sumExpr0.{SumExprFunc}(doc);
                item.sum0 += sum0;
            }
        }
    }

    public void result(MatchDocs docs, uint size) {
        uint i = 0;
        for (Any any = itemMap.begin(); any != null && i < size; any = itemMap.next()) {
            AggItem item = (AggItem)any;
            MatchDoc doc = docs.get(i);
            sumRef0.{SumRefSetFunc}(doc, item.sum0);
            groupKeyRef.{GroupKeyRefSetFunc}(doc, item.groupKey);
            ++i;
        }
    }
}
)";
    // single value
#define CASE_GROUP_KEY(group_key, GroupExprType,                        \
                       GroupKeyType, AggItemMapType, GroupKeyGetFunc, GroupKeyRefSetFunc, \
                       field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc) \
    {                                                                   \
        std::string resCode = templateStr;                              \
        autil::StringUtil::replaceAll(resCode, "{SumItemType}", #SumItemType); \
        autil::StringUtil::replaceAll(resCode, "{SumExprType}", #SumExprType); \
        autil::StringUtil::replaceAll(resCode, "{SumRefSetFunc}", #SumRefSetFunc); \
        autil::StringUtil::replaceAll(resCode, "{SumExprFunc}", #SumExprFunc); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", #GroupKeyType); \
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", #GroupExprType); \
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", #AggItemMapType); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", #GroupKeyGetFunc); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", #GroupKeyRefSetFunc); \
        testCodegen("group_key:" #group_key ",agg_fun:sum(" #field_name ")", resCode); \
        if (HasFatalFailure()) {                                        \
            return;                                                     \
        }                                                               \
    }

#define CASE_GROUP_KEY_ALL(field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc) \
    CASE_GROUP_KEY(attr_mint8, MInt8, byte, ByteAggItemMap, getMInt8, setInt8, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_mint16, MInt16, short, ShortAggItemMap, getMInt16, setInt16, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_mint32, MInt32, int, IntAggItemMap, getMInt32, setInt32, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_mint64, MInt64, long, LongAggItemMap, getMInt64, setInt64, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_muint8, MUInt8, ubyte, UByteAggItemMap, getMUInt8, setUInt8, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_muint16, MUInt16, ushort, UShortAggItemMap, getMUInt16, setUInt16, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_muint32, MUInt32, uint, UIntAggItemMap, getMUInt32, setUInt32, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_muint64, MUInt64, ulong, ULongAggItemMap, getMUInt64, setUInt64, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_mfloat, MFloat, float, FloatAggItemMap, getMFloat, setFloat, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_mdouble, MDouble, double, DoubleAggItemMap, getMDouble, setDouble, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc); \
    CASE_GROUP_KEY(attr_mstring, MString, MChar, MCharAggItemMap, getMString, setSTLString, \
                   field_name, SumItemType, SumExprType, SumRefSetFunc, SumExprFunc);

    CASE_GROUP_KEY_ALL(attr_int8, long, byte, setInt64, getInt8);
    CASE_GROUP_KEY_ALL(attr_int16, long, short, setInt64, getInt16);
    CASE_GROUP_KEY_ALL(attr_int32, long, int, setInt64, getInt32);
    CASE_GROUP_KEY_ALL(attr_int64, long, long, setInt64, getInt64);
    CASE_GROUP_KEY_ALL(attr_uint8, long, ubyte, setInt64, getUInt8);
    CASE_GROUP_KEY_ALL(attr_uint16, long, ushort, setInt64, getUInt16);
    CASE_GROUP_KEY_ALL(attr_uint32, long, uint, setInt64, getUInt32);
    CASE_GROUP_KEY_ALL(attr_uint64, ulong, ulong, setUInt64, getUInt64);
    CASE_GROUP_KEY_ALL(attr_float, float, float, setFloat, getFloat);
    CASE_GROUP_KEY_ALL(attr_double, double, double, setDouble, getDouble);

#undef CASE_GROUP_KEY_ALL
#undef CASE_GROUP_KEY
}

TEST_F(TEST_CLASS_NAME, testMinSingleValue) {
    const std::string templateStr = R"(package ha3;
import unsafe.*;

class AggItem {
    {MinItemType} min0;
    {GroupKeyType} groupKey;
};

class JitAggregator {

    public AggItemAllocator allocator;
    public AttributeExpression groupKeyExpr;
    public uint maxSortCount;
    public {AggItemMapType} itemMap;
    public Reference groupKeyRef;
    public Reference minRef0;
    public AttributeExpression minExpr0;

    public uint count() {
        return itemMap.size();
    }

    static public JitAggregator create(Aggregator aggregator) {
        JitAggregator jitAggregator = new JitAggregator();
        jitAggregator.groupKeyRef = aggregator.getGroupKeyRef();
        jitAggregator.groupKeyExpr = aggregator.getGroupKeyExpr();
        jitAggregator.maxSortCount = aggregator.getMaxSortCount();
        jitAggregator.allocator = AggItemAllocator.create((uint)16);
        jitAggregator.itemMap = {AggItemMapType}.create();
        jitAggregator.minRef0 = aggregator.getAggFunctionRef((uint)0, (uint)0);
        jitAggregator.minExpr0 = aggregator.getAggFunctionExpr((uint)0);
        return jitAggregator;
    }

    public void batch(MatchDocs docs, uint size) {
        for (uint i = 0; i < size; ++i) {
            MatchDoc doc = docs.get(i);
            {GroupKeyType} key = groupKeyExpr.{GroupKeyGetFunc}(doc);
            AggItem item = (AggItem)itemMap.get(key);
            if (item == null) {
                if (itemMap.size() >= maxSortCount) {
                    continue;
                }
                item = (AggItem)allocator.alloc();
                item.groupKey = key;
                item.min0 = {MinInitValue};
                itemMap.add(key, (Any)item);
            }
            {MinExprType} min0 = minExpr0.{MinExprFunc}(doc);
            if (min0 < item.min0) item.min0 = min0;
        }
    }

    public void result(MatchDocs docs, uint size) {
        uint i = 0;
        for (Any any = itemMap.begin(); any != null && i < size; any = itemMap.next()) {
            AggItem item = (AggItem)any;
            MatchDoc doc = docs.get(i);
            minRef0.{MinRefSetFunc}(doc, item.min0);
            groupKeyRef.{GroupKeyRefSetFunc}(doc, item.groupKey);
            ++i;
        }
    }
}
)";
    // single value
#define CASE_GROUP_KEY(group_key,                                       \
                       GroupKeyType, AggItemMapType, GroupKeyGetFunc, GroupKeyRefSetFunc, \
                       field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue) \
    {                                                                   \
        std::string resCode = templateStr;                              \
        autil::StringUtil::replaceAll(resCode, "{MinItemType}", #MinItemType); \
        autil::StringUtil::replaceAll(resCode, "{MinExprType}", #MinExprType); \
        autil::StringUtil::replaceAll(resCode, "{MinRefSetFunc}", #MinRefSetFunc); \
        autil::StringUtil::replaceAll(resCode, "{MinExprFunc}", #MinExprFunc); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", #GroupKeyType); \
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", #AggItemMapType); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", #GroupKeyGetFunc); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", #GroupKeyRefSetFunc); \
        autil::StringUtil::replaceAll(resCode, "{MinInitValue}", #MinInitValue); \
        testCodegen("group_key:" #group_key ",agg_fun:min(" #field_name ")", resCode); \
        if (HasFatalFailure()) {                                        \
            return;                                                     \
        }                                                               \
    }

#define CASE_GROUP_KEY_ALL(field_name, MinItemType, MinExprType, \
                           MinRefSetFunc, MinExprFunc, MinInitValue)    \
    CASE_GROUP_KEY(attr_int8, byte, ByteAggItemMap, getInt8, setInt8,   \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_int16, short, ShortAggItemMap, getInt16, setInt16, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_int32, int, IntAggItemMap, getInt32, setInt32,  \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_int64, long, LongAggItemMap, getInt64, setInt64, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_uint8, ubyte, UByteAggItemMap, getUInt8, setUInt8, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue);\
    CASE_GROUP_KEY(attr_uint16, ushort, UShortAggItemMap, getUInt16, setUInt16, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_uint32, uint, UIntAggItemMap, getUInt32, setUInt32, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_uint64, ulong, ULongAggItemMap, getUInt64, setUInt64, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_float, float, FloatAggItemMap, getFloat, setFloat, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_double, double, DoubleAggItemMap, getDouble, setDouble, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_string, MChar, MCharAggItemMap, getMChar, setSTLString, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue);

    CASE_GROUP_KEY_ALL(attr_int8, byte, byte, setInt8, getInt8, 127);
    CASE_GROUP_KEY_ALL(attr_int16, short, short, setInt16, getInt16, 32767);
    CASE_GROUP_KEY_ALL(attr_int32, int, int, setInt32, getInt32, 2147483647);
    CASE_GROUP_KEY_ALL(attr_int64, long, long, setInt64, getInt64, 9223372036854775807L);
    CASE_GROUP_KEY_ALL(attr_uint8, ubyte, ubyte, setUInt8, getUInt8, 255);
    CASE_GROUP_KEY_ALL(attr_uint16, ushort, ushort, setUInt16, getUInt16, 65535);
    CASE_GROUP_KEY_ALL(attr_uint32, uint, uint, setUInt32, getUInt32, -1);
    CASE_GROUP_KEY_ALL(attr_uint64, ulong, ulong, setUInt64, getUInt64, -1L);
    CASE_GROUP_KEY_ALL(attr_float, float, float, setFloat, getFloat,
                       340282346638528859811704183484516925440.000000F);
    CASE_GROUP_KEY_ALL(attr_double, double, double, setDouble, getDouble,
                       179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000D);

#undef CASE_GROUP_KEY_ALL
#undef CASE_GROUP_KEY
}


TEST_F(TEST_CLASS_NAME, testMinMultipleValue) {
    const std::string templateStr = R"(package ha3;
import unsafe.*;

class AggItem {
    {MinItemType} min0;
    {GroupKeyType} groupKey;
};

class JitAggregator {

    public AggItemAllocator allocator;
    public AttributeExpression groupKeyExpr;
    public uint maxSortCount;
    public {AggItemMapType} itemMap;
    public Reference groupKeyRef;
    public Reference minRef0;
    public AttributeExpression minExpr0;

    public uint count() {
        return itemMap.size();
    }

    static public JitAggregator create(Aggregator aggregator) {
        JitAggregator jitAggregator = new JitAggregator();
        jitAggregator.groupKeyRef = aggregator.getGroupKeyRef();
        jitAggregator.groupKeyExpr = aggregator.getGroupKeyExpr();
        jitAggregator.maxSortCount = aggregator.getMaxSortCount();
        jitAggregator.allocator = AggItemAllocator.create((uint)16);
        jitAggregator.itemMap = {AggItemMapType}.create();
        jitAggregator.minRef0 = aggregator.getAggFunctionRef((uint)0, (uint)0);
        jitAggregator.minExpr0 = aggregator.getAggFunctionExpr((uint)0);
        return jitAggregator;
    }

    public void batch(MatchDocs docs, uint size) {
        {GroupExprType} groupKey = null;
        for (uint i = 0; i < size; ++i) {
            MatchDoc doc = docs.get(i);
            groupKey = groupKeyExpr.{GroupKeyGetFunc}(doc, groupKey);
            for (uint i = 0; i < groupKey.size(); i++) {
                {GroupKeyType} key = groupKey.getWithoutCheck(i);
                AggItem item = (AggItem)itemMap.get(key);
                if (item == null) {
                    if (itemMap.size() >= maxSortCount) {
                        continue;
                    }
                    item = (AggItem)allocator.alloc();
                    item.groupKey = key;
                    item.min0 = {MinInitValue};
                    itemMap.add(key, (Any)item);
                }
                {MinExprType} min0 = minExpr0.{MinExprFunc}(doc);
                if (min0 < item.min0) item.min0 = min0;
            }
        }
    }

    public void result(MatchDocs docs, uint size) {
        uint i = 0;
        for (Any any = itemMap.begin(); any != null && i < size; any = itemMap.next()) {
            AggItem item = (AggItem)any;
            MatchDoc doc = docs.get(i);
            minRef0.{MinRefSetFunc}(doc, item.min0);
            groupKeyRef.{GroupKeyRefSetFunc}(doc, item.groupKey);
            ++i;
        }
    }
}
)";
    // single value
#define CASE_GROUP_KEY(group_key, GroupExprType,                        \
                       GroupKeyType, AggItemMapType, GroupKeyGetFunc, GroupKeyRefSetFunc, \
                       field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue) \
    {                                                                   \
        std::string resCode = templateStr;                              \
        autil::StringUtil::replaceAll(resCode, "{MinItemType}", #MinItemType); \
        autil::StringUtil::replaceAll(resCode, "{MinExprType}", #MinExprType); \
        autil::StringUtil::replaceAll(resCode, "{MinRefSetFunc}", #MinRefSetFunc); \
        autil::StringUtil::replaceAll(resCode, "{MinExprFunc}", #MinExprFunc); \
        autil::StringUtil::replaceAll(resCode, "{MinInitValue}", #MinInitValue); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", #GroupKeyType); \
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", #GroupExprType); \
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", #AggItemMapType); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", #GroupKeyGetFunc); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", #GroupKeyRefSetFunc); \
        testCodegen("group_key:" #group_key ",agg_fun:min(" #field_name ")", resCode); \
        if (HasFatalFailure()) {                                        \
            return;                                                     \
        }                                                               \
    }

#define CASE_GROUP_KEY_ALL(field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue) \
    CASE_GROUP_KEY(attr_mint8, MInt8, byte, ByteAggItemMap, getMInt8, setInt8, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_mint16, MInt16, short, ShortAggItemMap, getMInt16, setInt16, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_mint32, MInt32, int, IntAggItemMap, getMInt32, setInt32, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_mint64, MInt64, long, LongAggItemMap, getMInt64, setInt64, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_muint8, MUInt8, ubyte, UByteAggItemMap, getMUInt8, setUInt8, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_muint16, MUInt16, ushort, UShortAggItemMap, getMUInt16, setUInt16, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_muint32, MUInt32, uint, UIntAggItemMap, getMUInt32, setUInt32, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_muint64, MUInt64, ulong, ULongAggItemMap, getMUInt64, setUInt64, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_mfloat, MFloat, float, FloatAggItemMap, getMFloat, setFloat, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_mdouble, MDouble, double, DoubleAggItemMap, getMDouble, setDouble, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue); \
    CASE_GROUP_KEY(attr_mstring, MString, MChar, MCharAggItemMap, getMString, setSTLString, \
                   field_name, MinItemType, MinExprType, MinRefSetFunc, MinExprFunc, MinInitValue);

    CASE_GROUP_KEY_ALL(attr_int8, byte, byte, setInt8, getInt8, 127);
    CASE_GROUP_KEY_ALL(attr_int16, short, short, setInt16, getInt16, 32767);
    CASE_GROUP_KEY_ALL(attr_int32, int, int, setInt32, getInt32, 2147483647);
    CASE_GROUP_KEY_ALL(attr_int64, long, long, setInt64, getInt64, 9223372036854775807L);
    CASE_GROUP_KEY_ALL(attr_uint8, ubyte, ubyte, setUInt8, getUInt8, 255);
    CASE_GROUP_KEY_ALL(attr_uint16, ushort, ushort, setUInt16, getUInt16, 65535);
    CASE_GROUP_KEY_ALL(attr_uint32, uint, uint, setUInt32, getUInt32, -1);
    CASE_GROUP_KEY_ALL(attr_uint64, ulong, ulong, setUInt64, getUInt64, -1L);
    CASE_GROUP_KEY_ALL(attr_float, float, float, setFloat, getFloat,
                       340282346638528859811704183484516925440.000000F);
    CASE_GROUP_KEY_ALL(attr_double, double, double, setDouble, getDouble,
                       179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000D);

#undef CASE_GROUP_KEY_ALL
#undef CASE_GROUP_KEY
}

TEST_F(TEST_CLASS_NAME, testMaxSingleValue) {
    const std::string templateStr = R"(package ha3;
import unsafe.*;

class AggItem {
    {MaxItemType} max0;
    {GroupKeyType} groupKey;
};

class JitAggregator {

    public AggItemAllocator allocator;
    public AttributeExpression groupKeyExpr;
    public uint maxSortCount;
    public {AggItemMapType} itemMap;
    public Reference groupKeyRef;
    public Reference maxRef0;
    public AttributeExpression maxExpr0;

    public uint count() {
        return itemMap.size();
    }

    static public JitAggregator create(Aggregator aggregator) {
        JitAggregator jitAggregator = new JitAggregator();
        jitAggregator.groupKeyRef = aggregator.getGroupKeyRef();
        jitAggregator.groupKeyExpr = aggregator.getGroupKeyExpr();
        jitAggregator.maxSortCount = aggregator.getMaxSortCount();
        jitAggregator.allocator = AggItemAllocator.create((uint)16);
        jitAggregator.itemMap = {AggItemMapType}.create();
        jitAggregator.maxRef0 = aggregator.getAggFunctionRef((uint)0, (uint)0);
        jitAggregator.maxExpr0 = aggregator.getAggFunctionExpr((uint)0);
        return jitAggregator;
    }

    public void batch(MatchDocs docs, uint size) {
        for (uint i = 0; i < size; ++i) {
            MatchDoc doc = docs.get(i);
            {GroupKeyType} key = groupKeyExpr.{GroupKeyGetFunc}(doc);
            AggItem item = (AggItem)itemMap.get(key);
            if (item == null) {
                if (itemMap.size() >= maxSortCount) {
                    continue;
                }
                item = (AggItem)allocator.alloc();
                item.groupKey = key;
                item.max0 = {MaxInitValue};
                itemMap.add(key, (Any)item);
            }
            {MaxExprType} max0 = maxExpr0.{MaxExprFunc}(doc);
            if (max0 > item.max0) item.max0 = max0;
        }
    }

    public void result(MatchDocs docs, uint size) {
        uint i = 0;
        for (Any any = itemMap.begin(); any != null && i < size; any = itemMap.next()) {
            AggItem item = (AggItem)any;
            MatchDoc doc = docs.get(i);
            maxRef0.{MaxRefSetFunc}(doc, item.max0);
            groupKeyRef.{GroupKeyRefSetFunc}(doc, item.groupKey);
            ++i;
        }
    }
}
)";
    // single value
#define CASE_GROUP_KEY(group_key,                                       \
                       GroupKeyType, AggItemMapType, GroupKeyGetFunc, GroupKeyRefSetFunc, \
                       field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, \
                       MaxInitValue)                                    \
    {                                                                   \
        std::string resCode = templateStr;                              \
        autil::StringUtil::replaceAll(resCode, "{MaxItemType}", #MaxItemType); \
        autil::StringUtil::replaceAll(resCode, "{MaxExprType}", #MaxExprType); \
        autil::StringUtil::replaceAll(resCode, "{MaxRefSetFunc}", #MaxRefSetFunc); \
        autil::StringUtil::replaceAll(resCode, "{MaxInitValue}", #MaxInitValue); \
        autil::StringUtil::replaceAll(resCode, "{MaxExprFunc}", #MaxExprFunc); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", #GroupKeyType); \
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", #AggItemMapType); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", #GroupKeyGetFunc); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", #GroupKeyRefSetFunc); \
        testCodegen("group_key:" #group_key ",agg_fun:max(" #field_name ")", resCode); \
        if (HasFatalFailure()) {                                        \
            return;                                                     \
        }                                                               \
    }

#define CASE_GROUP_KEY_ALL(field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue) \
    CASE_GROUP_KEY(attr_int8, byte, ByteAggItemMap, getInt8, setInt8,   \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_int16, short, ShortAggItemMap, getInt16, setInt16, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_int32, int, IntAggItemMap, getInt32, setInt32,  \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_int64, long, LongAggItemMap, getInt64, setInt64, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_uint8, ubyte, UByteAggItemMap, getUInt8, setUInt8, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_uint16, ushort, UShortAggItemMap, getUInt16, setUInt16, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_uint32, uint, UIntAggItemMap, getUInt32, setUInt32, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_uint64, ulong, ULongAggItemMap, getUInt64, setUInt64, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_float, float, FloatAggItemMap, getFloat, setFloat, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_double, double, DoubleAggItemMap, getDouble, setDouble, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_string, MChar, MCharAggItemMap, getMChar, setSTLString, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue);

    CASE_GROUP_KEY_ALL(attr_int8, byte, byte, setInt8, getInt8, -128);
    CASE_GROUP_KEY_ALL(attr_int16, short, short, setInt16, getInt16, -32768);
    CASE_GROUP_KEY_ALL(attr_int32, int, int, setInt32, getInt32, -2147483648);
    CASE_GROUP_KEY_ALL(attr_int64, long, long, setInt64, getInt64, -(1L<<63));
    CASE_GROUP_KEY_ALL(attr_uint8, ubyte, ubyte, setUInt8, getUInt8, 0);
    CASE_GROUP_KEY_ALL(attr_uint16, ushort, ushort, setUInt16, getUInt16, 0);
    CASE_GROUP_KEY_ALL(attr_uint32, uint, uint, setUInt32, getUInt32, 0);
    CASE_GROUP_KEY_ALL(attr_uint64, ulong, ulong, setUInt64, getUInt64, 0);
    CASE_GROUP_KEY_ALL(attr_float, float, float, setFloat, getFloat,
                       -340282346638528859811704183484516925440.000000F);
    CASE_GROUP_KEY_ALL(attr_double, double, double, setDouble, getDouble,
                       -179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000D);

#undef CASE_GROUP_KEY_ALL
#undef CASE_GROUP_KEY
}


TEST_F(TEST_CLASS_NAME, testMaxMultipleValue) {
    const std::string templateStr = R"(package ha3;
import unsafe.*;

class AggItem {
    {MaxItemType} max0;
    {GroupKeyType} groupKey;
};

class JitAggregator {

    public AggItemAllocator allocator;
    public AttributeExpression groupKeyExpr;
    public uint maxSortCount;
    public {AggItemMapType} itemMap;
    public Reference groupKeyRef;
    public Reference maxRef0;
    public AttributeExpression maxExpr0;

    public uint count() {
        return itemMap.size();
    }

    static public JitAggregator create(Aggregator aggregator) {
        JitAggregator jitAggregator = new JitAggregator();
        jitAggregator.groupKeyRef = aggregator.getGroupKeyRef();
        jitAggregator.groupKeyExpr = aggregator.getGroupKeyExpr();
        jitAggregator.maxSortCount = aggregator.getMaxSortCount();
        jitAggregator.allocator = AggItemAllocator.create((uint)16);
        jitAggregator.itemMap = {AggItemMapType}.create();
        jitAggregator.maxRef0 = aggregator.getAggFunctionRef((uint)0, (uint)0);
        jitAggregator.maxExpr0 = aggregator.getAggFunctionExpr((uint)0);
        return jitAggregator;
    }

    public void batch(MatchDocs docs, uint size) {
        {GroupExprType} groupKey = null;
        for (uint i = 0; i < size; ++i) {
            MatchDoc doc = docs.get(i);
            groupKey = groupKeyExpr.{GroupKeyGetFunc}(doc, groupKey);
            for (uint i = 0; i < groupKey.size(); i++) {
                {GroupKeyType} key = groupKey.getWithoutCheck(i);
                AggItem item = (AggItem)itemMap.get(key);
                if (item == null) {
                    if (itemMap.size() >= maxSortCount) {
                        continue;
                    }
                    item = (AggItem)allocator.alloc();
                    item.groupKey = key;
                    item.max0 = {MaxInitValue};
                    itemMap.add(key, (Any)item);
                }
                {MaxExprType} max0 = maxExpr0.{MaxExprFunc}(doc);
                if (max0 > item.max0) item.max0 = max0;
            }
        }
    }

    public void result(MatchDocs docs, uint size) {
        uint i = 0;
        for (Any any = itemMap.begin(); any != null && i < size; any = itemMap.next()) {
            AggItem item = (AggItem)any;
            MatchDoc doc = docs.get(i);
            maxRef0.{MaxRefSetFunc}(doc, item.max0);
            groupKeyRef.{GroupKeyRefSetFunc}(doc, item.groupKey);
            ++i;
        }
    }
}
)";
    // single value
#define CASE_GROUP_KEY(group_key, GroupExprType,                        \
                       GroupKeyType, AggItemMapType, GroupKeyGetFunc, GroupKeyRefSetFunc, \
                       field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue) \
    {                                                                   \
        std::string resCode = templateStr;                              \
        autil::StringUtil::replaceAll(resCode, "{MaxItemType}", #MaxItemType); \
        autil::StringUtil::replaceAll(resCode, "{MaxExprType}", #MaxExprType); \
        autil::StringUtil::replaceAll(resCode, "{MaxRefSetFunc}", #MaxRefSetFunc); \
        autil::StringUtil::replaceAll(resCode, "{MaxInitValue}", #MaxInitValue); \
        autil::StringUtil::replaceAll(resCode, "{MaxExprFunc}", #MaxExprFunc); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyType}", #GroupKeyType); \
        autil::StringUtil::replaceAll(resCode, "{GroupExprType}", #GroupExprType); \
        autil::StringUtil::replaceAll(resCode, "{AggItemMapType}", #AggItemMapType); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyGetFunc}", #GroupKeyGetFunc); \
        autil::StringUtil::replaceAll(resCode, "{GroupKeyRefSetFunc}", #GroupKeyRefSetFunc); \
        testCodegen("group_key:" #group_key ",agg_fun:max(" #field_name ")", resCode); \
        if (HasFatalFailure()) {                                        \
            return;                                                     \
        }                                                               \
    }

#define CASE_GROUP_KEY_ALL(field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue) \
    CASE_GROUP_KEY(attr_mint8, MInt8, byte, ByteAggItemMap, getMInt8, setInt8, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_mint16, MInt16, short, ShortAggItemMap, getMInt16, setInt16, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_mint32, MInt32, int, IntAggItemMap, getMInt32, setInt32, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_mint64, MInt64, long, LongAggItemMap, getMInt64, setInt64, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_muint8, MUInt8, ubyte, UByteAggItemMap, getMUInt8, setUInt8, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_muint16, MUInt16, ushort, UShortAggItemMap, getMUInt16, setUInt16, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_muint32, MUInt32, uint, UIntAggItemMap, getMUInt32, setUInt32, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_muint64, MUInt64, ulong, ULongAggItemMap, getMUInt64, setUInt64, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_mfloat, MFloat, float, FloatAggItemMap, getMFloat, setFloat, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_mdouble, MDouble, double, DoubleAggItemMap, getMDouble, setDouble, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue); \
    CASE_GROUP_KEY(attr_mstring, MString, MChar, MCharAggItemMap, getMString, setSTLString, \
                   field_name, MaxItemType, MaxExprType, MaxRefSetFunc, MaxExprFunc, MaxInitValue);

    CASE_GROUP_KEY_ALL(attr_int8, byte, byte, setInt8, getInt8, -128);
    CASE_GROUP_KEY_ALL(attr_int16, short, short, setInt16, getInt16, -32768);
    CASE_GROUP_KEY_ALL(attr_int32, int, int, setInt32, getInt32, -2147483648);
    CASE_GROUP_KEY_ALL(attr_int64, long, long, setInt64, getInt64, -(1L<<63));
    CASE_GROUP_KEY_ALL(attr_uint8, ubyte, ubyte, setUInt8, getUInt8, 0);
    CASE_GROUP_KEY_ALL(attr_uint16, ushort, ushort, setUInt16, getUInt16, 0);
    CASE_GROUP_KEY_ALL(attr_uint32, uint, uint, setUInt32, getUInt32, 0);
    CASE_GROUP_KEY_ALL(attr_uint64, ulong, ulong, setUInt64, getUInt64, 0);
    CASE_GROUP_KEY_ALL(attr_float, float, float, setFloat, getFloat,
                       -340282346638528859811704183484516925440.000000F);
    CASE_GROUP_KEY_ALL(attr_double, double, double, setDouble, getDouble,
                       -179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000D);

#undef CASE_GROUP_KEY_ALL
#undef CASE_GROUP_KEY
}

TEST_F(TEST_CLASS_NAME, testMixedAggFunctionSingleValueAsGroupKey) {
    // test count
    const std::string resCode = R"(package ha3;
import unsafe.*;

class AggItem {
    long count0;
    long sum1;
    short min2;
    int max3;
    byte groupKey;
};

class JitAggregator {

    public AggItemAllocator allocator;
    public AttributeExpression groupKeyExpr;
    public uint maxSortCount;
    public ByteAggItemMap itemMap;
    public Reference groupKeyRef;
    public Reference countRef0;
    public Reference sumRef1;
    public Reference minRef2;
    public Reference maxRef3;
    public AttributeExpression sumExpr1;
    public AttributeExpression minExpr2;
    public AttributeExpression maxExpr3;

    public uint count() {
        return itemMap.size();
    }

    static public JitAggregator create(Aggregator aggregator) {
        JitAggregator jitAggregator = new JitAggregator();
        jitAggregator.groupKeyRef = aggregator.getGroupKeyRef();
        jitAggregator.groupKeyExpr = aggregator.getGroupKeyExpr();
        jitAggregator.maxSortCount = aggregator.getMaxSortCount();
        jitAggregator.allocator = AggItemAllocator.create((uint)40);
        jitAggregator.itemMap = ByteAggItemMap.create();
        jitAggregator.countRef0 = aggregator.getAggFunctionRef((uint)0, (uint)0);
        jitAggregator.sumRef1 = aggregator.getAggFunctionRef((uint)1, (uint)0);
        jitAggregator.minRef2 = aggregator.getAggFunctionRef((uint)2, (uint)0);
        jitAggregator.maxRef3 = aggregator.getAggFunctionRef((uint)3, (uint)0);
        jitAggregator.sumExpr1 = aggregator.getAggFunctionExpr((uint)1);
        jitAggregator.minExpr2 = aggregator.getAggFunctionExpr((uint)2);
        jitAggregator.maxExpr3 = aggregator.getAggFunctionExpr((uint)3);
        return jitAggregator;
    }

    public void batch(MatchDocs docs, uint size) {
        for (uint i = 0; i < size; ++i) {
            MatchDoc doc = docs.get(i);
            byte key = groupKeyExpr.getInt8(doc);
            AggItem item = (AggItem)itemMap.get(key);
            if (item == null) {
                if (itemMap.size() >= maxSortCount) {
                    continue;
                }
                item = (AggItem)allocator.alloc();
                item.groupKey = key;
                item.count0 = 0;
                item.sum1 = 0;
                item.min2 = 32767;
                item.max3 = -2147483648;
                itemMap.add(key, (Any)item);
            }
            byte sum1 = sumExpr1.getInt8(doc);
            short min2 = minExpr2.getInt16(doc);
            int max3 = maxExpr3.getInt32(doc);
            item.count0 += 1;
            item.sum1 += sum1;
            if (min2 < item.min2) item.min2 = min2;
            if (max3 > item.max3) item.max3 = max3;
        }
    }

    public void result(MatchDocs docs, uint size) {
        uint i = 0;
        for (Any any = itemMap.begin(); any != null && i < size; any = itemMap.next()) {
            AggItem item = (AggItem)any;
            MatchDoc doc = docs.get(i);
            countRef0.setInt64(doc, item.count0);
            sumRef1.setInt64(doc, item.sum1);
            minRef2.setInt16(doc, item.min2);
            maxRef3.setInt32(doc, item.max3);
            groupKeyRef.setInt8(doc, item.groupKey);
            ++i;
        }
    }
}
)";
    // single value
    ASSERT_NO_FATAL_FAILURE(testCodegen(
            "group_key:attr_int8,agg_fun:count()#sum(attr_int8)#min(attr_int16)#max(attr_int32)",
            resCode));
}

END_HA3_NAMESPACE();
