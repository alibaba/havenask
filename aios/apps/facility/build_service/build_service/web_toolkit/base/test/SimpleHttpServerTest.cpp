#include "build_service/web_toolkit/base/SimpleHttpServer.h"

#include "autil/StringUtil.h"
#include "build_service/test/unittest.h"

using namespace std;

namespace build_service { namespace web_toolkit {

class SimpleHttpServerTest : public TESTBASE
{
public:
    SimpleHttpServerTest();
    ~SimpleHttpServerTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(build_service.web_toolkit, SimpleHttpServerTest);

SimpleHttpServerTest::SimpleHttpServerTest() {}

SimpleHttpServerTest::~SimpleHttpServerTest() {}

class DemoTemplateDataAccessor : public TemplateDataAccessor
{
public:
    class DemoTemplateData : public TemplateData
    {
    public:
        DemoTemplateData(const vector<string>& data) : _data(data) {}

        autil::Result<std::string> toString() override
        {
            nlohmann::json j;
            j["guests"] = _data;
            return autil::Result<std::string>(j.dump());
        }

    private:
        vector<string> _data;
    };

public:
    DemoTemplateDataAccessor(const vector<string>& data) : _data(data) {}

    std::shared_ptr<TemplateData> extractData(const std::string& routePath, const httplib::Request& req) const override
    {
        auto ret = make_shared<DemoTemplateData>(_data);
        return ret;
    }

private:
    vector<string> _data;
};

TEST_F(SimpleHttpServerTest, TestSimpleProcess)
{
    vector<string> demoData = {"Tom", "Jeff", "Lucy", "Tony"};
    auto accessor = std::make_shared<DemoTemplateDataAccessor>(demoData);
    SimpleHttpServer httpServer(8, GET_PRIVATE_TEST_DATA_PATH(), accessor);

    // set html template handler
    ASSERT_TRUE(httpServer.registerHtmlTemplateHandler("/demo", "demo.html"));
    string modifyStr = R"(
<h1>AJAX</h1>
蒲坚 test)";
    auto modifyFunc = [&](const httplib::Request& req, httplib::Response& res) {
        res.set_content(modifyStr, "text/html;charset=utf-8");
    };
    ASSERT_TRUE(httpServer.registerRouteHandler("/modify", modifyFunc));

    // set udf route handler
    auto loginFunc = [&](const httplib::Request& req, httplib::Response& res) {
        string loginStr = R"(
<h1>登陆成功</h1>
)";
        if (req.has_param("username")) {
            loginStr += "<h2> username:";
            loginStr += req.get_param_value("username");
            loginStr += "</h2>";
        }

        if (req.has_param("pwd")) {
            loginStr += "<h2> password:";
            loginStr += req.get_param_value("pwd");
            loginStr += "</h2>";
        }
        res.set_content(loginStr, "text/html;charset=utf-8");
    };

    httpServer.registerRouteHandler("/login", loginFunc);

    // set error handler
    auto errorHandler = [](const httplib::Request& req, httplib::Response& res) {
        res.set_content("<h1>ERROR</h1>", "text/html;charset=utf-8");
    };
    httpServer.setErrorHandler(errorHandler);

    ASSERT_TRUE(httpServer.start(8888));

    // wait until running
    int cnt = 0;
    while (cnt <= 50 && !httpServer.isRunning()) {
        usleep(50 * 1000);
        ++cnt;
    }
    ASSERT_TRUE(httpServer.isRunning());

    string ipSpec = "http://localhost:" + autil::StringUtil::toString(httpServer.getPort());
    httplib::Client cli(ipSpec);
    auto result = cli.Get("/demo");

    string retData = result->body;
    // std::cout << retData << std::endl;
    //  sleep(300);

    ASSERT_TRUE(retData.find("Tom") != string::npos);
    ASSERT_TRUE(retData.find("Jeff") != string::npos);
    ASSERT_TRUE(retData.find("Lucy") != string::npos);
    ASSERT_TRUE(retData.find("Tony") != string::npos);
    httpServer.stop();
    // wait until stopped
    cnt = 0;
    while (cnt <= 50 && httpServer.isRunning()) {
        usleep(50 * 1000);
        ++cnt;
    }
    ASSERT_FALSE(httpServer.isRunning());
}

}} // namespace build_service::web_toolkit
