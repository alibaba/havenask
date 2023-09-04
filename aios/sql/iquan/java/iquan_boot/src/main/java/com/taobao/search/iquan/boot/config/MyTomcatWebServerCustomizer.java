package com.taobao.search.iquan.boot.config;

import org.apache.coyote.AbstractProtocol;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

@Component
public class MyTomcatWebServerCustomizer implements WebServerFactoryCustomizer<TomcatServletWebServerFactory> {

    @Override
    public void customize(TomcatServletWebServerFactory factory) {

        factory.addConnectorCustomizers(connector -> {

            connector.setAllowTrace(false);
            connector.setAsyncTimeout(1000); // ms
            connector.setEnableLookups(false);
            connector.setMaxPostSize(1048576); // 1M
            connector.setURIEncoding("UTF-8");
            connector.setUseBodyEncodingForURI(false);

            AbstractProtocol<?> abstractProtocol = (AbstractProtocol) connector.getProtocolHandler();
            abstractProtocol.setAcceptCount(200); // request queue
            abstractProtocol.setAcceptorThreadCount(2);
            abstractProtocol.setAcceptorThreadPriority(5);

            //abstractProtocol.setConnectionTimeout(300); // ms
            //abstractProtocol.setKeepAliveTimeout(3000); // ms
            //abstractProtocol.setMaxConnections(1000);
            //abstractProtocol.setMaxThreads(128);
            //abstractProtocol.setMinSpareThreads(64);
            abstractProtocol.setTcpNoDelay(true);
        });
    }
}

