package com.anilyanai.engine.configuration;

import com.anilyanai.engine.session.SessionRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class SessionEngineAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SessionRegistry sessionRegistry() {
        return new SessionRegistry();
    }
}
