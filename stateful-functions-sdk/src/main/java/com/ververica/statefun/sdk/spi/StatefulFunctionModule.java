/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.sdk.spi;

import com.ververica.statefun.sdk.FunctionType;
import com.ververica.statefun.sdk.StatefulFunction;
import com.ververica.statefun.sdk.StatefulFunctionProvider;
import com.ververica.statefun.sdk.io.EgressSpec;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import com.ververica.statefun.sdk.io.IngressSpec;
import com.ververica.statefun.sdk.io.Router;
import java.util.Map;

/**
 * A {@link StatefulFunctionModule} is the entry point for adding to a Stateful Functions
 * application the core building block primitives, i.e. {@link IngressSpec}s, {@link EgressSpec}s,
 * {@link Router}s, and {@link StatefulFunction}s.
 *
 * <h2>Extensibility of a Stateful Functions application</h2>
 *
 * <p>A Stateful Functions application is built up of ingresses, egresses, routers, and stateful
 * functions that are added to the application by multiple different {@link
 * StatefulFunctionModule}s. This allows different parts of the application to be contributed by
 * different modules; for example, one module may provide ingresses and egresses, while other
 * modules may individually contribute specific parts of the application as stateful functions.
 *
 * <p>The extensibility is achieved by leveraging the <a
 * href="https://docs.oracle.com/javase/tutorial/ext/basics/spi.html#the-serviceloader-class">Java
 * Service Loader</a>. In this context, each module is essentially a service provider.
 *
 * <h2>Registering a {@code StatefulFunctionModule}</h2>
 *
 * <p>In order for an application to discover a given module, likewise to how the Java Service
 * Loader works, a UTF-8 encoded provider configuration file needs to be stored in the {@code
 * MEGA-INF/services} directory of the module's containing JAR file. The name of the file should be
 * {@code com.ververica.statefun.sdk.spi.StatefulFunctionModule}, i.e. the fully qualified name of
 * the {@link StatefulFunctionModule} class. Each line in the file should be the fully qualified
 * class name of a module in that JAR that you want to register for the Stateful Functions
 * application.
 *
 * <p>Finally, to allow the Stateful Functions runtime to discover the registered modules, the JAR
 * files containing the modules and provider configuration files should be added to a
 * system-specific class path directory, {@code /opt/stateful-functions/modules/}.
 *
 * <p>For a simple demonstration, you can consult the {@code stateful-functions-greeter-example}
 * example.
 */
public interface StatefulFunctionModule {

  /**
   * This method is the entry point for extending a Stateful Functions application by binding
   * ingresses, egresses, routers, and functions.
   *
   * @param globalConfiguration global configuration of the Stateful Functions application.
   * @param binder the binder to be used to bind ingresses, egresses, routers, and functions.
   */
  void configure(Map<String, String> globalConfiguration, Binder binder);

  /**
   * A {@link Binder} binds ingresses, egresses, routers, and functions to a Stateful Functions
   * application.
   */
  interface Binder {

    /**
     * Binds an {@link IngressSpec} to the Stateful Functions application.
     *
     * @param spec the {@link IngressSpec} to bind.
     * @param <T> the output type of the ingress.
     */
    <T> void bindIngress(IngressSpec<T> spec);

    /**
     * Binds an {@link EgressSpec} to the Stateful Functions application.
     *
     * @param spec the {@link EgressSpec} to bind.
     * @param <T> the type of inputs that the egress consumes.
     */
    <T> void bindEgress(EgressSpec<T> spec);

    /**
     * Binds a {@link StatefulFunctionProvider} to the Stateful Functions application.
     *
     * @param functionType the type of functions that the {@link StatefulFunctionProvider} provides.
     * @param provider the provider to bind.
     */
    void bindFunctionProvider(FunctionType functionType, StatefulFunctionProvider provider);

    /**
     * Binds a {@link Router} for a given ingress to the Stateful Functions application.
     *
     * @param id the id of the ingress to bind the router to.
     * @param router the router to bind.
     * @param <T> the type of messages that is bbeing routed.
     */
    <T> void bindIngressRouter(IngressIdentifier<T> id, Router<T> router);
  }
}
