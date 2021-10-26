package org.apache.flink.statefun.flink.core.message;

import com.esotericsoftware.minlog.Log;
import javafx.util.Pair;

import org.apache.flink.statefun.sdk.*;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.sdk.utils.DataflowUtils;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedStateRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class MessageHandlingFunction extends BaseStatefulFunction {
    private static final Logger LOG = LoggerFactory.getLogger(MessageHandlingFunction.class);
    private final HashMap<String, StatefulFunction> registeredFunctions = new HashMap<>();
    private final HashMap<String, List<Pair<Integer, FunctionInvocation>>> pendingInvocations = new HashMap<>();
    @Persisted
    PersistedStateRegistry provider;
    private String resuableFunctionId = null;

    public MessageHandlingFunction(int stateMapSize) {
        if (stateMapSize != -1) {
            Log.info("Initialize State registry with size " + stateMapSize);
            provider = new PersistedStateRegistry(stateMapSize);
        } else {
            Log.info("Initialize State registry with size " + stateMapSize);
            provider = new PersistedStateRegistry();
        }
    }

    @Override
    public void invoke(Context context, Object input) {
        if (input instanceof FunctionRegistration) {
            FunctionRegistration registration = (FunctionRegistration) input;
            String functionId = DataflowUtils.typeToFunctionTypeString(registration.functionType);
            LOG.info("Register function request " + input + " functionType " + functionId
                    + " statefulFunction " + registration.statefulFunction + " self "
                    + context.self() + " caller " + context.caller());// +
            if (!registeredFunctions.containsKey(functionId)) {
                if (registration.statefulFunction instanceof DerivedStatefulFunction) {
                    ((DerivedStatefulFunction) registration.statefulFunction).setBaseFunction(this);
                }
                registeredFunctions.put(functionId, registration.statefulFunction);
                if (pendingInvocations.containsKey(functionId)) {
                    for (Pair<Integer, FunctionInvocation> f : pendingInvocations.get(functionId)) {
                        Log.info("Functiontype [" + functionId + "] consumes pending message"
                                + f.getValue().messageWrapper);
                        resuableFunctionId = functionId;
                        ((ReusableContext) context).setVirtualizedIndex(f.getKey());
                        registration.statefulFunction.invoke(context, f.getValue().messageWrapper);
                        resuableFunctionId = null;
                    }
                    pendingInvocations.remove(functionId);
                }
            }
        } else if (input instanceof FunctionInvocation) {
            context.setStateProvider(provider);
            FunctionInvocation localInvocation = (FunctionInvocation) input;
            String functionId = DataflowUtils.typeToFunctionTypeString(localInvocation.functionType);
            StatefulFunction function = registeredFunctions.getOrDefault(functionId, null);
            if (function == null) {
                Log.info("Functiontype [" + functionId + "] is never registered at " + this + " ["
                        + context.self() + "] [" + context.caller()
                        + "] function type [" + functionId
                        + "] ");
                if (!pendingInvocations.containsKey(functionId))
                    pendingInvocations.put(functionId, new ArrayList<>());
                pendingInvocations
                        .get(functionId)
                        .add(new Pair<>(
                                ((ReusableContext) context).getVirtualizedIndex(),
                                localInvocation));
            } else {
                resuableFunctionId = functionId;
                function.invoke(context, localInvocation.messageWrapper);
                resuableFunctionId = null;
            }
        }
    }

    @Override
    public boolean statefulSubFunction(Address address) {
        if (address.type().getInternalType() != null) {
            String functionId = DataflowUtils.typeToFunctionTypeString(address
                    .type()
                    .getInternalType());
            StatefulFunction function = registeredFunctions.get(functionId);
            if (function instanceof DerivedStatefulFunction) {
                return ((DerivedStatefulFunction) function).ifStateful();
            }
        }
        return false;
    }

    @Override
    public String getCurrentFunctionid() {
        return resuableFunctionId;
    }

}
