package org.apache.flink.statefun.flink.core.message;

import javafx.util.Pair;
import org.apache.flink.statefun.sdk.*;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.sdk.utils.DataflowUtils;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedStateRegistry;
import java.util.*;

public final class MessageHandlingFunction extends BaseStatefulFunction {
    private final HashMap<String, StatefulFunction> registeredFunctions = new HashMap<>();
    //TODO
    private final HashMap<String, List<Pair<Integer, FunctionInvocation>>> pendingInvocations = new HashMap<>();
    @Persisted
    PersistedStateRegistry provider;
    private String resuableFunctionId = null;

    public MessageHandlingFunction(int stateMapSize) {
        if (stateMapSize != -1) {
            System.out.println("Initialize State registry with size " + stateMapSize);
            provider = new PersistedStateRegistry(stateMapSize);
        } else {
            System.out.println("Initialize State registry with size " + stateMapSize);
            provider = new PersistedStateRegistry();
        }
    }

    @Override
    public void invoke(Context context, Object input) {
        if (input instanceof FunctionRegistration) {
            FunctionRegistration registration = (FunctionRegistration) input;
            String functionId = DataflowUtils.typeToFunctionTypeString(registration.functionType);
            System.out.println("Register function request " + input + " functionType " + functionId
                    + " statefulFunction " + registration.statefulFunction + " self " + context.self() + " caller " + context.caller());// +
            if (!registeredFunctions.containsKey(functionId)) {
                if (registration.statefulFunction instanceof DerivedStatefulFunction) {
                    ((DerivedStatefulFunction) registration.statefulFunction).setBaseFunction(this);
                }
                registeredFunctions.put(functionId, registration.statefulFunction);
                if (pendingInvocations.containsKey(functionId)) {
                    for (Pair<Integer, FunctionInvocation> f : pendingInvocations.get(functionId)) {
                        System.out.println("Functiontype [" + functionId + "] consumes pending message" + f.getValue().messageWrapper);
                        resuableFunctionId = functionId;
                        ((ReusableContext) context).setVirtualizedIndex(f.getKey());
                        registration.statefulFunction.invoke(context, f.getValue().messageWrapper);
                        resuableFunctionId = null;
                    }
                    pendingInvocations.remove(functionId);
                }
            }
        } else if (input instanceof FunctionInvocation) {
//                Long currentMillis = System.currentTimeMillis();
            context.setStateProvider(provider);
            FunctionInvocation localInvocation = (FunctionInvocation) input;
            String functionId = DataflowUtils.typeToFunctionTypeString(localInvocation.functionType);
            StatefulFunction function = registeredFunctions.getOrDefault(functionId, null);
            if (function == null) {
                System.out.println("Functiontype [" + functionId + "] is never registered at " + this.toString() + " [" + context.self() + "] [" + context.caller()
                        + "] function type [" + functionId
                        + "] ");
                if (!pendingInvocations.containsKey(functionId)) pendingInvocations.put(functionId, new ArrayList<>());
                pendingInvocations.get(functionId).add(new Pair<>(((ReusableContext) context).getVirtualizedIndex(), localInvocation));
            } else {
                resuableFunctionId = functionId;
                function.invoke(context, localInvocation.messageWrapper);
                resuableFunctionId = null;
            }
//                System.out.println("MessageHandlingFunction time in millis " + (System.currentTimeMillis() - currentMillis)
//                + " FunctionId: " + functionId);
        }
    }

    @Override
    public boolean statefulSubFunction(Address address) {
        if (address.type().getInternalType() != null) {
            String functionId = DataflowUtils.typeToFunctionTypeString(address.type().getInternalType());
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
