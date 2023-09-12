package statefun

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
)

// helper to create a protocol Address from an Address
func toProtocolAddress(address *Address) *protocol.Address {
	if address != nil {
		return &protocol.Address{
			Namespace: address.FunctionType.GetNamespace(),
			Type:      address.FunctionType.GetType(),
			Id:        address.Id,
		}
	} else {
		return nil
	}
}

// helper to create a handler and invoke the function
func invokeStatefulFunction(ctx context.Context, target *Address, caller *Address, argument *protocol.TypedValue, statefulFunction StatefulFunction) error {

	builder := StatefulFunctionsBuilder()
	err := builder.WithSpec(StatefulFunctionSpec{
		FunctionType: target.FunctionType,
		Function:     statefulFunction,
	})
	if err != nil {
		return err
	}

	toFunction := protocol.ToFunction{
		Request: &protocol.ToFunction_Invocation_{
			Invocation: &protocol.ToFunction_InvocationBatchRequest{
				Target: toProtocolAddress(target),
				Invocations: []*protocol.ToFunction_Invocation{
					{
						Caller:   toProtocolAddress(caller),
						Argument: argument,
					},
				},
			},
		},
	}

	bytes, err := proto.Marshal(&toFunction)
	if err != nil {
		return err
	}

	_, err = builder.AsHandler().Invoke(ctx, bytes)
	if err != nil {
		return err
	}

	return nil
}

func TestStatefunHandler_WithNoCaller_ContextCallerIsNil(t *testing.T) {

	target := Address{FunctionType: TypeNameFrom("namespace/function1"), Id: "1"}

	statefulFunction := func(ctx Context, message Message) error {
		assert.Nil(t, ctx.Caller())
		return nil
	}

	err := invokeStatefulFunction(context.Background(), &target, nil, nil, StatefulFunctionPointer(statefulFunction))
	assert.Nil(t, err)
}

func TestStatefunHandler_WithCaller_ContextCallerIsCorrect(t *testing.T) {

	target := Address{FunctionType: TypeNameFrom("namespace/function1"), Id: "1"}
	caller := Address{FunctionType: TypeNameFrom("namespace/function2"), Id: "2"}

	statefulFunction := func(ctx Context, message Message) error {
		assert.Equal(t, caller.String(), ctx.Caller().String())
		return nil
	}

	err := invokeStatefulFunction(context.Background(), &target, &caller, nil, StatefulFunctionPointer(statefulFunction))
	assert.Nil(t, err)
}

func TestStatefulFunctionsBuilder_FunctionTypeRequired(t *testing.T) {
	caller := Address{FunctionType: TypeNameFrom("namespace/function2"), Id: "2"}

	statefulFunction := func(ctx Context, message Message) error {
		assert.Equal(t, caller.String(), ctx.Caller().String())
		return nil
	}

	builder := StatefulFunctionsBuilder()
	err := builder.WithSpec(StatefulFunctionSpec{
		Function: StatefulFunctionPointer(statefulFunction),
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "function type is required")
}
