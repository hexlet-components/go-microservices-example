package compatdemo

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestEventVersionMatrix(t *testing.T) {
	for _, file := range []string{"v1.json", "v2.json"} {
		for name, consume := range map[string]func([]byte) (Report, error){"v1": ConsumeV1, "v2": ConsumeV2} {
			t.Run(file+"/"+name, func(t *testing.T) {
				data, err := os.ReadFile("fixtures/" + file)
				if err != nil {
					t.Fatal(err)
				}
				got, err := consume(data)
				if err != nil {
					t.Fatal(err)
				}
				want := Report{AuditID: "a-42", Ready: true, Findings: []Finding{{PageURL: "http://localhost:8090/about", Code: "missing-title"}}}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("report=%+v", got)
				}
			})
		}
	}
}
func TestDecodedButIncompatible(t *testing.T) {
	data, err := os.ReadFile("fixtures/broken-semantics.json")
	if err != nil {
		t.Fatal(err)
	}
	var e EventV1
	if err = json.Unmarshal(data, &e); err != nil {
		t.Fatalf("fixture must decode: %v", err)
	}
	if _, err = ConsumeV1(data); err == nil {
		t.Fatal("changed meaning accepted")
	}
}

// Private descriptors allow old and new schemas with the same Protobuf name
// in one test process. Production RPC service names remain unchanged.
func responseSchema(t *testing.T, newVersion bool) protoreflect.MessageDescriptor {
	t.Helper()
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	fields := []*descriptorpb.FieldDescriptorProto{
		{Name: proto.String("result_id"), Number: proto.Int32(1), Label: &optional, Type: &stringType},
		{Name: proto.String("audit_id"), Number: proto.Int32(2), Label: &optional, Type: &stringType},
	}
	message := &descriptorpb.DescriptorProto{Name: proto.String("GetResultResponse"), Field: fields}
	if newVersion {
		message.OneofDecl = []*descriptorpb.OneofDescriptorProto{{Name: proto.String("_rule_version")}}
		message.Field = append(message.Field, &descriptorpb.FieldDescriptorProto{Name: proto.String("rule_version"), Number: proto.Int32(4), Label: &optional, Type: &stringType, Proto3Optional: proto.Bool(true), OneofIndex: proto.Int32(0)})
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{Name: proto.String("checks.proto"), Package: proto.String("checks.v1"), Syntax: proto.String("proto3"), MessageType: []*descriptorpb.DescriptorProto{message}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return file.Messages().Get(0)
}
func TestProtobufVersionMatrix(t *testing.T) {
	old := responseSchema(t, false)
	updated := responseSchema(t, true)
	for _, sender := range []protoreflect.MessageDescriptor{old, updated} {
		for _, receiver := range []protoreflect.MessageDescriptor{old, updated} {
			source := dynamicpb.NewMessage(sender)
			source.Set(sender.Fields().ByNumber(1), protoreflect.ValueOfString("r-42"))
			source.Set(sender.Fields().ByNumber(2), protoreflect.ValueOfString("a-42"))
			if sender.Fields().ByNumber(4) != nil {
				source.Set(sender.Fields().ByNumber(4), protoreflect.ValueOfString("2026-11"))
			}
			data, err := proto.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			target := dynamicpb.NewMessage(receiver)
			if err = proto.Unmarshal(data, target); err != nil {
				t.Fatal(err)
			}
			if target.Get(receiver.Fields().ByNumber(1)).String() != "r-42" {
				t.Fatal("result changed")
			}
			if receiver.Fields().ByNumber(4) != nil && sender.Fields().ByNumber(4) == nil && target.Has(receiver.Fields().ByNumber(4)) {
				t.Fatal("missing field got fabricated presence")
			}
		}
	}
}
