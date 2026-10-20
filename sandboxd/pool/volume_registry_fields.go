package pool

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"reflect"
	"strings"
)

// mergeRegistryFields retains additive metadata only on objects that still exist.
// Known fields absent from the new projection stay absent, including operation fences.
func mergeRegistryFields(current, original jsontext.Value, typ reflect.Type) (jsontext.Value, error) {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if len(current) == 0 || len(original) == 0 || current[0] != original[0] {
		return current, nil
	}
	if current[0] == '[' && typ.Kind() == reflect.Slice {
		var next, old []jsontext.Value
		if json.Unmarshal(current, &next) != nil || json.Unmarshal(original, &old) != nil {
			return current, nil
		}
		for i := range next {
			for j := range old {
				if registryObjectKey(next[i], typ.Elem()) != "" && registryObjectKey(next[i], typ.Elem()) == registryObjectKey(old[j], typ.Elem()) {
					value, err := mergeRegistryFields(next[i], old[j], typ.Elem())
					if err != nil {
						return nil, err
					}
					next[i] = value
					break
				}
			}
		}
		return json.Marshal(next)
	}
	if current[0] != '{' || typ.Kind() != reflect.Struct && typ.Kind() != reflect.Map {
		return current, nil
	}
	var next, old map[string]jsontext.Value
	if json.Unmarshal(current, &next) != nil || json.Unmarshal(original, &old) != nil {
		return current, nil
	}
	if a, b := registryObjectKey(current, typ), registryObjectKey(original, typ); typ.Kind() == reflect.Struct && a != b {
		return current, nil
	}
	fields := map[string]reflect.Type{}
	if typ.Kind() == reflect.Struct {
		for field := range typ.Fields() {
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "" {
				name = field.Name
			}
			fields[name] = field.Type
		}
	}
	for name, previous := range old {
		child, known := fields[name]
		if typ.Kind() == reflect.Map {
			child, known = typ.Elem(), true
		}
		if !known {
			next[name] = previous
			continue
		}
		if value, exists := next[name]; exists {
			merged, err := mergeRegistryFields(value, previous, child)
			if err != nil {
				return nil, err
			}
			next[name] = merged
		}
	}
	return json.Marshal(next)
}

func registryObjectKey(raw jsontext.Value, typ reflect.Type) string {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return ""
	}
	fields := map[string]reflect.Type{}
	for field := range typ.Fields() {
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "" {
			name = field.Name
		}
		fields[name] = field.Type
	}

	var obj map[string]jsontext.Value
	if json.Unmarshal(raw, &obj) != nil {
		return ""
	}
	if request, ok := obj["request"]; ok && fields["request"] != nil {
		if key := registryObjectKey(request, fields["request"]); key != "" {
			return key
		}
	}
	if boot, ok := obj["boot_id"]; ok && fields["boot_id"] != nil {
		return string(boot) + "/" + string(obj["pid"]) + "/" + string(obj["start"])
	}
	if inode, ok := obj["inode"]; ok && fields["inode"] != nil {
		return string(obj["device"]) + "/" + string(inode)
	}
	for _, key := range []string{"id", "sandbox_id", "operation", "operation_id", "name", "number"} {
		if value, ok := obj[key]; ok && fields[key] != nil {
			return key + string(value)
		}
	}
	if volume, ok := obj["volume"]; ok && fields["volume"] != nil {
		return registryObjectKey(volume, fields["volume"])
	}
	return ""
}
