package transformer

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dop251/goja"
)

// vm pool: creates and reuses goja instances so we don't allocate memory on every request
var vmPool = &sync.Pool{
	New: func() interface{} {
		return goja.New()
	},
}

// script cache: caches compiled js bytecode (AST) so we only compile once
var scriptCache sync.Map

// fetches from cache, or compiles it if it's the first time
func getCompiledScript(script string) (*goja.Program, error) {
	if cached, ok := scriptCache.Load(script); ok {
		return cached.(*goja.Program), nil
	}
	// wrap the script
	wrappedScript := fmt.Sprintf(`(function(payload) {%s return payload;})(payload);`, script)

	// compile it into machine bytecode
	prog, err := goja.Compile("", wrappedScript, false)
	if err != nil {
		return nil, err
	}

	// save it in the cache for next time
	scriptCache.Store(script, prog)
	return prog, nil
}

func Transform(script string, payload string) (string, error) {
	if script == "" {
		return payload, nil
	}

	// get pre-compiled bytecode
	prog, err := getCompiledScript(script)

	if err != nil {
		return "", fmt.Errorf("compile error: %w", err)
	}

	// get a reusable VM from the pool
	vm := vmPool.Get().(*goja.Runtime)

	defer func() {
		vm.Set("payload", nil)
		vmPool.Put(vm)
	}()

	var payloadMap map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &payloadMap); err != nil {
		return "", fmt.Errorf("invalid json payload: %w", err)
	}
	vm.Set("payload", payloadMap)

	// run compiled bytecode
	val, err := vm.RunProgram(prog)
	if err != nil {
		return "", fmt.Errorf("script execution failed: %w", err)
	}

	// convert result back to JSON
	outBytes, err := json.Marshal(val.Export())

	if err != nil {
		return "", fmt.Errorf("failed to marshal result: %w", err)
	}

	return string(outBytes), nil
}
